"""
Servidor del banco distribuido (Guia 5 - Consistencia y Replicacion)
Universidad Manuela Beltran - Sistemas Distribuidos

Modelo cliente-servidor: este programa es el SERVIDOR. Guarda las tres replicas
del banco (Bogota, Nueva York y Madrid) y el coordinador de transacciones.
Los CLIENTES son los cajeros (cajero.py), uno por ciudad, que se conectan por
sockets TCP y mandan peticiones en formato JSON, una por linea.

Se ejecuta con:  python servidor_banco.py            (escucha en 0.0.0.0:5000)
                 python servidor_banco.py 6000       (otro puerto)

Reutiliza las clases del emulador banco_distribuido.py (debe estar en la misma carpeta).
"""

import json
import socket
import sys
import threading
import time

import banco_distribuido as bd

HOST = "0.0.0.0"
PUERTO = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

# ---------------------------------------------------------------------------
# Bitacora: ademas de imprimir en consola, se guarda para enviarla al cajero
# ---------------------------------------------------------------------------
_bitacora_lock = threading.Lock()
_bitacora = []           # lista de (numero, linea)
_contador = [0]


def log_servidor(origen, mensaje):
    ms = (time.time() - bd._t0) * 1000
    linea = f"[{ms:9.1f} ms] {origen:<18} {mensaje}"
    with _bitacora_lock:
        _contador[0] += 1
        _bitacora.append((_contador[0], linea))
        if len(_bitacora) > 500:
            del _bitacora[:100]
    print(linea, flush=True)


bd.log = log_servidor     # las clases del emulador usan esta funcion


def bitacora_desde(n):
    with _bitacora_lock:
        return [(k, l) for (k, l) in _bitacora if k > n]


# ---------------------------------------------------------------------------
# Estado del banco
# ---------------------------------------------------------------------------
class Banco:
    def __init__(self):
        self._lock = threading.Lock()
        self.reiniciar()
        self.barrera = None
        self.esperando = 0
        # hilo que propaga las escrituras del modo eventual cada 300 ms
        threading.Thread(target=self._propagador, daemon=True).start()

    def reiniciar(self, saldo=100.0):
        with self._lock:
            self.replicas = bd.crear_banco()
            for r in self.replicas:
                r.cuentas["1001"].saldo = saldo
            self.por_ciudad = {r.ciudad: r for r in self.replicas}
            self.coordinador = bd.CoordinadorTransacciones(self.replicas, tiempo_espera_ms=400)
            self.replicador = bd.ReplicadorAsincrono(self.replicas, retraso_ms=0)
            self.contador_tx = 0
        log_servidor("Servidor", f"cuenta 1001 reiniciada con saldo {saldo:.0f} en las 3 replicas")

    def _propagador(self):
        while True:
            time.sleep(0.3)
            try:
                hay = any(r.cola_replicacion for r in self.replicas)
                if hay:
                    log_servidor("Replicador", "propagando escrituras pendientes...")
                    self.replicador.propagar()
            except Exception as e:      # una replica caida no debe tumbar el hilo
                log_servidor("Replicador", f"error al propagar: {e}")

    def nueva_tx(self, cuenta, monto, origen):
        with self._lock:
            self.contador_tx += 1
            return bd.Transaccion(f"TX-{self.contador_tx:03d}", cuenta, monto, "RETIRO", origen)

    def saldos(self):
        return {r.ciudad: {"saldo": r.cuentas["1001"].saldo,
                           "version": r.cuentas["1001"].version,
                           "disponible": r.disponible} for r in self.replicas}

    def replica_de(self, ciudad):
        return self.por_ciudad.get(ciudad, self.replicas[0])


banco = Banco()


# ---------------------------------------------------------------------------
# Operaciones que piden los cajeros
# ---------------------------------------------------------------------------
def op_retirar(req):
    ciudad = req.get("ciudad", "Bogota")
    monto = float(req.get("monto", 0))
    cuenta = req.get("cuenta", "1001")
    modo = req.get("modo", "fuerte")
    origen = f"Cajero-{ciudad.replace(' ', '')}"
    replica = banco.replica_de(ciudad)

    if monto <= 0:
        return {"ok": False, "estado": "RECHAZADA", "mensaje": "El monto debe ser mayor que cero"}

    # Retiro simultaneo: el servidor espera a que lleguen los DOS cajeros y
    # dispara las dos transacciones en el mismo instante (como en el enunciado).
    if req.get("sincronizar"):
        with banco._lock:
            if banco.barrera is None or banco.barrera.broken:
                banco.barrera = threading.Barrier(2)
            barrera = banco.barrera
        log_servidor(origen, "espera al otro cajero para retirar al mismo tiempo...")
        try:
            barrera.wait(timeout=20)
        except threading.BrokenBarrierError:
            return {"ok": False, "estado": "CANCELADA",
                    "mensaje": "El otro cajero no llego en 20 s; intente de nuevo"}

    tx = banco.nueva_tx(cuenta, monto, origen)
    log_servidor(origen, f"solicita retirar {monto:.0f} de {cuenta} (modo {modo})")

    if modo == "eventual":
        if not replica.disponible:
            tx.estado = "CANCELADA"
            mensaje = "Su replica local no responde (particion de red)"
        elif replica.escribir_local(tx):
            tx.estado = "APROBADA"
            mensaje = f"Entregando {monto:.0f} (validado solo en la replica de {ciudad})"
        else:
            tx.estado = "RECHAZADA"
            mensaje = "Fondos insuficientes en la replica local"
    else:
        tx = banco.coordinador.ejecutar(tx)
        if tx.estado == "APROBADA":
            mensaje = f"Entregando {monto:.0f} en efectivo"
        elif tx.estado == "RECHAZADA":
            mensaje = "Transaccion rechazada: fondos insuficientes o cuenta bloqueada"
        else:
            mensaje = "Transaccion cancelada: no fue posible confirmar con todas las replicas"

    log_servidor(origen, f"pantalla: '{mensaje}'")
    return {"ok": tx.estado == "APROBADA", "estado": tx.estado, "id_tx": tx.id_tx, "mensaje": mensaje}


def op_consultar(req):
    ciudad = req.get("ciudad", "Bogota")
    replica = banco.replica_de(ciudad)
    try:
        saldo = replica.consultar_saldo(req.get("cuenta", "1001"))
        return {"ok": True, "mensaje": f"Saldo segun la replica de {ciudad}: {saldo:.0f}", "saldo": saldo}
    except TimeoutError as e:
        return {"ok": False, "mensaje": str(e)}


def op_red(req):
    ciudad = req.get("nodo", "Madrid")
    replica = banco.replica_de(ciudad)
    replica.disponible = bool(req.get("disponible", True))
    estado = "se restablece" if replica.disponible else "se cae"
    log_servidor("Red", f"el enlace hacia {replica.nombre} {estado}")
    return {"ok": True, "mensaje": f"Enlace hacia {ciudad}: {'ARRIBA' if replica.disponible else 'CAIDO'}"}


def op_reiniciar(req):
    banco.reiniciar(float(req.get("saldo", 100)))
    return {"ok": True, "mensaje": "Cuenta 1001 reiniciada"}


OPERACIONES = {
    "RETIRAR": op_retirar,
    "CONSULTAR": op_consultar,
    "RED": op_red,
    "REINICIAR": op_reiniciar,
    "ESTADO": lambda req: {"ok": True, "mensaje": "estado"},
}


# ---------------------------------------------------------------------------
# Atencion de conexiones (un hilo por cliente)
# ---------------------------------------------------------------------------
def atender(conn, addr):
    with conn:
        archivo = conn.makefile("r", encoding="utf-8")
        for linea in archivo:
            linea = linea.strip()
            if not linea:
                continue
            try:
                req = json.loads(linea)
                fn = OPERACIONES.get(req.get("op", ""), None)
                if fn is None:
                    resp = {"ok": False, "mensaje": f"operacion desconocida: {req.get('op')}"}
                else:
                    resp = fn(req)
            except Exception as e:
                resp = {"ok": False, "mensaje": f"error en el servidor: {e}"}
            resp["saldos"] = banco.saldos()
            resp["bitacora"] = bitacora_desde(int(req.get("desde", 0)) if isinstance(req, dict) else 0)
            try:
                conn.sendall((json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8"))
            except OSError:
                break


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PUERTO))
    srv.listen()
    print("=" * 78)
    print(f"SERVIDOR DEL BANCO escuchando en {HOST}:{PUERTO}")
    print("Replicas: Bogota (5 ms), Nueva York (60 ms), Madrid (90 ms)")
    print("Conecte los cajeros con:  python cajero.py \"Nueva York\"   y   python cajero.py Madrid")
    print("=" * 78, flush=True)
    while True:
        conn, addr = srv.accept()
        log_servidor("Servidor", f"conexion desde {addr[0]}:{addr[1]}")
        threading.Thread(target=atender, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()

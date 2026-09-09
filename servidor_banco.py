"""
Servidor del banco distribuido (Guia 5 - Consistencia y Replicacion)
Universidad Manuela Beltran - Sistemas Distribuidos

Modelo cliente-servidor: este programa es el SERVIDOR. Guarda las tres replicas
del banco (Bogota, Nueva York y Madrid) y el coordinador de transacciones.
Los CLIENTES son los cajeros (cajero.py), uno por ciudad, que se conectan por
sockets TCP y mandan peticiones en formato JSON, una por linea.

Se ejecuta con:  python servidor_banco.py            (escucha en 0.0.0.0:5000, espera 10 s)
                 python servidor_banco.py 5000 20    (mismo puerto, espera 20 s por el otro cajero)

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
ESPERA_OTRO_CAJERO = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0   # segundos

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
        # turnos por orden de llegada: el primero que pulsa RETIRAR es el primero que se procesa
        self.turno_condicion = threading.Condition()
        self.siguiente_turno = 0      # turno que se entrega al proximo retiro que llegue
        self.turno_en_curso = 0       # turno al que le toca ejecutarse
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

    def tomar_turno(self):
        with self.turno_condicion:
            t = self.siguiente_turno
            self.siguiente_turno += 1
            return t

    def esperar_turno(self, turno):
        with self.turno_condicion:
            while self.turno_en_curso != turno:
                self.turno_condicion.wait()

    def liberar_turno(self):
        with self.turno_condicion:
            self.turno_en_curso += 1
            self.turno_condicion.notify_all()

    def nueva_tx(self, cuenta, monto, origen, tipo="RETIRO"):
        with self._lock:
            self.contador_tx += 1
            return bd.Transaccion(f"TX-{self.contador_tx:03d}", cuenta, monto, tipo, origen)

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

    turno = banco.tomar_turno()          # orden de llegada: quien pulsa primero va primero
    log_servidor(origen, f"retiro recibido (turno {turno + 1})")

    # Ventana de espera silenciosa: todo retiro espera hasta ESPERA_OTRO_CAJERO segundos
    # por si el otro cajero tambien retira. Si llegan los dos, el servidor dispara ambas
    # transacciones en el mismo instante (como en el enunciado); si no llega nadie mas,
    # el retiro continua solo.
    with banco._lock:
        if banco.barrera is None or banco.barrera.broken:
            banco.barrera = threading.Barrier(2)
        barrera = banco.barrera
    log_servidor(origen, f"ventana de {ESPERA_OTRO_CAJERO:.0f} s por si llega otro cajero")
    try:
        barrera.wait(timeout=ESPERA_OTRO_CAJERO)
        log_servidor(origen, "llego el otro cajero: los dos retiros se procesan al mismo tiempo")
    except threading.BrokenBarrierError:
        log_servidor(origen, "no llego otro cajero; el retiro continua solo")
    with banco._lock:
        if banco.barrera is barrera:
            banco.barrera = None

    banco.esperar_turno(turno)           # respeta el orden en que llegaron los retiros
    try:
        tx = banco.nueva_tx(cuenta, monto, origen)
        log_servidor(origen, f"solicita retirar {monto:.0f} de {cuenta} (modo {modo})")
        return _procesar_retiro(tx, replica, ciudad, monto, modo, origen)
    finally:
        banco.liberar_turno()


def _procesar_retiro(tx, replica, ciudad, monto, modo, origen):
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


def op_consignar(req):
    """Consignacion: suma saldo pasando por el mismo protocolo que el retiro."""
    ciudad = req.get("ciudad", "Bogota")
    monto = float(req.get("monto", 0))
    cuenta = req.get("cuenta", "1001")
    modo = req.get("modo", "fuerte")
    origen = f"Cajero-{ciudad.replace(' ', '')}"
    replica = banco.replica_de(ciudad)
    if monto <= 0:
        return {"ok": False, "estado": "RECHAZADA", "mensaje": "El monto debe ser mayor que cero"}
    tx = banco.nueva_tx(cuenta, monto, origen, tipo="CONSIGNACION")
    log_servidor(origen, f"solicita consignar {monto:.0f} en {cuenta} (modo {modo})")
    if modo == "eventual":
        if not replica.disponible:
            tx.estado = "CANCELADA"
            mensaje = "Su replica local no responde (particion de red)"
        else:
            replica.escribir_local(tx)
            tx.estado = "APROBADA"
            mensaje = f"Consignacion de {monto:.0f} recibida (solo en la replica de {ciudad}, se propaga despues)"
    else:
        tx = banco.coordinador.ejecutar(tx)
        if tx.estado == "APROBADA":
            mensaje = f"Consignacion de {monto:.0f} confirmada en las 3 replicas"
        else:
            mensaje = "Consignacion cancelada: no fue posible confirmar con todas las replicas"
    log_servidor(origen, f"pantalla: '{mensaje}'")
    return {"ok": tx.estado == "APROBADA", "estado": tx.estado, "id_tx": tx.id_tx, "mensaje": mensaje}


def op_consultar(req):
    ciudad = req.get("ciudad", "Bogota")
    replica = banco.replica_de(ciudad)
    origen = f"Cajero-{ciudad.replace(' ', '')}"
    log_servidor(origen, f"consulta saldo de {req.get('cuenta', '1001')} en {replica.nombre}")
    try:
        saldo = replica.consultar_saldo(req.get("cuenta", "1001"))
        log_servidor(replica.nombre, f"responde saldo {saldo:.0f} (v{replica.cuentas['1001'].version})")
        return {"ok": True, "mensaje": f"Saldo segun la replica de {ciudad}: {saldo:.0f}", "saldo": saldo}
    except TimeoutError as e:
        log_servidor(replica.nombre, f"consulta fallida: {e}")
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
    "CONSIGNAR": op_consignar,
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

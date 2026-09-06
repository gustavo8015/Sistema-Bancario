"""
Emulador de un sistema bancario distribuido (Guia 5 - Consistencia y Replicacion)
Universidad Manuela Beltran - Sistemas Distribuidos

Que se emula:
  - Tres nodos (replicas) del banco ubicados en Bogota, Nueva York y Madrid.
  - Cajeros automaticos (clientes) conectados a la replica de su ciudad.
  - Dos formas de replicar el saldo:
      a) Consistencia fuerte: bloqueo global + confirmacion en dos fases (2PC).
      b) Consistencia eventual: se escribe en la replica local y se propaga despues.
  - Retiros simultaneos sobre la misma cuenta para ver que pasa en cada caso.
  - Una particion de red (Madrid no alcanza a las demas replicas).

Se ejecuta con:  python banco_distribuido.py
"""

import threading
import time
import random
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Utilidades de registro (bitacora)
# ---------------------------------------------------------------------------
_t0 = time.time()
_print_lock = threading.Lock()


def log(origen, mensaje):
    """Imprime una linea de bitacora con el tiempo relativo en milisegundos."""
    ms = (time.time() - _t0) * 1000
    with _print_lock:
        print(f"[{ms:8.1f} ms] {origen:<18} {mensaje}")


# ---------------------------------------------------------------------------
# Modelo de datos
# ---------------------------------------------------------------------------
@dataclass
class Cuenta:
    numero: str
    titular: str
    saldo: float
    version: int = 0          # crece con cada escritura confirmada

    def copia(self):
        return Cuenta(self.numero, self.titular, self.saldo, self.version)


@dataclass
class Transaccion:
    id_tx: str
    cuenta: str
    monto: float
    tipo: str                 # RETIRO o TRANSFERENCIA
    origen: str               # cajero que la genero
    estado: str = "PENDIENTE"  # APROBADA / RECHAZADA / CANCELADA


# ---------------------------------------------------------------------------
# Nodo replica: guarda una copia de las cuentas y atiende peticiones
# ---------------------------------------------------------------------------
class NodoReplica:
    def __init__(self, nombre, ciudad, latencia_ms):
        self.nombre = nombre
        self.ciudad = ciudad
        self.latencia_ms = latencia_ms          # latencia de red hacia este nodo
        self.cuentas = {}                       # numero -> Cuenta
        self.bloqueos = {}                      # numero -> id_tx que lo tiene
        self.disponible = True                  # False = particion de red
        self._mutex = threading.Lock()
        self.cola_replicacion = []              # usada en modo eventual

    # --- red -------------------------------------------------------------
    def _viajar_por_la_red(self):
        """Simula el tiempo que tarda un mensaje en llegar al nodo."""
        if not self.disponible:
            raise TimeoutError(f"{self.nombre} no responde (particion de red)")
        time.sleep(self.latencia_ms / 1000.0)

    # --- lecturas ----------------------------------------------------------
    def consultar_saldo(self, numero):
        self._viajar_por_la_red()
        return self.cuentas[numero].saldo

    # --- protocolo de dos fases (consistencia fuerte) -----------------------
    def preparar(self, tx: Transaccion):
        """Fase 1: intenta bloquear la cuenta y verifica fondos."""
        self._viajar_por_la_red()
        with self._mutex:
            if tx.cuenta in self.bloqueos:
                log(self.nombre, f"NO puede preparar {tx.id_tx}: cuenta bloqueada por {self.bloqueos[tx.cuenta]}")
                return False
            cuenta = self.cuentas[tx.cuenta]
            if cuenta.saldo < tx.monto:
                log(self.nombre, f"NO puede preparar {tx.id_tx}: saldo {cuenta.saldo:.0f} < {tx.monto:.0f}")
                return False
            self.bloqueos[tx.cuenta] = tx.id_tx
            log(self.nombre, f"preparado {tx.id_tx}, cuenta {tx.cuenta} bloqueada")
            return True

    def confirmar(self, tx: Transaccion):
        """Fase 2 (commit): aplica el cambio y libera el bloqueo."""
        self._viajar_por_la_red()
        with self._mutex:
            cuenta = self.cuentas[tx.cuenta]
            cuenta.saldo -= tx.monto
            cuenta.version += 1
            self.bloqueos.pop(tx.cuenta, None)
            log(self.nombre, f"confirmado {tx.id_tx}, saldo ahora {cuenta.saldo:.0f} (v{cuenta.version})")

    def abortar(self, tx: Transaccion):
        """Fase 2 (abort): deshace el bloqueo sin tocar el saldo."""
        try:
            self._viajar_por_la_red()
        except TimeoutError:
            pass  # si el nodo esta aislado, el bloqueo expira cuando vuelva
        with self._mutex:
            if self.bloqueos.get(tx.cuenta) == tx.id_tx:
                self.bloqueos.pop(tx.cuenta)
                log(self.nombre, f"abortado {tx.id_tx}, bloqueo liberado")

    # --- escritura local (consistencia eventual) -----------------------------
    def escribir_local(self, tx: Transaccion):
        """Aplica el retiro solo en esta replica y lo deja en cola para propagar."""
        with self._mutex:
            cuenta = self.cuentas[tx.cuenta]
            if cuenta.saldo < tx.monto:
                return False
            cuenta.saldo -= tx.monto
            cuenta.version += 1
            self.cola_replicacion.append(tx)
            log(self.nombre, f"escritura local {tx.id_tx}, saldo local {cuenta.saldo:.0f}")
            return True

    def aplicar_replica(self, tx: Transaccion, desde):
        """Recibe una actualizacion propagada por otra replica."""
        self._viajar_por_la_red()
        with self._mutex:
            cuenta = self.cuentas[tx.cuenta]
            cuenta.saldo -= tx.monto
            cuenta.version += 1
            log(self.nombre, f"replica recibida de {desde}: {tx.id_tx}, saldo ahora {cuenta.saldo:.0f}")


# ---------------------------------------------------------------------------
# Coordinador: implementa la consistencia fuerte con 2PC sobre todas las replicas
# ---------------------------------------------------------------------------
class CoordinadorTransacciones:
    def __init__(self, replicas, tiempo_espera_ms=400):
        self.replicas = replicas
        self.tiempo_espera = tiempo_espera_ms / 1000.0
        self._orden = threading.Lock()   # garantiza orden secuencial de las tx

    def ejecutar(self, tx: Transaccion):
        log("Coordinador", f"inicia {tx.id_tx}: {tx.tipo} de {tx.monto:.0f} desde {tx.origen}")
        with self._orden:
            inicio = time.time()
            preparados = []
            # ---- Fase 1: pedir a TODAS las replicas que se preparen ----
            for r in self.replicas:
                try:
                    if time.time() - inicio > self.tiempo_espera:
                        raise TimeoutError("se agoto el tiempo de espera del coordinador")
                    if r.preparar(tx):
                        preparados.append(r)
                    else:
                        raise RuntimeError(f"{r.nombre} voto NO")
                except (TimeoutError, RuntimeError) as e:
                    log("Coordinador", f"{tx.id_tx} -> ABORTAR ({e})")
                    for p in preparados:
                        p.abortar(tx)
                    tx.estado = "RECHAZADA" if isinstance(e, RuntimeError) else "CANCELADA"
                    return tx
            # ---- Fase 2: todas dijeron SI, confirmar en todas ----
            for r in self.replicas:
                r.confirmar(tx)
            tx.estado = "APROBADA"
            log("Coordinador", f"{tx.id_tx} -> APROBADA en {len(self.replicas)} replicas")
            return tx


# ---------------------------------------------------------------------------
# Cajero automatico (cliente)
# ---------------------------------------------------------------------------
class Cajero:
    contador = 0
    _c_lock = threading.Lock()

    def __init__(self, nombre, replica_local, coordinador=None):
        self.nombre = nombre
        self.replica = replica_local
        self.coordinador = coordinador

    def _nuevo_id(self):
        with Cajero._c_lock:
            Cajero.contador += 1
            return f"TX-{Cajero.contador:03d}"

    def retirar_fuerte(self, cuenta, monto):
        tx = Transaccion(self._nuevo_id(), cuenta, monto, "RETIRO", self.nombre)
        log(self.nombre, f"solicita retirar {monto:.0f} de {cuenta}")
        resultado = self.coordinador.ejecutar(tx)
        if resultado.estado == "APROBADA":
            log(self.nombre, f"ENTREGA {monto:.0f} en efectivo")
        else:
            log(self.nombre, f"pantalla: 'Transaccion {resultado.estado.lower()}, intente mas tarde'")
        return resultado

    def retirar_eventual(self, cuenta, monto):
        tx = Transaccion(self._nuevo_id(), cuenta, monto, "RETIRO", self.nombre)
        log(self.nombre, f"solicita retirar {monto:.0f} de {cuenta} (modo eventual)")
        if self.replica.escribir_local(tx):
            tx.estado = "APROBADA"
            log(self.nombre, f"ENTREGA {monto:.0f} en efectivo (solo valido la replica local)")
        else:
            tx.estado = "RECHAZADA"
            log(self.nombre, "pantalla: 'Fondos insuficientes'")
        return tx


# ---------------------------------------------------------------------------
# Replicador asincrono: propaga las escrituras locales con retraso
# ---------------------------------------------------------------------------
class ReplicadorAsincrono:
    def __init__(self, replicas, retraso_ms):
        self.replicas = replicas
        self.retraso = retraso_ms / 1000.0

    def propagar(self):
        time.sleep(self.retraso)
        for origen in self.replicas:
            while origen.cola_replicacion:
                tx = origen.cola_replicacion.pop(0)
                for destino in self.replicas:
                    if destino is not origen:
                        destino.aplicar_replica(tx, origen.nombre)


# ---------------------------------------------------------------------------
# Escenarios
# ---------------------------------------------------------------------------
def crear_banco():
    bogota = NodoReplica("Nodo-Bogota", "Bogota", latencia_ms=5)
    nueva_york = NodoReplica("Nodo-NuevaYork", "Nueva York", latencia_ms=60)
    madrid = NodoReplica("Nodo-Madrid", "Madrid", latencia_ms=90)
    replicas = [bogota, nueva_york, madrid]
    for r in replicas:
        r.cuentas["1001"] = Cuenta("1001", "Gustavo Cardona", 100.0)
    return replicas


def mostrar_saldos(replicas, titulo):
    print()
    print(f"  {titulo}")
    for r in replicas:
        c = r.cuentas["1001"]
        print(f"    {r.nombre:<16} saldo = {c.saldo:>7.0f}   version = {c.version}")
    saldos = {r.cuentas['1001'].saldo for r in replicas}
    print("    Replicas coherentes:", "SI" if len(saldos) == 1 else "NO")
    print()


def separador(titulo):
    print()
    print("=" * 78)
    print(titulo)
    print("=" * 78)


def escenario_1_fuerte():
    separador("ESCENARIO 1. Retiros simultaneos con CONSISTENCIA FUERTE (2PC + bloqueo)")
    replicas = crear_banco()
    coord = CoordinadorTransacciones(replicas)
    ny = Cajero("Cajero-NuevaYork", replicas[1], coord)
    md = Cajero("Cajero-Madrid", replicas[2], coord)
    mostrar_saldos(replicas, "Saldos iniciales")

    barrera = threading.Barrier(2)   # los dos cajeros disparan en el mismo instante

    def retiro(cajero):
        barrera.wait()
        cajero.retirar_fuerte("1001", 100)

    h1 = threading.Thread(target=retiro, args=(ny,))
    h2 = threading.Thread(target=retiro, args=(md,))
    h1.start(); h2.start(); h1.join(); h2.join()
    mostrar_saldos(replicas, "Saldos finales")


def escenario_2_eventual():
    separador("ESCENARIO 2. Retiros simultaneos con CONSISTENCIA EVENTUAL (replicacion asincrona)")
    replicas = crear_banco()
    ny = Cajero("Cajero-NuevaYork", replicas[1])
    md = Cajero("Cajero-Madrid", replicas[2])
    replicador = ReplicadorAsincrono(replicas, retraso_ms=300)
    mostrar_saldos(replicas, "Saldos iniciales")

    barrera = threading.Barrier(2)

    def retiro(cajero):
        barrera.wait()
        cajero.retirar_eventual("1001", 100)

    h1 = threading.Thread(target=retiro, args=(ny,))
    h2 = threading.Thread(target=retiro, args=(md,))
    h1.start(); h2.start(); h1.join(); h2.join()
    mostrar_saldos(replicas, "Saldos justo despues de los retiros (antes de propagar)")
    log("Replicador", "propagando escrituras pendientes...")
    replicador.propagar()
    mostrar_saldos(replicas, "Saldos despues de la propagacion")
    saldo = replicas[0].cuentas["1001"].saldo
    if saldo < 0:
        log("Auditoria", f"CONFLICTO detectado: el banco entrego {abs(saldo):.0f} que no existian (sobregiro)")


def escenario_3_particion():
    separador("ESCENARIO 3. Consistencia fuerte con PARTICION DE RED (Madrid aislado)")
    replicas = crear_banco()
    coord = CoordinadorTransacciones(replicas, tiempo_espera_ms=400)
    replicas[2].disponible = False
    log("Red", "el enlace hacia Nodo-Madrid se cae")
    md = Cajero("Cajero-Madrid", replicas[2], coord)
    md.retirar_fuerte("1001", 100)
    mostrar_saldos([replicas[0], replicas[1]], "Saldos en las replicas alcanzables")
    replicas[2].disponible = True
    log("Red", "el enlace hacia Nodo-Madrid se restablece")
    md.retirar_fuerte("1001", 100)
    mostrar_saldos(replicas, "Saldos finales")


if __name__ == "__main__":
    random.seed(7)
    escenario_1_fuerte()
    escenario_2_eventual()
    escenario_3_particion()

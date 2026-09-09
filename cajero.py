"""
Cajero automatico (CLIENTE grafico) del banco distribuido
Guia 5 - Consistencia y Replicacion - Universidad Manuela Beltran

Cada ventana es un cajero en una ciudad y se conecta al servidor por TCP.
Para reproducir el enunciado se abren dos cajeros:

    python cajero.py "Nueva York"
    python cajero.py Madrid

Si el servidor esta en otro computador:  python cajero.py Madrid 192.168.1.20 5000
"""

import json
import queue
import socket
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox

CIUDAD = sys.argv[1] if len(sys.argv) > 1 else "Nueva York"
HOST = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
PUERTO = int(sys.argv[3]) if len(sys.argv) > 3 else 5000


class ClienteBanco:
    """Envia una peticion JSON por linea y recibe una respuesta JSON por linea."""

    def __init__(self, host, puerto):
        self.host, self.puerto = host, puerto
        self.ultimo_log = 0

    def enviar(self, req):
        req["desde"] = self.ultimo_log
        with socket.create_connection((self.host, self.puerto), timeout=60) as s:
            s.sendall((json.dumps(req) + "\n").encode("utf-8"))
            datos = s.makefile("r", encoding="utf-8").readline()
        resp = json.loads(datos)
        if resp.get("bitacora"):
            self.ultimo_log = resp["bitacora"][-1][0]
        return resp


class VentanaCajero(tk.Tk):
    def __init__(self, ciudad, cliente):
        super().__init__()
        self.ciudad = ciudad
        self.cliente = cliente
        self.red_arriba = True
        self.cola = queue.Queue()        # respuestas que llegan desde los hilos de red
        self.title(f"Cajero automatico - {ciudad}")
        self.geometry("760x600")
        self.resizable(True, True)
        self._construir()
        self.after(100, self._revisar_cola)
        self.after(200, self.consultar)

    # ------------------------------------------------------------------ UI
    def _construir(self):
        estilo = ttk.Style(self)
        estilo.configure("Titulo.TLabel", font=("Segoe UI", 16, "bold"))
        estilo.configure("Saldo.TLabel", font=("Consolas", 28, "bold"), foreground="#0b6e4f")
        estilo.configure("Msg.TLabel", font=("Segoe UI", 11))

        cab = ttk.Frame(self, padding=10)
        cab.pack(fill="x")
        ttk.Label(cab, text=f"Banco Distribuido UMB   |   Cajero {self.ciudad}", style="Titulo.TLabel").pack(side="left")
        self.lbl_red = ttk.Label(cab, text="Red: ARRIBA", foreground="green")
        self.lbl_red.pack(side="right")

        cuerpo = ttk.Frame(self, padding=10)
        cuerpo.pack(fill="both", expand=True)

        # --- panel izquierdo: operacion ---
        izq = ttk.LabelFrame(cuerpo, text="Operacion", padding=10)
        izq.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        ttk.Label(izq, text="Saldo segun mi replica local").grid(row=0, column=0, columnspan=2, sticky="w")
        self.lbl_saldo = ttk.Label(izq, text="---", style="Saldo.TLabel")
        self.lbl_saldo.grid(row=1, column=0, columnspan=2, sticky="w")
        self.lbl_msg = ttk.Label(izq, text="Conectando con el servidor...", style="Msg.TLabel", wraplength=300, justify="left")
        self.lbl_msg.grid(row=2, column=0, columnspan=2, sticky="w", pady=(0, 10))

        ttk.Label(izq, text="Cuenta").grid(row=3, column=0, sticky="w")
        self.ent_cuenta = ttk.Entry(izq, width=12)
        self.ent_cuenta.insert(0, "1001")
        self.ent_cuenta.grid(row=3, column=1, sticky="w")

        ttk.Label(izq, text="Monto").grid(row=4, column=0, sticky="w")
        self.ent_monto = ttk.Entry(izq, width=12)
        self.ent_monto.insert(0, "100")
        self.ent_monto.grid(row=4, column=1, sticky="w")

        ttk.Label(izq, text="Modo de consistencia").grid(row=5, column=0, sticky="w", pady=(8, 0))
        self.modo = tk.StringVar(value="fuerte")
        ttk.Radiobutton(izq, text="Fuerte (bloqueo + 2PC)", variable=self.modo, value="fuerte").grid(row=6, column=0, columnspan=2, sticky="w")
        ttk.Radiobutton(izq, text="Eventual (replica local, propaga despues)", variable=self.modo, value="eventual").grid(row=7, column=0, columnspan=2, sticky="w")


        self.btn_retirar = ttk.Button(izq, text="RETIRAR", command=self.retirar)
        self.btn_retirar.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(12, 4))
        ttk.Button(izq, text="Consignar (agregar saldo)", command=self.consignar).grid(row=10, column=0, columnspan=2, sticky="ew", pady=2)
        ttk.Button(izq, text="Consultar saldo", command=self.consultar).grid(row=11, column=0, columnspan=2, sticky="ew", pady=2)
        self.btn_red = ttk.Button(izq, text="Simular caida de red de mi ciudad", command=self.alternar_red)
        self.btn_red.grid(row=12, column=0, columnspan=2, sticky="ew", pady=2)
        ttk.Button(izq, text="Reiniciar cuenta a 100", command=self.reiniciar).grid(row=13, column=0, columnspan=2, sticky="ew", pady=2)


        # --- panel derecho: replicas y bitacora ---
        der = ttk.Frame(cuerpo)
        der.grid(row=0, column=1, sticky="nsew")
        cuerpo.columnconfigure(1, weight=1)
        cuerpo.rowconfigure(0, weight=1)

        rep = ttk.LabelFrame(der, text="Saldo en cada replica del banco", padding=8)
        rep.pack(fill="x")
        self.lbl_replicas = {}
        for i, c in enumerate(["Bogota", "Nueva York", "Madrid"]):
            ttk.Label(rep, text=c, width=12).grid(row=i, column=0, sticky="w")
            l = ttk.Label(rep, text="---", font=("Consolas", 11))
            l.grid(row=i, column=1, sticky="w")
            self.lbl_replicas[c] = l
        self.lbl_coherente = ttk.Label(rep, text="")
        self.lbl_coherente.grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 0))

        bit = ttk.LabelFrame(der, text="Bitacora del servidor", padding=4)
        bit.pack(fill="both", expand=True, pady=(8, 0))
        self.txt = tk.Text(bit, font=("Consolas", 8), wrap="word", state="disabled", bg="#111", fg="#d6d6d6")
        sb = ttk.Scrollbar(bit, command=self.txt.yview)
        self.txt.configure(yscrollcommand=sb.set)
        self.txt.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

    # ------------------------------------------------------------- acciones
    def _en_hilo(self, req, al_terminar=None):
        """Manda la peticion en un hilo para que la ventana no se congele."""
        self.btn_retirar.configure(state="disabled")

        def trabajo():
            try:
                resp = self.cliente.enviar(req)
            except OSError as e:
                resp = {"ok": False, "mensaje": f"No se pudo conectar con el servidor ({e})", "saldos": {}, "bitacora": []}
            self.cola.put((resp, al_terminar))   # Tk solo se toca desde el hilo principal

        threading.Thread(target=trabajo, daemon=True).start()

    def _revisar_cola(self):
        try:
            while True:
                resp, al_terminar = self.cola.get_nowait()
                self._mostrar(resp, al_terminar)
        except queue.Empty:
            pass
        self.after(100, self._revisar_cola)

    def _mostrar(self, resp, al_terminar=None):
        self.btn_retirar.configure(state="normal")
        self.lbl_msg.configure(text=resp.get("mensaje", ""),
                               foreground="#0b6e4f" if resp.get("ok") else "#b00020")
        saldos = resp.get("saldos") or {}
        if saldos:
            mio = saldos.get(self.ciudad, {})
            if mio:
                self.lbl_saldo.configure(text=f"{mio['saldo']:.0f}")
            valores = set()
            for c, l in self.lbl_replicas.items():
                d = saldos.get(c)
                if d:
                    estado = "" if d["disponible"] else "   (SIN RED)"
                    l.configure(text=f"saldo {d['saldo']:>6.0f}   v{d['version']}{estado}")
                    valores.add(d["saldo"])
            coh = len(valores) == 1
            self.lbl_coherente.configure(text="Replicas coherentes: " + ("SI" if coh else "NO"),
                                         foreground="green" if coh else "#b00020")
        for _, linea in resp.get("bitacora", []):
            self.txt.configure(state="normal")
            self.txt.insert("end", linea + "\n")
            self.txt.configure(state="disabled")
            self.txt.see("end")
        if al_terminar:
            al_terminar(resp)

    def retirar(self):
        try:
            monto = float(self.ent_monto.get())
        except ValueError:
            messagebox.showerror("Monto", "Escriba un monto numerico")
            return
        # El servidor espera en silencio hasta 10 s por si el otro cajero tambien retira;
        # si llegan los dos, los procesa en el mismo instante; si no, sigue solo.
        self.lbl_msg.configure(text="Procesando retiro...", foreground="#555")
        self._en_hilo({"op": "RETIRAR", "ciudad": self.ciudad, "cuenta": self.ent_cuenta.get(),
                       "monto": monto, "modo": self.modo.get()})

    def consignar(self):
        try:
            monto = float(self.ent_monto.get())
        except ValueError:
            messagebox.showerror("Monto", "Escriba un monto numerico")
            return
        self._en_hilo({"op": "CONSIGNAR", "ciudad": self.ciudad, "cuenta": self.ent_cuenta.get(),
                       "monto": monto, "modo": self.modo.get()})

    def consultar(self):
        self._en_hilo({"op": "CONSULTAR", "ciudad": self.ciudad, "cuenta": self.ent_cuenta.get()})

    def alternar_red(self):
        self.red_arriba = not self.red_arriba
        self.lbl_red.configure(text=f"Red: {'ARRIBA' if self.red_arriba else 'CAIDA'}",
                               foreground="green" if self.red_arriba else "red")
        self.btn_red.configure(text="Restablecer red de mi ciudad" if not self.red_arriba
                               else "Simular caida de red de mi ciudad")
        self._en_hilo({"op": "RED", "nodo": self.ciudad, "disponible": self.red_arriba})

    def reiniciar(self):
        self._en_hilo({"op": "REINICIAR", "saldo": 100})


if __name__ == "__main__":
    app = VentanaCajero(CIUDAD, ClienteBanco(HOST, PUERTO))
    app.mainloop()

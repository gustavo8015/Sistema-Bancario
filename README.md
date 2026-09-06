# Banco distribuido: modelo cliente-servidor (2 clientes y 1 servidor)

Guía 5 - S7 - Consistencia y Replicación. Sistemas Distribuidos, Universidad Manuela Beltrán.

## Archivos

- `banco_distribuido.py`  Emulador base: réplicas, coordinador 2PC, replicador asíncrono. También corre solo (`python banco_distribuido.py`) y muestra los tres escenarios en consola.
- `servidor_banco.py`  SERVIDOR. Carga las tres réplicas (Bogotá, Nueva York, Madrid) y atiende a los cajeros por TCP con mensajes JSON.
- `cajero.py`  CLIENTE gráfico (Tkinter). Una ventana por ciudad. Se abren dos: Nueva York y Madrid.
- `diagramas/*.puml`  Diagramas PlantUML (clases, componentes, despliegue, casos de uso, secuencia cliente-servidor, despliegue cliente-servidor).

Solo se necesita Python 3 (sin instalar nada). Tkinter viene incluido con Python en Windows.

## Cómo ejecutarlo (tres terminales)

Abrir la carpeta en VS Code, abrir tres terminales y ejecutar en orden:

```
python servidor_banco.py
python cajero.py "Nueva York"
python cajero.py Madrid
```

Si el servidor está en otro computador de la red: `python cajero.py Madrid 192.168.1.20 5000`
(cambiar la IP por la del servidor y abrir el puerto 5000 en el firewall).

## Guion para la sustentación

1. Escenario 1, consistencia fuerte. En los dos cajeros dejar marcado "Fuerte" y "Retiro simultáneo". Pulsar RETIRAR en el primero (queda "Esperando al otro cajero...") y luego en el segundo. El servidor dispara ambos retiros en el mismo instante. Un cajero muestra "Entregando 100 en efectivo" y el otro "Transacción rechazada". Las tres réplicas quedan en 0 y "Réplicas coherentes: SI".
2. Escenario 2, consistencia eventual. Pulsar "Reiniciar cuenta a 100" en cualquier cajero. Cambiar los dos cajeros a "Eventual" y repetir el retiro simultáneo. Los DOS entregan dinero. Pulsar "Consultar saldo" un segundo después: las réplicas coinciden en -100 (sobregiro).
3. Escenario 3, partición de red. Reiniciar la cuenta. En el cajero de Madrid pulsar "Simular caída de red de mi ciudad", quitar "Retiro simultáneo", modo "Fuerte" y RETIRAR: sale "Transacción cancelada" y ningún saldo cambia. Pulsar "Restablecer red de mi ciudad" y RETIRAR de nuevo: ahora se aprueba.

La bitácora del servidor aparece en la consola del servidor y también dentro de cada ventana de cajero.

## Protocolo (una línea JSON por petición y por respuesta)

Peticiones del cliente:

```
{"op":"RETIRAR","ciudad":"Madrid","cuenta":"1001","monto":100,"modo":"fuerte","sincronizar":true}
{"op":"CONSULTAR","ciudad":"Madrid","cuenta":"1001"}
{"op":"RED","nodo":"Madrid","disponible":false}
{"op":"REINICIAR","saldo":100}
```

Respuesta del servidor:

```
{"ok":true,"estado":"APROBADA","id_tx":"TX-001","mensaje":"Entregando 100 en efectivo",
 "saldos":{"Bogota":{"saldo":0,"version":1,"disponible":true}, ...},
 "bitacora":[[12,"[  313.6 ms] Coordinador  TX-001 -> APROBADA en 3 replicas"], ...]}
```

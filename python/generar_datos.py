"""Genera la base de datos ficticia de una fintech de crédito digital (enero–junio 2026).

Generates the fictional dataset of a digital-lending fintech (January–June 2026).

Todos los datos son inventados. Incluye a propósito problemas de calidad de datos
(pagos duplicados, fechas imposibles, ciudades mal escritas, préstamos con solicitud
rechazada) para demostrar cómo se detectan.

Uso / usage:  python python/generar_datos.py
Salida / output:  data/fintech_demo.db (SQLite)
"""
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
DB = RAIZ / "data" / "fintech_demo.db"
rng = np.random.default_rng(20261103)
CORTE = date(2026, 6, 30)
CORTES = [date(2026, m + 1, 1) - timedelta(1) for m in range(1, 7)]


def tramo(dias):
    if dias == 0:
        return "Al día"
    if dias <= 30:
        return "1-30"
    if dias <= 60:
        return "31-60"
    if dias <= 90:
        return "61-90"
    return "90+"


# ---------------------------------------------------------------- clientes
N = 1800
CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga", "Manizales"]
ciudad = rng.choice(CIUDADES, N, p=[.40, .20, .15, .12, .08, .05])
ocup = rng.choice(["Empleado", "Independiente", "Pensionado", "Estudiante"], N, p=[.5, .3, .1, .1])
edad = rng.integers(22, 60, N)
edad = np.where(ocup == "Estudiante", rng.integers(18, 26, N), edad)
edad = np.where(ocup == "Pensionado", rng.integers(57, 76, N), edad)
score = np.clip(rng.normal(640, 110, N).round(), 150, 950).astype(int)
cli_ids = [f"C{i:05d}" for i in range(1, N + 1)]

# ---------------------------------------------------------------- solicitudes, en orden de fecha
VOL = {1: 560, 2: 520, 3: 610, 4: 720, 5: 800, 6: 850}
sol = []
for m, n in VOL.items():
    ini = date(2026, m, 1)
    dias = (CORTES[m - 1] - ini).days + 1
    for _ in range(n):
        d = ini + timedelta(int(rng.integers(0, dias)))
        if d >= date(2026, 4, 1):  # el canal WhatsApp se lanza en abril
            canal = rng.choice(["Web", "App", "WhatsApp"], p=[.33, .37, .30])
        else:
            canal = rng.choice(["Web", "App"], p=[.55, .45])
        sol.append([d, str(canal)])
sol.sort(key=lambda r: r[0])

w_uni = np.full(N, 1 / N)
w_wa = np.exp(-(score - 640) / 120)  # WhatsApp atrae solicitantes de menor score
w_wa = w_wa / w_wa.sum()
prestamos_cliente = {i: [] for i in range(N)}


def pagado(prestamo, d):
    return sum(v for f, v in prestamo["pagos"] if f <= d)


def calendario_pagos(desemb, venc, plazo, monto, canal, primero, sc):
    """Pagos (fecha, valor) de un préstamo, incluidos los posteriores al corte."""
    logit = -2.4 - 0.012 * (sc - 640) + 0.7 * (canal == "WhatsApp") + 0.6 * primero + 0.2 * (monto > 500_000)
    if rng.random() < 1 / (1 + np.exp(-logit)):
        if rng.random() < 0.6:
            return []  # nunca paga
        f = venc + timedelta(int(rng.integers(-3, 21)))
        return [(max(f, desemb + timedelta(1)), int(round(monto * rng.uniform(.3, .6), -3)))]
    if rng.random() < 0.25:  # paga completo, tarde
        return [(venc + timedelta(int(rng.integers(1, 46))), monto)]
    f = desemb + timedelta(int(rng.integers(1, plazo + 1)))
    if rng.random() < 0.2:  # en dos cuotas, a tiempo
        p1 = int(round(monto * rng.uniform(.3, .7), -3))
        f2 = f + timedelta(int(rng.integers(0, max(1, (venc - f).days) + 1)))
        return [(f, p1), (f2, monto - p1)]
    return [(f, monto)]


solicitudes, prestamos = [], []
for k, (d, canal) in enumerate(sol, start=1):
    c = int(rng.choice(N, p=w_wa if canal == "WhatsApp" else w_uni))
    previos = prestamos_cliente[c]
    activo = any(p["desemb"] <= d and pagado(p, d) < p["monto"] for p in previos)
    primero = not any(pagado(p, d) >= p["monto"] for p in previos)
    if primero:
        monto = int(rng.choice([100, 150, 200, 250])) * 1000
    else:
        opciones = np.arange(200, 1001, 50)
        prob = 1 / opciones
        monto = int(rng.choice(opciones, p=prob / prob.sum())) * 1000
    sid = f"S{k:06d}"
    if activo:
        decision, motivo = "Rechazada", "Crédito activo"
    elif score[c] + rng.normal(0, 45) >= (580 if primero else 550):
        decision, motivo = "Aprobada", ""
    else:
        decision = "Rechazada"
        motivo = str(rng.choice(["Score bajo", "Capacidad de pago", "Validación de identidad"], p=[.6, .3, .1]))
    solicitudes.append(dict(solicitud_id=sid, cliente_id=cli_ids[c], fecha_solicitud=d, canal=canal,
                            monto_solicitado=monto, decision=decision, motivo_rechazo=motivo))
    if decision == "Aprobada":
        desemb = min(d + timedelta(int(rng.random() < .3)), CORTE)
        plazo = int(rng.choice([5, 10, 15, 20, 30], p=[.1, .2, .25, .2, .25]))
        venc = desemb + timedelta(plazo)
        numero = len(previos) + 1
        p = dict(desemb=desemb, monto=monto,
                 pagos=calendario_pagos(desemb, venc, plazo, monto, canal, primero, score[c]))
        previos.append(p)
        prestamos.append(dict(prestamo_id=f"P{len(prestamos) + 1:06d}", solicitud_id=sid, cliente_id=cli_ids[c],
                              producto="Flex" if (numero == 1 or monto <= 500_000) else "Plazo",
                              fecha_desembolso=desemb, fecha_vencimiento=venc, plazo_dias=plazo, monto=monto,
                              tasa_ea=0.26, numero_credito=numero, _p=p))

# ---------------------------------------------------------------- pagos reales hasta el corte
pagos = []
for p in prestamos:
    for f, v in p["_p"]["pagos"]:
        if f <= CORTE:
            pagos.append(dict(prestamo_id=p["prestamo_id"], fecha_pago=f, valor=int(v),
                              medio=str(rng.choice(["PSE", "Efectivo", "Corresponsal", "Transferencia"], p=[.5, .25, .1, .15]))))
pagos.sort(key=lambda r: (r["fecha_pago"], r["prestamo_id"]))
for i, r in enumerate(pagos, start=1):
    r["pago_id"] = f"PG{i:06d}"

# ---------------------------------------------------------------- foto de la cartera al cierre de cada mes
cierre = []
for corte in CORTES:
    for p in prestamos:
        if p["fecha_desembolso"] > corte:
            continue
        saldo = p["monto"] - sum(v for f, v in p["_p"]["pagos"] if f <= corte)
        if saldo > 0:
            dm = max(0, (corte - p["fecha_vencimiento"]).days)
            cierre.append(dict(fecha_corte=corte, prestamo_id=p["prestamo_id"], saldo=int(saldo),
                               dias_mora=dm, tramo_mora=tramo(dm)))

primera_solicitud = {}
for s in solicitudes:
    primera_solicitud.setdefault(s["cliente_id"], s["fecha_solicitud"])
clientes = []
for i, cid in enumerate(cli_ids):
    base = primera_solicitud.get(cid)
    registro = base - timedelta(int(rng.integers(0, 31))) if base else date(2025, 7, 1) + timedelta(int(rng.integers(0, 365)))
    clientes.append(dict(cliente_id=cid, fecha_registro=registro, ciudad=str(ciudad[i]), edad=int(edad[i]),
                         ocupacion=str(ocup[i]), score_buro=int(score[i])))

df_cli = pd.DataFrame(clientes)
df_sol = pd.DataFrame(solicitudes)
df_pre = pd.DataFrame([{k: v for k, v in p.items() if not k.startswith("_")} for p in prestamos])
df_pag = pd.DataFrame(pagos)[["pago_id", "prestamo_id", "fecha_pago", "valor", "medio"]]
df_cie = pd.DataFrame(cierre)

# ================================================================ problemas de calidad, a propósito
# 1) ciudades mal escritas
VARIANTES = {"Bogotá": ["Bogota", "BOGOTÁ", "bogotá ", "Bogotá D.C."], "Medellín": ["Medellin", "MEDELLÍN", " Medellín"],
             "Cali": ["CALI", "Cali "], "Barranquilla": ["Barranquila", "BARRANQUILLA"],
             "Bucaramanga": ["Bucaramanga ", "BUCARAMANGA"], "Manizales": ["manizales"]}
for i in rng.choice(N, int(N * .08), replace=False):
    df_cli.loc[i, "ciudad"] = str(rng.choice(VARIANTES[df_cli.loc[i, "ciudad"]]))
# 2) pagos duplicados
duplicados = rng.choice(df_pag.drop_duplicates("prestamo_id").index, 30, replace=False)
# 3) pagos con fecha anterior al desembolso
desembolso = df_pre.set_index("prestamo_id")["fecha_desembolso"]
for i in rng.choice(df_pag.index.difference(duplicados), 6, replace=False):
    df_pag.loc[i, "fecha_pago"] = desembolso[df_pag.loc[i, "prestamo_id"]] - timedelta(int(rng.integers(1, 11)))
df_pag = pd.concat([df_pag, df_pag.loc[duplicados]]).sort_values(["fecha_pago", "pago_id"]).reset_index(drop=True)
# 4) préstamos cuya solicitud figura como rechazada
rechazadas = rng.choice(df_pre["solicitud_id"].values, 4, replace=False)
df_sol.loc[df_sol["solicitud_id"].isin(rechazadas), ["decision", "motivo_rechazo"]] = ["Rechazada", "Score bajo"]

# ================================================================ salida
DB.parent.mkdir(exist_ok=True)
DB.unlink(missing_ok=True)
tablas = {"clientes": df_cli, "solicitudes": df_sol, "prestamos": df_pre, "pagos": df_pag, "cartera_cierre": df_cie}
with sqlite3.connect(DB) as con:
    for nombre, df in tablas.items():
        out = df.copy()
        for col in out.columns:
            if col.startswith("fecha"):
                out[col] = pd.to_datetime(out[col]).dt.strftime("%Y-%m-%d")
        out.to_sql(nombre, con, index=False)
print({k: len(v) for k, v in tablas.items()}, "->", DB.relative_to(RAIZ))

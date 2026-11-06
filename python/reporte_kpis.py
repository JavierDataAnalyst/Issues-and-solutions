"""Reporte mensual de KPIs con limpieza, conciliación y un control que puede fallar.

Monthly KPI report with cleaning, reconciliation and a control that can fail.

1. Lee las tablas de data/fintech_demo.db.
2. Limpia: normaliza ciudades a 6 valores y elimina pagos duplicados.
3. Concilia: saldo recalculado (monto - pagos) vs. la foto oficial del cierre.
4. Exporta output/reporte_kpis.xlsx con KPIs mensuales y mora por cosecha y canal.
5. Control: si el total del reporte no coincide con la fuente, el proceso falla y no exporta.

Uso / usage:
    python python/reporte_kpis.py            # corrida normal
    python python/reporte_kpis.py --romper   # fuerza el fallo del control, para probarlo
"""
import argparse
import sqlite3
import unicodedata
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
DB = RAIZ / "data" / "fintech_demo.db"
SALIDA = RAIZ / "output" / "reporte_kpis.xlsx"
CORTE = "2026-06-30"
CIUDADES = {"BOGOTA": "Bogotá", "BOGOTA D.C.": "Bogotá", "MEDELLIN": "Medellín", "CALI": "Cali",
            "BARRANQUILLA": "Barranquilla", "BARRANQUILA": "Barranquilla",
            "BUCARAMANGA": "Bucaramanga", "MANIZALES": "Manizales"}


class ControlFallido(Exception):
    pass


def sin_tildes(texto):
    return "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c))


def normalizar_ciudad(valor):
    clave = sin_tildes(str(valor)).strip().upper()
    if clave not in CIUDADES:
        raise ControlFallido(f"Ciudad sin regla de normalización: {valor!r}")
    return CIUDADES[clave]


def cargar():
    with sqlite3.connect(DB) as con:
        t = {n: pd.read_sql(f"SELECT * FROM {n}", con) for n in
             ["clientes", "solicitudes", "prestamos", "pagos", "cartera_cierre"]}
    for df in t.values():
        for col in [c for c in df.columns if c.startswith("fecha")]:
            df[col] = pd.to_datetime(df[col])
    return t


def conciliar(prestamos, pagos, cierre):
    """Préstamos cuyo saldo recalculado no coincide con la foto oficial del corte."""
    pagado = pagos.groupby("prestamo_id")["valor"].sum()
    saldo = prestamos.set_index("prestamo_id")["monto"].sub(pagado, fill_value=0)
    foto = cierre[cierre["fecha_corte"] == CORTE].set_index("prestamo_id")["saldo"]
    return int((saldo != foto.reindex(saldo.index, fill_value=0)).sum())


def main(romper=False):
    t = cargar()
    clientes = t["clientes"].assign(ciudad=t["clientes"]["ciudad"].map(normalizar_ciudad))
    pagos_limpios = t["pagos"].drop_duplicates()
    print(f"Ciudades: {t['clientes']['ciudad'].nunique()} valores crudos -> {clientes['ciudad'].nunique()} normalizados")
    print(f"Pagos duplicados eliminados: {len(t['pagos']) - len(pagos_limpios)}")
    print(f"No cuadran con pagos crudos: {conciliar(t['prestamos'], t['pagos'], t['cartera_cierre'])}")
    print(f"No cuadran sin duplicados:   {conciliar(t['prestamos'], pagos_limpios, t['cartera_cierre'])}")

    sol = t["solicitudes"].assign(mes=t["solicitudes"]["fecha_solicitud"].dt.strftime("%Y-%m"),
                                  aprobada=t["solicitudes"]["decision"].eq("Aprobada"))
    pre = t["prestamos"].merge(t["solicitudes"][["solicitud_id", "canal"]], on="solicitud_id")
    pre["mes"] = pre["fecha_desembolso"].dt.strftime("%Y-%m")

    kpis = (sol.groupby("mes").agg(solicitudes=("solicitud_id", "size"), aprobadas=("aprobada", "sum"))
               .join(pre.groupby("mes").agg(prestamos=("prestamo_id", "size"), desembolsado=("monto", "sum"))))
    kpis["tasa_aprobacion"] = (kpis["aprobadas"] / kpis["solicitudes"]).round(4)

    foto = t["cartera_cierre"].query("fecha_corte == @CORTE and dias_mora > 30")[["prestamo_id", "saldo"]]
    mora = pre.merge(foto, on="prestamo_id", how="left").fillna({"saldo": 0})
    mora = mora.pivot_table(index="mes", columns="canal", values=["saldo", "monto"], aggfunc="sum")
    mora = (mora["saldo"] / mora["monto"]).round(4)

    # Control: el reporte debe sumar exactamente lo mismo que la fuente.
    total_reporte = kpis["desembolsado"].sum() + (1 if romper else 0)
    total_fuente = t["prestamos"]["monto"].sum()
    if total_reporte != total_fuente:
        raise ControlFallido(f"El reporte suma {total_reporte:,} y la fuente {total_fuente:,}: no se exporta")

    SALIDA.parent.mkdir(exist_ok=True)
    with pd.ExcelWriter(SALIDA) as xw:
        kpis.to_excel(xw, sheet_name="KPIs mensuales")
        mora.to_excel(xw, sheet_name="Mora 30+ cosecha y canal")
    print("ok ->", SALIDA.relative_to(RAIZ))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--romper", action="store_true", help="altera el total para comprobar que el control falla")
    main(ap.parse_args().romper)

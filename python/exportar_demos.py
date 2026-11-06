"""Ejecuta los archivos de sql/ contra data/fintech_demo.db y publica data/demos.json para el sitio.

Runs the sql/ files against data/fintech_demo.db and writes data/demos.json for the site.
El sitio muestra exactamente las mismas consultas que producen sus números.

Uso / usage:  python python/exportar_demos.py
Solo usa la librería estándar de Python.
"""
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DB = RAIZ / "data" / "fintech_demo.db"
SALIDA = RAIZ / "data" / "demos.json"
CORTE = date(2026, 6, 30)


def consulta(con, archivo):
    cur = con.execute((RAIZ / "sql" / archivo).read_text(encoding="utf-8"))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, fila)) for fila in cur.fetchall()]


def fin_de_mes(anio_mes):
    y, m = map(int, anio_mes.split("-"))
    return date(y + m // 12, m % 12 + 1, 1) - timedelta(1)


def main():
    con = sqlite3.connect(DB)
    aprobacion = consulta(con, "01_aprobacion_por_canal.sql")
    mora = consulta(con, "02_mora_por_cosecha.sql")
    roll = consulta(con, "03_roll_rate.sql")
    calidad = consulta(con, "04_calidad_de_datos.sql")[0]
    conciliacion = consulta(con, "05_conciliacion_saldos.sql")[0]

    # Una cosecha es comparable en mora 30+ cuando su último crédito (plazo máximo 30 días)
    # pudo llegar a más de 30 días de mora antes del corte: fin de mes + 60 días <= corte.
    cosechas = sorted({r["cosecha"] for r in mora})
    madurez = {c: fin_de_mes(c) + timedelta(60) <= CORTE for c in cosechas}

    # Control: lo publicado debe sumar lo mismo que la fuente, o no se publica.
    total_fuente = con.execute("SELECT SUM(monto) FROM prestamos").fetchone()[0]
    total_publicado = sum(r["desembolsado"] for r in mora)
    if total_publicado != total_fuente:
        raise SystemExit(f"Control fallido: publicado {total_publicado} vs fuente {total_fuente}")

    SALIDA.write_text(json.dumps({
        "corte": CORTE.isoformat(),
        "aprobacion": aprobacion,
        "mora": mora,
        "madurez": madurez,
        "roll": roll,
        "calidad": calidad,
        "conciliacion": conciliacion,
        "totales": {"solicitudes": sum(r["solicitudes"] for r in aprobacion),
                    "prestamos": con.execute("SELECT COUNT(*) FROM prestamos").fetchone()[0],
                    "desembolsado": total_fuente},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print("ok ->", SALIDA.relative_to(RAIZ), {k: len(v) for k, v in [("aprobacion", aprobacion), ("mora", mora), ("roll", roll)]})


if __name__ == "__main__":
    main()

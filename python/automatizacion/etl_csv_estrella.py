"""ETL de archivos CSV a un modelo estrella: detecta el tipo de archivo, limpia, deduplica y carga sin duplicar.

CSV-to-star-schema ETL: detects the file type, cleans, deduplicates and loads idempotently.

- El tipo de archivo se reconoce por sus encabezados, no por su nombre.
- Se normalizan duraciones HH:MM:SS a segundos y nombres de agente con espacios o mayúsculas distintas.
- Se descartan filas de totales y llamadas repetidas (misma llave).
- La carga usa upsert: correr dos veces el mismo archivo deja las mismas filas.
- Al final se detectan días sin datos, para no confundir una carga fallida con un día sin actividad.

Uso / usage:
    python etl_csv_estrella.py --demo             # genera CSV ficticios en una carpeta temporal
    python etl_csv_estrella.py carpeta_csv/ base.db
"""
import argparse
import csv
import random
import sqlite3
import statistics
import tempfile
from datetime import date, timedelta
from pathlib import Path

from comun import registro

log = registro("etl_csv_estrella")

TIPOS = {
    "llamadas": {"id_llamada", "inicio", "agente", "cola", "duracion", "estado"},
    "sesiones": {"agente", "fecha", "conectado"},
}

ESQUEMA = """
CREATE TABLE IF NOT EXISTS dim_agente (agente_id TEXT PRIMARY KEY, nombre TEXT);
CREATE TABLE IF NOT EXISTS dim_fecha (fecha TEXT PRIMARY KEY, anio INT, mes INT, dia_semana INT);
CREATE TABLE IF NOT EXISTS fact_llamada (
    id_llamada TEXT PRIMARY KEY, fecha TEXT REFERENCES dim_fecha, agente_id TEXT REFERENCES dim_agente,
    cola TEXT, segundos INT, estado TEXT);
CREATE TABLE IF NOT EXISTS fact_sesion (
    agente_id TEXT REFERENCES dim_agente, fecha TEXT REFERENCES dim_fecha, segundos_conectado INT,
    PRIMARY KEY (agente_id, fecha));
"""


def detectar_tipo(encabezados):
    columnas = {c.strip().lower() for c in encabezados}
    for tipo, requeridas in TIPOS.items():
        if requeridas <= columnas:
            return tipo
    return None


def a_segundos(hhmmss):
    h, m, s = (int(x) for x in hhmmss.strip().split(":"))
    return h * 3600 + m * 60 + s


def agente_id(nombre):
    return " ".join(nombre.split()).upper()


def cargar_archivo(con, ruta):
    with open(ruta, newline="", encoding="utf-8") as f:
        lector = csv.DictReader(f)
        tipo = detectar_tipo(lector.fieldnames or [])
        if tipo is None:
            log.warning("%s: tipo de archivo desconocido, se omite", ruta.name)
            return tipo, 0
        filas = [{k.strip().lower(): (v or "").strip() for k, v in fila.items()} for fila in lector]
    filas = [f for f in filas if not f["agente"].lower().startswith("total")]  # filas de resumen

    for f in filas:
        aid = agente_id(f["agente"])
        con.execute("INSERT INTO dim_agente VALUES (?, ?) ON CONFLICT(agente_id) DO NOTHING", (aid, aid.title()))
        dia = date.fromisoformat((f.get("inicio") or f.get("fecha"))[:10])
        con.execute("INSERT INTO dim_fecha VALUES (?, ?, ?, ?) ON CONFLICT(fecha) DO NOTHING",
                    (dia.isoformat(), dia.year, dia.month, dia.isoweekday()))
        if tipo == "llamadas":
            con.execute("""INSERT INTO fact_llamada VALUES (?, ?, ?, ?, ?, ?)
                           ON CONFLICT(id_llamada) DO UPDATE SET segundos = excluded.segundos, estado = excluded.estado""",
                        (f["id_llamada"], dia.isoformat(), aid, f["cola"], a_segundos(f["duracion"]), f["estado"].lower()))
        else:
            con.execute("""INSERT INTO fact_sesion VALUES (?, ?, ?)
                           ON CONFLICT(agente_id, fecha) DO UPDATE SET segundos_conectado = excluded.segundos_conectado""",
                        (aid, dia.isoformat(), a_segundos(f["conectado"])))
    return tipo, len(filas)


def dias_sin_datos(con, desde, hasta):
    con_datos = {r[0] for r in con.execute("SELECT DISTINCT fecha FROM fact_llamada")}
    dias = (desde + timedelta(d) for d in range((hasta - desde).days + 1))
    return [d.isoformat() for d in dias if d.isoweekday() <= 5 and d.isoformat() not in con_datos]


def generar_demo(carpeta, desde, dias=10):
    """CSV ficticios con los problemas de siempre: duplicados, espacios, mayúsculas, totales y un día faltante."""
    azar = random.Random(11)
    agentes = ["ana ruiz", "Luis  Peña", "MARTA GIL"]
    for d in range(dias):
        dia = desde + timedelta(d)
        if dia.isoweekday() > 5 or d == 4:  # fines de semana y un día que "no llegó"
            continue
        llamadas = [[f"L{dia:%m%d}{i:03d}", f"{dia} 0{azar.randint(7, 9)}:{azar.randint(10, 59)}:00", azar.choice(agentes),
                     azar.choice(["cobranza", "servicio"]), f"00:0{azar.randint(1, 9)}:{azar.randint(10, 59)}",
                     azar.choice(["Atendida", "Atendida", "Perdida"])] for i in range(30)]
        llamadas += llamadas[:2] + [["", f"{dia} 23:59:00", "Totales", "", "05:00:00", ""]]
        with open(carpeta / f"export_{dia:%Y%m%d}_a.csv", "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows([["Id_Llamada", "Inicio", "Agente", "Cola", "Duracion", "Estado"], *llamadas])
        with open(carpeta / f"export_{dia:%Y%m%d}_b.csv", "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows([["Agente", "Fecha", "Conectado"],
                                     *[[a, dia.isoformat(), f"0{azar.randint(6, 9)}:{azar.randint(10, 59)}:00"] for a in agentes]])


def correr(carpeta, base):
    con = sqlite3.connect(base)
    con.executescript(ESQUEMA)
    for ruta in sorted(carpeta.glob("*.csv")):
        tipo, n = cargar_archivo(con, ruta)
        log.info("%s -> %s (%d filas)", ruta.name, tipo, n)
    con.commit()
    return con


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("carpeta", nargs="?")
    ap.add_argument("base", nargs="?", default="estrella.db")
    ap.add_argument("--demo", action="store_true")
    args = ap.parse_args()

    if args.demo:
        tmp = Path(tempfile.mkdtemp(prefix="demo_etl_"))
        desde = date(2026, 6, 1)
        generar_demo(tmp, desde)
        con = correr(tmp, tmp / "estrella.db")
        antes = con.execute("SELECT COUNT(*) FROM fact_llamada").fetchone()[0]
        con = correr(tmp, tmp / "estrella.db")  # segunda corrida: mismas filas
        despues = con.execute("SELECT COUNT(*) FROM fact_llamada").fetchone()[0]
        print(f"Llamadas cargadas: {antes} (segunda corrida: {despues}, sin duplicar)")
        print("Por estado:", dict(con.execute("SELECT estado, COUNT(*) FROM fact_llamada GROUP BY estado")))
        horas = [s / 3600 for (s,) in con.execute("SELECT segundos_conectado FROM fact_sesion")]
        print(f"Mediana de horas conectadas por agente y día: {statistics.median(horas):.1f}")
        print("Días hábiles sin datos:", dias_sin_datos(con, desde, desde + timedelta(9)))
    else:
        if not args.carpeta:
            ap.error("indica la carpeta de CSV o usa --demo")
        correr(Path(args.carpeta), args.base)


if __name__ == "__main__":
    main()

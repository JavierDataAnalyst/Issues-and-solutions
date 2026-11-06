"""Alerta cuando NO llegan datos: el fallo más peligroso no es el que da error, sino el que no hace nada.

Alerts when data does NOT arrive: the most dangerous failure is the silent one.

Revisa la fecha del último dato cargado y los días hábiles sin datos. Si algo falta, envía un correo
(o lo imprime en modo demo) y termina con código 2, para que el programador de tareas lo marque en rojo.

Uso / usage:
    python alerta_sin_datos.py --demo
    python alerta_sin_datos.py --base estrella.db --tabla fact_llamada --columna fecha --max-dias 1
"""
import argparse
import smtplib
import sqlite3
import sys
from datetime import date, timedelta
from email.message import EmailMessage

from comun import cargar_env, registro, requerir

log = registro("alerta_sin_datos")


def dias_habiles(desde, hasta):
    return [desde + timedelta(d) for d in range((hasta - desde).days + 1) if (desde + timedelta(d)).isoweekday() <= 5]


def revisar(fechas_con_datos, hoy, max_dias=1, ventana=10):
    """Devuelve la lista de problemas encontrados; vacía si todo está en orden."""
    problemas = []
    if not fechas_con_datos:
        return ["No hay ningún dato cargado."]
    ultima = max(fechas_con_datos)
    atraso = len(dias_habiles(ultima + timedelta(1), hoy - timedelta(1)))
    if atraso > max_dias:
        problemas.append(f"El último dato es del {ultima}: {atraso} días hábiles sin carga.")
    huecos = [d for d in dias_habiles(hoy - timedelta(ventana), ultima) if d not in fechas_con_datos]
    if huecos:
        problemas.append("Días hábiles sin datos: " + ", ".join(d.isoformat() for d in huecos))
    return problemas


def leer_fechas(base, tabla, columna):
    if not tabla.isidentifier() or not columna.isidentifier():
        raise SystemExit("Nombre de tabla o columna inválido")
    with sqlite3.connect(base) as con:
        return {date.fromisoformat(r[0][:10]) for r in con.execute(f"SELECT DISTINCT {columna} FROM {tabla}")}


def enviar_correo(asunto, cuerpo):
    cargar_env()
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = asunto, requerir("SMTP_USER"), requerir("ALERTA_PARA")
    msg.set_content(cuerpo)
    with smtplib.SMTP(requerir("SMTP_HOST"), int(requerir("SMTP_PORT")), timeout=30) as s:
        s.starttls()
        s.login(requerir("SMTP_USER"), requerir("SMTP_PASSWORD"))
        s.send_message(msg)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--base")
    ap.add_argument("--tabla", default="fact_llamada")
    ap.add_argument("--columna", default="fecha")
    ap.add_argument("--max-dias", type=int, default=1)
    args = ap.parse_args()

    if args.demo:
        hoy = date(2026, 6, 12)
        fechas = set(dias_habiles(date(2026, 6, 1), date(2026, 6, 9))) - {date(2026, 6, 5)}  # falta un día y no llegó lo de ayer
    else:
        if not args.base:
            ap.error("indica --base o usa --demo")
        hoy, fechas = date.today(), leer_fechas(args.base, args.tabla, args.columna)

    problemas = revisar(fechas, hoy, args.max_dias)
    if not problemas:
        log.info("Datos al día")
        return
    cuerpo = "\n".join(f"- {p}" for p in problemas)
    if args.demo:
        print("ALERTA (modo demo, no se envía correo):\n" + cuerpo)
    else:
        enviar_correo("Alerta: faltan datos en la carga diaria", cuerpo)
        log.warning("Alerta enviada:\n%s", cuerpo)
    sys.exit(2)


if __name__ == "__main__":
    main()

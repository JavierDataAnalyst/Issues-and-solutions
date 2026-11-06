"""Utilidades compartidas: variables de entorno y registro de eventos.

Shared helpers: environment variables and logging.

Las credenciales nunca van en el código: se leen del entorno o de un archivo .env local
que no se sube al repositorio (ver .env.example).
"""
import logging
import os
from pathlib import Path


def cargar_env(ruta=Path(__file__).with_name(".env")):
    """Carga un .env sencillo (CLAVE=valor) sin pisar variables que ya existan en el entorno."""
    if not ruta.exists():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


def requerir(nombre):
    """Devuelve la variable de entorno o falla con un mensaje claro (nunca con un valor por defecto inventado)."""
    valor = os.environ.get(nombre)
    if not valor:
        raise SystemExit(f"Falta la variable de entorno {nombre}. Revisa .env.example.")
    return valor


def registro(nombre):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger(nombre)

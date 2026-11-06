"""Despacho diario de archivos por SFTP: idempotente, con reintentos, evidencia y verificación del servidor.

Daily SFTP file dispatch: idempotent, with retries, evidence and server-key verification.

- Nombres fijos por fecha: repetir el envío sobrescribe el mismo archivo, no lo duplica.
- Una marca de "ya enviado" por fecha evita despachar y notificar dos veces.
- La llave del servidor se verifica contra known_hosts (RejectPolicy): nunca se acepta una llave desconocida.
- Cada archivo enviado queda en un manifiesto (nombre, tamaño, sha256, hora) como evidencia para auditoría.

Uso / usage:
    python despacho_sftp.py --demo                  # "sube" a una carpeta local, sin red
    python despacho_sftp.py --fecha 2026-11-06      # real: SFTP_* del entorno, autenticación con llave
"""
import argparse
import csv
import hashlib
import json
import random
import shutil
import tempfile
import time
from datetime import date, datetime
from pathlib import Path

from comun import cargar_env, registro, requerir

log = registro("despacho_sftp")


def preparar_archivos(fecha, carpeta):
    """Genera el lote del día con datos ficticios. El nombre depende solo de la fecha."""
    azar = random.Random(fecha)
    ruta = carpeta / f"cartas_{fecha}.csv"
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cuenta", "tipo_carta", "dias_mora", "saldo"])
        for i in range(1, 26):
            w.writerow([f"CTA-{i:05d}", azar.choice(["recordatorio", "aviso_mora"]), azar.randint(5, 90), azar.randint(100, 900) * 1000])
    return [ruta]


class DestinoLocal:
    """Destino de demostración: una carpeta local que hace las veces de servidor."""

    def __init__(self, carpeta, fallas=1):
        self.carpeta = carpeta
        self.fallas = fallas  # simula una caída de red en el primer intento

    def __enter__(self):
        if self.fallas:
            self.fallas -= 1
            raise OSError("conexión rechazada (simulada)")
        self.carpeta.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, *exc):
        return False

    def subir(self, local):
        shutil.copy2(local, self.carpeta / local.name)


class DestinoSFTP:
    """Conexión SFTP real con paramiko, autenticación por llave y llave del servidor verificada."""

    def __init__(self):
        cargar_env()
        self.host = requerir("SFTP_HOST")
        self.puerto = int(requerir("SFTP_PORT"))
        self.usuario = requerir("SFTP_USER")
        self.llave = str(Path(requerir("SFTP_KEY_PATH")).expanduser())
        self.known_hosts = str(Path(requerir("SFTP_KNOWN_HOSTS")).expanduser())
        self.remoto = requerir("SFTP_REMOTE_DIR").rstrip("/")

    def __enter__(self):
        import paramiko

        self.ssh = paramiko.SSHClient()
        self.ssh.load_host_keys(self.known_hosts)
        self.ssh.set_missing_host_key_policy(paramiko.RejectPolicy())  # nunca AutoAddPolicy
        self.ssh.connect(self.host, port=self.puerto, username=self.usuario, key_filename=self.llave, timeout=30)
        self.sftp = self.ssh.open_sftp()
        return self

    def __exit__(self, *exc):
        self.sftp.close()
        self.ssh.close()
        return False

    def subir(self, local):
        self.sftp.put(str(local), f"{self.remoto}/{local.name}")


def sha256(ruta):
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def despachar(fecha, destino, carpeta_trabajo, intentos=3, espera=2.0, forzar=False):
    marca = carpeta_trabajo / f".enviado_{fecha}"
    if marca.exists() and not forzar:
        log.info("El lote del %s ya se envió (%s); no se repite", fecha, marca.name)
        return False
    archivos = preparar_archivos(fecha, carpeta_trabajo)
    for intento in range(1, intentos + 1):
        try:
            with destino() as d:
                for a in archivos:
                    d.subir(a)
            break
        except OSError as e:
            if intento == intentos:
                raise
            log.warning("Intento %d/%d falló (%s); reintento en %.1f s", intento, intentos, e, espera)
            time.sleep(espera)
    with open(carpeta_trabajo / "manifiesto.jsonl", "a", encoding="utf-8") as m:
        for a in archivos:
            m.write(json.dumps({"archivo": a.name, "bytes": a.stat().st_size, "sha256": sha256(a),
                                "enviado": datetime.now().isoformat(timespec="seconds")}) + "\n")
    marca.write_text(datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
    log.info("Lote del %s enviado: %s", fecha, ", ".join(a.name for a in archivos))
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--demo", action="store_true", help="envía a una carpeta local, sin red ni credenciales")
    ap.add_argument("--fecha", default=date.today().isoformat())
    ap.add_argument("--forzar", action="store_true", help="reenvía aunque el lote ya tenga marca")
    args = ap.parse_args()

    if args.demo:
        base = Path(tempfile.mkdtemp(prefix="demo_sftp_"))
        servidor = base / "servidor"
        conexiones = []

        def fabrica():  # la primera conexión falla, para mostrar el reintento
            conexiones.append(1)
            return DestinoLocal(servidor, fallas=1 if len(conexiones) == 1 else 0)

        despachar(args.fecha, fabrica, base, espera=0.1)
        despachar(args.fecha, fabrica, base, espera=0.1)  # segunda corrida: no duplica
        print("Servidor simulado:", sorted(p.name for p in servidor.iterdir()))
        print("Manifiesto:", (base / "manifiesto.jsonl").read_text(encoding="utf-8").strip())
    else:
        despachar(args.fecha, DestinoSFTP, Path.cwd(), forzar=args.forzar)


if __name__ == "__main__":
    main()

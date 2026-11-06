"""Cliente de una API de BI: token OAuth renovable, exportación asíncrona y reintentos.

Client for a BI REST API: refreshable OAuth token, asynchronous export jobs and retries.

1. Pide un access token con el refresh token y lo renueva si la API responde 401.
2. Crea un trabajo de exportación y consulta su estado con espera creciente y un límite de tiempo.
3. Reintenta errores temporales (429 y 5xx) con espera exponencial; cualquier otro error falla de inmediato.

Uso / usage:
    python cliente_api.py --demo                        # sin red: usa una API simulada
    python cliente_api.py --consulta "SELECT ..."       # real: API_BASE_URL y OAUTH_* del entorno
"""
import argparse
import json
import random
import time
from dataclasses import dataclass

from comun import cargar_env, registro, requerir

log = registro("cliente_api")
TEMPORALES = {429, 500, 502, 503, 504}


class ErrorAPI(Exception):
    pass


@dataclass
class Respuesta:
    status: int
    datos: dict


class TransporteHTTP:
    """Transporte real con requests; solo se importa cuando se usa."""

    def __init__(self, base_url, token_url):
        import requests

        self.sesion = requests.Session()
        self.base_url = base_url.rstrip("/")
        self.token_url = token_url

    def token(self, refresh_token, client_id, client_secret):
        r = self.sesion.post(self.token_url, timeout=30, data={
            "grant_type": "refresh_token", "refresh_token": refresh_token,
            "client_id": client_id, "client_secret": client_secret,
        })
        return Respuesta(r.status_code, r.json() if r.content else {})

    def pedir(self, metodo, ruta, token, **kw):
        r = self.sesion.request(metodo, f"{self.base_url}{ruta}", timeout=60,
                                headers={"Authorization": f"Bearer {token}"}, **kw)
        return Respuesta(r.status_code, r.json() if r.content else {})


class APISimulada:
    """API falsa para el modo demo: el primer token vence, hay errores 503 al azar y el trabajo tarda unos ciclos."""

    def __init__(self, semilla=7):
        self.azar = random.Random(semilla)
        self.tokens = 0
        self.trabajos = {}

    def token(self, *_):
        self.tokens += 1
        return Respuesta(200, {"access_token": f"demo-{self.tokens}"})

    def pedir(self, metodo, ruta, token, **kw):
        if token == "demo-1":
            return Respuesta(401, {"error": "token vencido"})
        if self.azar.random() < 0.2:
            return Respuesta(503, {"error": "servicio ocupado"})
        if metodo == "POST" and ruta == "/exports":
            job = f"job-{len(self.trabajos) + 1}"
            self.trabajos[job] = 0
            return Respuesta(202, {"job_id": job})
        partes = ruta.strip("/").split("/")
        if len(partes) == 3 and partes[2] == "status":
            self.trabajos[partes[1]] += 1
            return Respuesta(200, {"status": "COMPLETED" if self.trabajos[partes[1]] >= 3 else "RUNNING"})
        if len(partes) == 3 and partes[2] == "data":
            return Respuesta(200, {"rows": [{"mes": f"2026-0{m}", "desembolsado": 25_000_000 * m} for m in range(1, 4)]})
        return Respuesta(404, {"error": f"ruta desconocida {ruta}"})


class ClienteBI:
    def __init__(self, transporte, refresh_token, client_id, client_secret,
                 intentos=4, espera_base=1.0, dormir=time.sleep):
        self.t = transporte
        self.credenciales = (refresh_token, client_id, client_secret)
        self.intentos = intentos
        self.espera_base = espera_base
        self.dormir = dormir
        self._token = None

    def _token_vigente(self, renovar=False):
        if self._token is None or renovar:
            r = self.t.token(*self.credenciales)
            if r.status != 200 or "access_token" not in r.datos:
                raise ErrorAPI(f"No se pudo obtener el token (HTTP {r.status})")
            self._token = r.datos["access_token"]
        return self._token

    def _llamar(self, metodo, ruta, **kw):
        renovado = False
        for intento in range(1, self.intentos + 1):
            r = self.t.pedir(metodo, ruta, self._token_vigente(), **kw)
            if r.status == 401 and not renovado:
                log.info("Token vencido: se renueva y se repite la llamada")
                self._token_vigente(renovar=True)
                renovado = True
                continue
            if r.status in TEMPORALES and intento < self.intentos:
                espera = self.espera_base * 2 ** (intento - 1)
                log.warning("HTTP %s en %s; reintento %d en %.1f s", r.status, ruta, intento, espera)
                self.dormir(espera)
                continue
            if r.status >= 400:
                raise ErrorAPI(f"{metodo} {ruta}: HTTP {r.status} {r.datos}")
            return r.datos
        raise ErrorAPI(f"{metodo} {ruta}: sin respuesta válida tras {self.intentos} intentos")

    def exportar(self, consulta, limite_seg=600, sondeo_seg=2.0):
        """Crea la exportación, espera a que termine y devuelve las filas."""
        job = self._llamar("POST", "/exports", json={"query": consulta})["job_id"]
        inicio, espera = time.monotonic(), sondeo_seg
        while True:
            estado = self._llamar("GET", f"/exports/{job}/status")["status"]
            if estado == "COMPLETED":
                break
            if estado not in ("QUEUED", "RUNNING"):
                raise ErrorAPI(f"La exportación {job} terminó en estado {estado}")
            if time.monotonic() - inicio > limite_seg:
                raise TimeoutError(f"La exportación {job} no terminó en {limite_seg} s")
            self.dormir(espera)
            espera = min(espera * 1.5, 30)
        filas = self._llamar("GET", f"/exports/{job}/data")["rows"]
        log.info("Exportación %s: %d filas", job, len(filas))
        return filas


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--demo", action="store_true", help="usa la API simulada, sin red ni credenciales")
    ap.add_argument("--consulta", default="SELECT mes, SUM(monto) AS desembolsado FROM prestamos GROUP BY mes")
    args = ap.parse_args()

    if args.demo:
        cliente = ClienteBI(APISimulada(), "demo", "demo", "demo", espera_base=0.01, dormir=lambda s: None)
    else:
        cargar_env()
        cliente = ClienteBI(TransporteHTTP(requerir("API_BASE_URL"), requerir("OAUTH_TOKEN_URL")),
                            requerir("OAUTH_REFRESH_TOKEN"), requerir("OAUTH_CLIENT_ID"), requerir("OAUTH_CLIENT_SECRET"))
    print(json.dumps(cliente.exportar(args.consulta, sondeo_seg=0.01 if args.demo else 2.0), indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()

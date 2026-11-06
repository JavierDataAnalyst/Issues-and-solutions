"""Publica solo agregados y los cifra: una barrera bloquea columnas que identifiquen a una persona.

Publishes only aggregates and encrypts them: a guard blocks any column that identifies a person.

1. Barrera: si los datos traen columnas como nombre, correo, teléfono, documento o dirección, el proceso se detiene.
2. Agrega por las columnas pedidas (sumas y conteos).
3. Cifra el resultado con AES-256-GCM; la llave viene del entorno (LLAVE_DATOS_B64), nunca del código.

Uso / usage:
    python cifrar_resultados.py --demo
    python cifrar_resultados.py --nueva-llave      # imprime una llave nueva para guardarla como secreto
"""
import argparse
import base64
import json
import os
import re
from collections import defaultdict

from comun import cargar_env, requerir

PROHIBIDAS = re.compile(r"nombre|name|correo|email|tel[eé]fono|phone|celular|documento|c[eé]dula|ssn|"
                        r"direcci[oó]n|address|cuenta|account|fecha_nac", re.IGNORECASE)


def verificar_sin_datos_personales(filas):
    columnas = set().union(*(f.keys() for f in filas)) if filas else set()
    malas = sorted(c for c in columnas if PROHIBIDAS.search(c))
    if malas:
        raise ValueError(f"Columnas con datos personales, no se publica: {', '.join(malas)}")


def agregar(filas, por, suma):
    grupos = defaultdict(lambda: {"registros": 0, suma: 0})
    for f in filas:
        g = grupos[tuple(f[c] for c in por)]
        g["registros"] += 1
        g[suma] += f[suma]
    return [{**dict(zip(por, k)), **v} for k, v in sorted(grupos.items())]


def cifrar(datos, llave):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    nonce = os.urandom(12)
    return nonce + AESGCM(llave).encrypt(nonce, json.dumps(datos, ensure_ascii=False).encode(), None)


def descifrar(blob, llave):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    return json.loads(AESGCM(llave).decrypt(blob[:12], blob[12:], None))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--nueva-llave", action="store_true")
    args = ap.parse_args()

    if args.nueva_llave:
        print(base64.b64encode(os.urandom(32)).decode())
        return
    if args.demo:
        llave = os.urandom(32)  # llave efímera solo para la demostración
    else:
        cargar_env()
        llave = base64.b64decode(requerir("LLAVE_DATOS_B64"))
        if len(llave) != 32:
            raise SystemExit("LLAVE_DATOS_B64 debe tener 32 bytes (AES-256)")

    detalle = [{"producto": p, "mes": m, "saldo": s} for p, m, s in
               [("Flex", "2026-05", 1_200_000), ("Flex", "2026-05", 800_000), ("Plazo", "2026-05", 3_100_000),
                ("Flex", "2026-06", 950_000), ("Plazo", "2026-06", 2_400_000)]]
    try:
        verificar_sin_datos_personales([{**detalle[0], "correo_cliente": "x"}])
    except ValueError as e:
        print("Barrera activada como se esperaba ->", e)

    verificar_sin_datos_personales(detalle)
    agregados = agregar(detalle, ["producto", "mes"], "saldo")
    blob = cifrar(agregados, llave)
    print(f"Agregados: {len(agregados)} filas -> {len(blob)} bytes cifrados")
    assert descifrar(blob, llave) == agregados, "el descifrado no coincide"
    print("Descifrado verificado:", descifrar(blob, llave))


if __name__ == "__main__":
    main()

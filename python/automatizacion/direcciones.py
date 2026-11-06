"""Normaliza direcciones y decide si una dirección "nueva" es, en realidad, la misma que ya falló.

Normalizes addresses and decides whether a "new" address is really the same one that already bounced.

El riesgo no es solo una dirección mal escrita: es enviar otra vez a una dirección corregida solo en
apariencia (otro orden, otra abreviatura). Por eso se compara por partes (zona, vía y números) además
del texto. Si se parece demasiado a una que falló, la cuenta se retiene; liberarla es decisión de una persona.

Uso / usage:
    python direcciones.py --demo
    python -m unittest test_direcciones.py
"""
import argparse
import re
import unicodedata
from difflib import SequenceMatcher

ABREVIATURAS = {
    "AVENIDA": "AV", "AVDA": "AV", "CARRERA": "CRA", "KR": "CRA", "CALLE": "CL", "CLL": "CL", "DIAGONAL": "DG",
    "TRANSVERSAL": "TV", "URBANIZACION": "URB", "BARRIO": "BRR", "CONJUNTO": "CONJ", "SECTOR": "SECT",
    "APARTAMENTO": "APTO", "APT": "APTO", "EDIFICIO": "EDIF", "INTERIOR": "INT", "NUMERO": "NO", "N": "NO",
}
ZONA = {"URB", "BRR", "CONJ", "SECT"}             # palabras que abren el nombre de la zona
VIA = {"CL", "CRA", "AV", "DG", "TV"}              # palabras que abren la vía
INTERNAS = {"APTO", "EDIF", "INT", "NO", "TORRE"}  # identifican un punto dentro de la zona, no la zona
UMBRAL_TEXTO = 0.80

VEREDICTOS = ("IDENTICA", "MISMO_PUNTO", "MISMA_ZONA_Y_VIA", "TEXTO_PARECIDO", "DISTINTA")


def normalizar(texto):
    """Mayúsculas, sin tildes ni signos, con abreviaturas estándar y espacios simples."""
    if not texto:
        return ""
    s = unicodedata.normalize("NFKD", texto)
    s = "".join(c for c in s if not unicodedata.combining(c)).upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    return " ".join(ABREVIATURAS.get(t, t) for t in s.split())


def partes(normalizada):
    """Separa la dirección normalizada en zona, vía y números."""
    zona, via, actual = [], [], None
    for t in normalizada.split():
        if t in ZONA:
            actual = zona
        elif t in VIA:
            actual = via
            via.append(t)
        elif t in INTERNAS or t.isdigit():
            actual = None if t in INTERNAS else actual
        elif actual is not None:
            actual.append(t)
    numeros = {t for t in normalizada.split() if t.isdigit()}
    return {"zona": frozenset(zona), "via": frozenset(via), "numeros": frozenset(numeros)}


def comparar(nueva, fallida):
    """Veredicto y parecido de texto entre una dirección nueva y una que ya rebotó."""
    a, b = normalizar(nueva), normalizar(fallida)
    parecido = SequenceMatcher(None, " ".join(sorted(a.split())), " ".join(sorted(b.split()))).ratio()
    if a == b:
        return "IDENTICA", parecido
    pa, pb = partes(a), partes(b)
    misma_zona = bool(pa["zona"]) and pa["zona"] == pb["zona"]
    misma_via = bool(pa["via"]) and pa["via"] == pb["via"]
    if misma_zona and misma_via and pa["numeros"] == pb["numeros"]:
        return "MISMO_PUNTO", parecido  # solo cambió el orden o las abreviaturas
    if misma_zona and misma_via:
        return "MISMA_ZONA_Y_VIA", parecido
    if parecido >= UMBRAL_TEXTO:
        return "TEXTO_PARECIDO", parecido
    return "DISTINTA", parecido


def debe_retenerse(nueva, fallidas):
    """La cuenta se retiene si la dirección nueva se parece a cualquiera de las que ya fallaron."""
    peores = [comparar(nueva, f) for f in fallidas]
    veredicto = min(peores, key=lambda v: VEREDICTOS.index(v[0]))[0] if peores else "DISTINTA"
    return veredicto != "DISTINTA", veredicto


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--demo", action="store_true")
    ap.parse_args()
    fallida = "Urbanización Los Pinos, Calle 8 # 14-20, Apartamento 301"
    casos = [
        "Calle 8 No 14 20 Urb. Los Pinos Apto 301",           # mismo punto, otro orden
        "URB LOS PINOS CL 8 14-22 APTO 301",                  # misma zona y vía, otro número
        "Barrio El Prado, Carrera 45 # 30-12",                # dirección realmente distinta
    ]
    print("Dirección que falló:", normalizar(fallida))
    for c in casos:
        retener, veredicto = debe_retenerse(c, [fallida])
        print(f"  {normalizar(c):45} -> {veredicto:17} {'retener' if retener else 'puede enviarse'}")


if __name__ == "__main__":
    main()

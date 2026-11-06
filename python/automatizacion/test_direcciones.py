"""Pruebas de direcciones.py. Correr con: python -m unittest test_direcciones.py"""
import unittest

from direcciones import INTERNAS, ZONA, comparar, debe_retenerse, normalizar

FALLIDA = "Urbanización Los Pinos, Calle 8 # 14-20, Apartamento 301"


class Normalizacion(unittest.TestCase):
    def test_quita_tildes_signos_y_abrevia(self):
        self.assertEqual(normalizar("Urbanización  Los Pinos, Calle 8 #14"), "URB LOS PINOS CL 8 14")

    def test_vacia_no_falla(self):
        self.assertEqual(normalizar(None), "")


class Retencion(unittest.TestCase):
    def test_mismo_punto_en_otro_orden_se_retiene(self):
        retener, veredicto = debe_retenerse("Calle 8 No 14 20 Urb. Los Pinos Apto 301", [FALLIDA])
        self.assertTrue(retener)
        self.assertEqual(veredicto, "MISMO_PUNTO")

    def test_identica_se_retiene(self):
        self.assertEqual(comparar(FALLIDA, FALLIDA)[0], "IDENTICA")

    def test_direccion_realmente_distinta_puede_enviarse(self):
        retener, veredicto = debe_retenerse("Barrio El Prado, Carrera 45 # 30-12", [FALLIDA])
        self.assertFalse(retener)
        self.assertEqual(veredicto, "DISTINTA")

    def test_manda_el_peor_veredicto_entre_varias_fallidas(self):
        retener, _ = debe_retenerse("URB LOS PINOS CL 8 14 20 APTO 301", ["Barrio El Prado Carrera 45", FALLIDA])
        self.assertTrue(retener)


class Reglas(unittest.TestCase):
    """Pruebas que fallan a propósito si alguien rompe una regla de diseño."""

    def test_el_edificio_no_es_una_zona(self):
        # Si EDIF se tratara como zona, dos apartamentos de barrios distintos con el mismo número de
        # edificio parecerían la misma dirección. Esta prueba debe fallar si alguien lo agrega a ZONA.
        self.assertNotIn("EDIF", ZONA)
        self.assertIn("EDIF", INTERNAS)


if __name__ == "__main__":
    unittest.main()

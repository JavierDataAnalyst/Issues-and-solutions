# Automatización con Python / Python automation

**ES** · Ejemplos de los patrones que uso para automatizar procesos de datos, escritos desde cero para este portafolio. Cada script tiene un **modo demo** que corre sin red ni credenciales, con datos ficticios. Ningún código proviene de sistemas de un empleador.

**EN** · Examples of the patterns I use to automate data processes, written from scratch for this portfolio. Every script has a **demo mode** that runs with no network or credentials, on fictional data. No code comes from an employer's systems.

| Script | Qué muestra / What it shows | Demo |
|---|---|---|
| `cliente_api.py` | Token OAuth que se renueva, exportación asíncrona con espera creciente y límite de tiempo, reintentos con backoff exponencial | `python cliente_api.py --demo` |
| `despacho_sftp.py` | Envío idempotente por fecha, reintentos, manifiesto con sha256 como evidencia y **verificación de la llave del servidor** (RejectPolicy) | `python despacho_sftp.py --demo` |
| `etl_csv_estrella.py` | Detecta el tipo de CSV por sus encabezados, limpia, deduplica, carga en un modelo estrella con upsert y detecta días sin datos | `python etl_csv_estrella.py --demo` |
| `direcciones.py` | Normaliza direcciones y decide si una "nueva" es la misma que ya falló; la decisión de liberar es humana | `python direcciones.py --demo` |
| `test_direcciones.py` | Pruebas, incluida una que **falla a propósito** si alguien rompe una regla de diseño | `python -m unittest test_direcciones.py` |
| `alerta_sin_datos.py` | Alerta cuando **no llegan datos** (no solo cuando hay error) y termina con código de salida distinto de 0 | `python alerta_sin_datos.py --demo` |
| `cifrar_resultados.py` | Barrera contra columnas con datos personales, agregación y cifrado AES-256-GCM con llave del entorno | `python cifrar_resultados.py --demo` |
| `ejemplos/workflow_diario.yml` | Ejecución diaria en GitHub Actions con secretos (ejemplo, no activo) | — |

## Seguridad / Security
- Las credenciales se leen del entorno o de un `.env` local (plantilla: `.env.example`); el `.env` nunca se sube.
- SFTP con llave privada y `known_hosts`: una llave de servidor desconocida se rechaza.
- Los resultados que se publican son agregados y van cifrados.

```bash
pip install -r requirements.txt   # solo hace falta para los modos reales y el cifrado
```

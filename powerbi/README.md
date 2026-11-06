# Power BI · dashboard de riesgo / risk dashboard

**ES** · En construcción. Para armarlo sobre los datos ficticios:

1. Corre `python python/generar_datos.py`.
2. En Power BI Desktop: *Obtener datos* → ODBC o el conector de SQLite sobre `data/fintech_demo.db`, o exporta las tablas a CSV.
3. Relaciones: `Calendario[Date]` → `prestamos[fecha_desembolso]`; `prestamos[prestamo_id]` → `cartera_cierre[prestamo_id]`; `solicitudes[solicitud_id]` → `prestamos[solicitud_id]`.
4. Pega las medidas de `medidas.dax`.
5. Página sugerida: KPIs, desembolso por mes y canal, matriz de mora 30+ por cosecha y canal, segmentadores de ciudad y primer crédito.

Las capturas se agregarán a esta carpeta.

**EN** · In progress. To build it on the fictional data: run the generator, load `data/fintech_demo.db` (or CSV exports), create the relationships above and paste the measures from `medidas.dax`. Screenshots will be added to this folder.

# Business Issues & SQL Solutions

### [Acerca de lo que soy y dónde estoy](https://www.youtube.com/watch?v=WbwHtW0s6B8)

### 🔗 [Portafolio interactivo: proyectos de BI y riesgo de crédito con demos](https://javierdataanalyst.github.io/Issues-and-solutions/)

Aquí comparto cómo convierto **preguntas de negocio** en **respuestas accionables** usando datos.

![Data](https://d2nzy1qhita6w.cloudfront.net/media/magefan_blog/data-analysis-skills-duties-responsibilities.jpeg)


## Sobre mí
Soy analista de datos enfocado en **resolver dudas de negocio** con **SQL**. Mi fuerte es traducir necesidades de marketing, ventas y operaciones en consultas eficientes que limpian, relacionan y explican los datos con claridad.  
Trabajo mucho con **window functions**, **CTEs**, deduplicación, estandarización (E.164) y construcción de **funnels** para tomar decisiones con evidencia, no con suposiciones.

## Experiencia
**Analista de BI y Riesgo de Crédito** · financiera de crédito al consumo en Puerto Rico  
Soy el único analista de datos de la empresa y llevo los proyectos de punta a punta: de la API y el SQL al dashboard, la automatización y la conversación con gerencia, del CEO a contabilidad.

**Formación:** Ingeniero de producción, con 3 años de experiencia en datos en banca y marketing.

Resultados destacados:
- **Certificado de la línea de crédito bancaria**: lo recalculé desde la fuente y lo dejé auditable; las filas que cuadran pasaron de **269 a 301** y consultas de auditoría independientes reproducen **441 de 441** cifras.
- **Dashboard para inversionistas**: **10 de 10** secciones del data request en un solo tablero, cobertura de FICO de **72 % a 99 %** y solo datos agregados.
- **Cartas de cobro automatizadas**: **0** cartas enviadas a cuentas retenidas gracias a una regla de similitud de direcciones con revisión humana.
- **Call center**: mostré que el problema no era falta de personal sino presencia en horas pico (nivel de servicio **~94 %**, ocupación **5–16 %**).

## Qué encontrarás en este repositorio
- **Casos reales**: lead scoring sin ML, referidos & NPS, funnels unificados.
- **Limpieza y estandarización**: teléfonos en E.164, normalización de campos, de-dup.
- **Análisis**: cohortes, funciones ventana, métricas de conversión y retención.
- **Performance**: cuándo usar CTE vs subconsulta, índices y orden de ejecución.
- **Visualización**: salidas listas para **Power BI / Zoho Analytics**.
- **Portafolio de BI y riesgo de crédito**: sitio bilingüe con demos interactivas, SQL y Python sobre una fintech ficticia (`index.html`, `sql/`, `python/`, `powerbi/`).

## Proyectos destacados
- **Lead Scoring (SQL puro)** – priorización inicial sin modelos predictivos. → `[link]`
- **Programa de Referidos + NPS** – pipeline, segmentación y métricas clave. → `[link]`
- **Funnel Outbound Unificado (Zoho CRM)** – tracking de etapas y tasas. → `[link]`

## Proyectos profesionales de BI y riesgo de crédito
Descritos sin nombres de clientes, bancos, contrapartes ni montos. El detalle de cada uno está en el [portafolio](https://javierdataanalyst.github.io/Issues-and-solutions/#proyectos).

| Proyecto | Qué resolví | Herramientas |
|---|---|---|
| Certificado de borrowing base | Certificado mensual armado a mano y no reproducible → recalculado desde la fuente, auditable y con hallazgos llevados a gerencia | Python, SQL, Zoho Analytics, Next.js |
| Dashboard para inversionistas | Data request de 10 secciones respondido a mano → flujo automático que solo publica agregados validados | Python, Zoho Analytics API, Next.js |
| Modelo de datos por préstamo | Dos sistemas sin conexión → una fila por préstamo, sin duplicados, base de todos los reportes | SQL (CTE, window functions) |
| Estratificaciones para agencias calificadoras | 17 tablas exactas al corte → generador reproducible y Excel auditable con 0 errores de fórmula | Python, SQL, Excel |
| Modelo de roll rate | Sin visibilidad de la migración entre tramos de mora → matriz de 6 tramos por conteo o saldo | SQL, PostgreSQL, Next.js |
| Analítica de call center | Datos atrapados en CSV → modelo estrella, corrección de sesgos y recomendación de cobertura | Python, PostgreSQL, Power Automate |
| Cartas de cobro y control de direcciones | Cartas devueltas sin reacción → despacho automático, analizador USPS y retención con revisión humana | Python, SFTP, SQL |
| Dashboard financiero en Power BI | Rentabilidad revisada a mano → modelo estrella, Power Query y DAX validado con contabilidad | Power BI, DAX, Power Query |

## Demos y código de este repositorio
| Carpeta | Contenido |
|---|---|
| `index.html`, `assets/` | Portafolio bilingüe (GitHub Pages) |
| `sql/` | Aprobación por canal, mora por cosecha, roll rate, calidad de datos y conciliación |
| `python/` | Generador de datos ficticios, exportador de demos y reporte de KPIs con pandas |
| `powerbi/` | Medidas DAX del dashboard de riesgo |
| `data/` | `fintech_demo.db` (SQLite ficticio) y `demos.json` |

```bash
pip install -r requirements.txt
python python/generar_datos.py           # crea data/fintech_demo.db
python python/exportar_demos.py          # corre sql/ y publica data/demos.json
python python/reporte_kpis.py            # Excel de KPIs con un control
python python/reporte_kpis.py --romper   # el control falla a propósito
python -m http.server 8000               # abre http://localhost:8000
```

Todos los datos de las demos son inventados y traen problemas de calidad a propósito (pagos duplicados, fechas imposibles, ciudades mal escritas) para mostrar cómo se detectan.

## Stack
**SQL** (PostgreSQL/MySQL/Zoho), **Power BI**, **Zoho Analytics**, **Python (pandas)**.

También: **DAX y Power Query**, **Excel**, **Python** (requests, APIs, automatización), **Next.js / Vercel**, **Supabase**, **Git y GitHub Actions**, **IA (Claude Code)** con validación independiente.

## Contacto
**LinkedIn:** `https://www.linkedin.com/in/javier-daza-5201b1176/` · **Email:** `javier.m.daza.p@gmail.com`

---

### English summary
BI & Credit Risk Analyst at a consumer lender. I turn business questions into reliable numbers with SQL, Python, Power BI and Excel. This repo includes a bilingual [interactive portfolio](https://javierdataanalyst.github.io/Issues-and-solutions/) with credit-risk demos (approval mix, vintages, roll rates, data-quality checks) built on a fictional fintech, plus the SQL, Python and DAX behind them. Professional projects are described without client, bank or counterparty names or amounts.

// Contenido bilingüe del portafolio. Texto en español (es) e inglés (en).
// Regla de privacidad: sin nombres de clientes, bancos, contrapartes, montos ni identificadores internos.

const CONFIG = {
  repoUrl: "https://github.com/JavierDataAnalyst/Issues-and-solutions",
  githubUrl: "https://github.com/JavierDataAnalyst",
  linkedinUrl: "https://www.linkedin.com/in/javier-daza-5201b1176/",
  email: "javier.m.daza.p@gmail.com",
  cvUrl: null, // p. ej. "cv/Javier_Daza_CV.pdf" cuando esté el PDF
};

const UI = {
  es: {
    navProfile: "Perfil", navExperience: "Experiencia", navProjects: "Proyectos", navDemos: "Demos", navCode: "Código", navPowerbi: "Power BI",
    brandKicker: "Portafolio · Hoja de vida",
    fullName: "Javier Moisés Daza Padilla",
    role: "Analista de BI · Riesgo de crédito y portafolio",
    location: "Bogotá, Colombia · remoto",
    tagline: "Convierto datos de crédito en cifras confiables y en decisiones. SQL, Python, Power BI y Excel, con IA y controles que pueden fallar.",
    bio: "Analista de BI en portafolios de crédito de consumo. Construyo la fuente única de verdad del portafolio, las métricas de riesgo (roll rates, mora por tramos, cosechas, UPB) y la reportería de covenants para financiadores, y automatizo procesos con Python. Perfil híbrido de negocio e ingeniería, con comunicación ejecutiva en español e inglés.",
    privacy: "Proyectos descritos sin nombres de clientes, bancos ni montos. Todas las demos usan datos ficticios.",
    stats: [
      ["+30", "dashboards en producción", "finanzas, operaciones, ventas y marketing"],
      ["441/441", "cifras reproducidas", "por una auditoría independiente"],
      ["5–16 %", "ocupación del call center", "la decisión pasó de contratar más a cubrir las horas pico"],
      ["> 100 %", "tasa de fondeo errónea detectada", "corregida antes de llegar a los inversionistas"],
    ],
    summaryKicker: "Perfil · resumen",
    summaryTitle: "Del dato crudo a la decisión, con cifras que se pueden auditar",
    summarySub: "Lo que aporto a un equipo de BI y riesgo de crédito.",
    experienceTitle: "Experiencia",
    educationTitle: "Educación", inProgress: "En curso",
    projectsTitle: "Proyectos",
    projectsIntro: "Situación, resultado y lo que hice. Empresa actual: financiera de crédito al consumo en Puerto Rico.",
    problem: "Situación", actions: "Lo que hice", results: "Resultado", stack: "Herramientas",
    demosTitle: "Demos con datos ficticios",
    demosIntro: "Una fintech de crédito digital inventada, de enero a junio de 2026, generada con Python. Los datos traen problemas de calidad a propósito. Cada demo se calcula con el SQL que aparece debajo.",
    viewSql: "Ver el SQL", loading: "Cargando…",
    codeTitle: "Código",
    codeIntro: "SQL y Python escritos para este portafolio sobre los datos ficticios. Nada proviene de sistemas de un empleador.",
    openGithub: "Abrir en GitHub",
    powerbiTitle: "Power BI",
    powerbiIntro: "En construcción: un dashboard de riesgo en Power BI sobre los mismos datos ficticios. Estas son las medidas DAX del modelo.",
    cvSoon: "CV · pronto", cvDownload: "Descargar CV",
    footer: "Demos con datos ficticios generados con python/generar_datos.py. Proyectos descritos sin nombres de clientes, bancos, contrapartes ni montos.",
    months: ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul"],
    total: "Total", notMature: "sin madurar", count: "Préstamos", balance: "Saldo", from: "Desde", to: "Hacia",
    paid: "Pagado",
  },
  en: {
    navProfile: "Profile", navExperience: "Experience", navProjects: "Projects", navDemos: "Demos", navCode: "Code", navPowerbi: "Power BI",
    brandKicker: "Portfolio · Resume",
    fullName: "Javier Moisés Daza Padilla",
    role: "BI Analyst · Credit Risk & Portfolio",
    location: "Bogotá, Colombia · remote",
    tagline: "I turn lending data into reliable numbers and decisions. SQL, Python, Power BI and Excel, with AI and controls that can actually fail.",
    bio: "BI analyst for consumer-credit portfolios. I build the portfolio's single source of truth, the risk metrics (roll rates, delinquency buckets, vintages, UPB) and covenant reporting for funders, and I automate processes with Python. A hybrid business and engineering profile with executive communication in Spanish and English.",
    privacy: "Projects are described without client names, banks or amounts. Every demo uses fictional data.",
    stats: [
      ["30+", "dashboards in production", "finance, operations, sales and marketing"],
      ["441/441", "figures reproduced", "by an independent audit query"],
      ["5–16%", "call center occupancy", "the decision shifted from hiring more to covering peak hours"],
      ["> 100%", "flawed funding rate caught", "fixed before it reached investors"],
    ],
    summaryKicker: "Profile · summary",
    summaryTitle: "From raw data to decisions, with numbers you can audit",
    summarySub: "What I bring to a BI and credit-risk team.",
    experienceTitle: "Experience",
    educationTitle: "Education", inProgress: "In progress",
    projectsTitle: "Projects",
    projectsIntro: "Situation, result and what I did. Current employer: a consumer lender in Puerto Rico.",
    problem: "Situation", actions: "What I did", results: "Result", stack: "Tools",
    demosTitle: "Demos on fictional data",
    demosIntro: "A made-up digital lending fintech, January to June 2026, generated with Python. The data has quality problems on purpose. Each demo is computed with the SQL shown below it.",
    viewSql: "See the SQL", loading: "Loading…",
    codeTitle: "Code",
    codeIntro: "SQL and Python written for this portfolio on the fictional data. Nothing comes from an employer's systems.",
    openGithub: "Open on GitHub",
    powerbiTitle: "Power BI",
    powerbiIntro: "In progress: a credit-risk dashboard in Power BI on the same fictional data. These are the model's DAX measures.",
    cvSoon: "Resume · soon", cvDownload: "Download resume",
    footer: "Demos use fictional data generated with python/generar_datos.py. Projects are described without client, bank or counterparty names or amounts.",
    months: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"],
    total: "Total", notMature: "not mature", count: "Loans", balance: "Balance", from: "From", to: "To",
    paid: "Paid off",
  },
};

// Tarjetas del panel de perfil (estilo "resumen del portafolio")
const SKILLS = [
  { label: { es: "Riesgo", en: "Risk" }, title: { es: "Riesgo y cartera", en: "Risk & portfolio" },
    items: { es: "Roll rates · Mora por tramos (DPD) · NPL · PAR · Cosechas · UPB · First-payment default · Covenants · NIM, ROA, ROE",
             en: "Roll rates · DPD buckets · NPL · PAR · Vintages · UPB · First-payment default · Covenants · NIM, ROA, ROE" } },
  { label: { es: "SQL", en: "SQL" }, title: { es: "SQL y modelado", en: "SQL & modeling" },
    items: { es: "PostgreSQL · MySQL/MariaDB · SQL Server · SuiteQL · Funciones de ventana y CTEs · Modelado dimensional (Kimball) · ETL y calidad de datos",
             en: "PostgreSQL · MySQL/MariaDB · SQL Server · SuiteQL · Window functions and CTEs · Dimensional modeling (Kimball) · ETL and data quality" } },
  { label: { es: "BI", en: "BI" }, title: { es: "BI y visualización", en: "BI & visualization" },
    items: { es: "Power BI (DAX, Power Query) · Zoho Analytics · Tableau · Excel avanzado · Dashboards web",
             en: "Power BI (DAX, Power Query) · Zoho Analytics · Tableau · Advanced Excel · Web dashboards" } },
  { label: { es: "Python", en: "Python" }, title: { es: "Automatización", en: "Automation" },
    items: { es: "Python (pandas, APIs REST y OData) · GitHub Actions · Supabase · n8n · SFTP",
             en: "Python (pandas, REST and OData APIs) · GitHub Actions · Supabase · n8n · SFTP" } },
  { label: { es: "IA", en: "AI" }, title: { es: "IA con control", en: "AI with controls" },
    items: { es: "Claude Code para construir más rápido · cada resultado validado contra la fuente",
             en: "Claude Code to build faster · every result validated against the source" } },
  { label: { es: "Idiomas", en: "Languages" }, title: { es: "Idiomas", en: "Languages" },
    items: { es: "Español nativo · Inglés profesional · reuniones ejecutivas con equipos en EE. UU.",
             en: "Native Spanish · Professional English · executive meetings with US teams" } },
];

const EXPERIENCE = [
  {
    period: { es: "2024 – hoy", en: "2024 – present" },
    role: { es: "Analista de Datos Biz Ops · BI y Riesgo de Cartera", en: "Biz Ops Data Analyst · BI & Portfolio Risk" },
    org: { es: "Financiera de crédito al consumo (préstamos solares y de techos) · Puerto Rico y Florida · remoto desde Bogotá",
           en: "Consumer lender (solar and roofing loans) · Puerto Rico and Florida · remote from Bogotá" },
    bullets: {
      es: [
        "**Fuente única de verdad del portafolio:** consolidé originación y servicing con deduplicación por funciones de ventana; hoy es la base oficial de solicitudes, aprobaciones y desembolsos.",
        "**Motor de mora y riesgo:** histórico de tramos de mora y matriz de desempeño mensual, trimestral y acumulada; eliminé la inflación por snapshots que distorsionaba las tasas.",
        "**Reportería para financiadores:** cumplimiento de covenants y proyección de UPB y flujo de caja a 28 años por producto.",
        "**KPIs de riesgo temprano:** cumplimiento del primer pago (proxy de first-payment default) y flujo de caja proyectado vs. real.",
        "**Automatización de cobranza:** despacho diario de cartas con Python, Supabase y GitHub Actions, sin doble envío bajo Regulation F.",
        "**Dashboard financiero ejecutivo** en Power BI con modelo estrella: NIM, ROA, ROE, gasto operativo sobre ingresos y uso de la línea de fondeo.",
        "**Operación y contactabilidad:** tracker de gestión por gestor y ETL del conmutador del call center de cobranza.",
        "**Segmentación para Black Friday:** ETL que unifica la base de clientes (+500,000 registros) y activa campañas outbound multicanal con n8n y CRM.",
      ],
      en: [
        "**Portfolio single source of truth:** consolidated origination and servicing with window-function deduplication; it is now the official base for applications, approvals and disbursements.",
        "**Delinquency and risk engine:** DPD bucket history and a monthly, quarterly and cumulative performance matrix; removed snapshot inflation that distorted the rates.",
        "**Funder reporting:** covenant compliance and 28-year UPB and cash-flow projections by product.",
        "**Early-risk KPIs:** first-payment compliance (a first-payment-default proxy) and projected vs. actual cash flow.",
        "**Collections automation:** daily letter dispatch with Python, Supabase and GitHub Actions, with no double sends under Regulation F.",
        "**Executive financial dashboard** in Power BI on a star schema: NIM, ROA, ROE, operating expense ratio and funding-line utilization.",
        "**Operations and contactability:** a per-agent activity tracker and an ETL for the collections call center's phone system.",
        "**Black Friday segmentation:** an ETL that unifies the customer base (500,000+ records) and drives multichannel outbound campaigns with n8n and CRM.",
      ],
    },
  },
  {
    period: { es: "2021", en: "2021" },
    role: { es: "Analista de Marketing Comercial", en: "Commercial Marketing Analyst" },
    org: { es: "Kimberly-Clark · contrato de prácticas", en: "Kimberly-Clark · internship" },
    bullets: {
      es: ["Facturación y análisis de desempeño por punto de venta y por campaña de posicionamiento de marca."],
      en: ["Billing and performance analysis by point of sale and by brand-positioning campaign."],
    },
  },
];

const EDUCATION = [
  { title: { es: "Ingeniero de Producción", en: "B.Sc. in Production Engineering" }, org: "Universidad Distrital, Bogotá", period: "2020 – 2023" },
  { title: { es: "Tecnólogo Industrial", en: "Industrial Technologist" }, org: "Universidad Distrital, Bogotá", period: "2015 – 2019" },
];
const IN_PROGRESS = { es: "Datos en AWS (S3, Glue, Athena, Lambda) · Python avanzado", en: "Data on AWS (S3, Glue, Athena, Lambda) · Advanced Python" };

const PROJECTS = [
  {
    id: "borrowing-base",
    title: { es: "Certificado de borrowing base para la línea de crédito", en: "Borrowing base certificate for the credit facility" },
    tag: { es: "Calidad de datos · Riesgo", en: "Data quality · Risk" },
    problem: {
      es: "El certificado mensual que define cuánto presta el banco a la empresa se armaba a mano en Excel, y varias cifras firmadas no se podían reproducir.",
      en: "The monthly certificate that sets how much the bank lends to the company was built by hand in Excel, and several signed figures could not be reproduced.",
    },
    actions: {
      es: [
        "Recalculé el certificado desde la fuente: la foto de la cartera al cierre y las fórmulas del contrato.",
        "Validé cada cifra contra el certificado firmado, préstamo por préstamo y día por día, y documenté la causa de cada diferencia.",
        "Encontré reglas que no podían fallar, como una prueba de APR que solo promediaba préstamos que ya cumplían, y las llevé a gerencia antes de cambiarlas.",
        "Construí un dashboard que llena la plantilla original del banco y publica solo cifras agregadas, cifradas (AES-256) y con login.",
      ],
      en: [
        "Recomputed the certificate from the source: the month-end portfolio snapshot and the contract formulas.",
        "Validated every figure against the signed certificate, loan by loan and day by day, and documented the cause of each difference.",
        "Found rules that could never fail, such as an APR test that only averaged loans already above the minimum, and took them to management before changing them.",
        "Built a dashboard that fills the bank's original template and publishes only aggregated, encrypted (AES-256), login-protected figures.",
      ],
    },
    results: {
      es: ["Filas que cuadran: de 269 a 301.", "El acumulado de septiembre quedó idéntico al certificado firmado.", "Consultas de auditoría independientes reproducen 441 de 441 cifras."],
      en: ["Matching rows: from 269 to 301.", "The September cumulative now equals the signed certificate.", "Independent audit queries reproduce 441 of 441 figures."],
    },
    stack: ["Python", "SQL", "Zoho Analytics", "Next.js", "Vercel", "GitHub Actions"],
  },
  {
    id: "investor-dashboard",
    title: { es: "Dashboard para inversionistas", en: "Investor dashboard" },
    tag: { es: "BI · Gobierno de datos", en: "BI · Data governance" },
    problem: {
      es: "Un data request de inversionistas con más de diez secciones (portafolio, mora, cosechas, calidad crediticia, perfil del prestatario) se respondía armando cada cifra a mano.",
      en: "An investor data request with more than ten sections (portfolio, delinquency, vintages, credit quality, borrower profile) was answered by building each figure by hand.",
    },
    actions: {
      es: [
        "Flujo automático en Python que publica solo agregados; una barrera detiene el proceso si una consulta trae columnas que identifiquen a una persona o un préstamo.",
        "Cada cifra se valida contra su fuente en cada corrida; si algo no cuadra, no se publica.",
        "Esa disciplina destapó problemas reales: una tasa de fondeo por encima de 100 % y préstamos sin APR que subestimaban la tasa promedio.",
        "Nueve pestañas con filtros comunes, títulos que dicen la conclusión y exportes marcados como confidenciales.",
      ],
      en: [
        "Automated Python pipeline that publishes only aggregates; a guard stops the run if a query returns columns that identify a person or a loan.",
        "Every figure is validated against its source on every run; if something does not match, it is not published.",
        "That discipline surfaced real issues: a funding rate above 100% and loans without APR that understated the average rate.",
        "Nine tabs with shared filters, titles that state the conclusion, and exports marked as confidential.",
      ],
    },
    results: {
      es: ["10 de 10 secciones del data request en un solo dashboard.", "Cobertura de FICO de 72 % a 99 %.", "Más de 15 conjuntos de datos validados contra la fuente cada día."],
      en: ["10 of 10 data-request sections in a single dashboard.", "FICO coverage from 72% to 99%.", "15+ datasets validated against the source every day."],
    },
    stack: ["Python", "SQL", "Zoho Analytics API", "Next.js", "Vercel"],
  },
  {
    id: "dim-loan",
    title: { es: "Modelo de datos: una fila por préstamo", en: "Data model: one row per loan" },
    tag: { es: "Modelado · SQL", en: "Modeling · SQL" },
    problem: {
      es: "La originación y la administración de préstamos vivían en dos sistemas sin conexión; cada reporte cruzaba exports a mano, con riesgo de duplicados y definiciones distintas.",
      en: "Loan origination and servicing lived in two disconnected systems; every report joined exports by hand, risking duplicates and inconsistent definitions.",
    },
    actions: {
      es: [
        "Definí la llave de integración entre los dos sistemas.",
        "Evité que el join multiplicara filas: una sola solicitud por préstamo con ROW_NUMBER y pagos agregados antes de unir.",
        "Resolví la relación muchos a muchos entre préstamo y portafolio con banderas y una regla de prioridad.",
        "Estandaricé 11 segmentaciones (FICO, ingreso, DTI, antigüedad laboral, plazo, producto, tier), cada una con su columna de orden.",
      ],
      en: [
        "Defined the integration key between both systems.",
        "Kept the join from multiplying rows: one application per loan with ROW_NUMBER, and payments aggregated before joining.",
        "Solved the many-to-many loan–portfolio relationship with flags and a priority rule.",
        "Standardized 11 segmentations (FICO, income, DTI, job tenure, term, product, tier), each with its sort column.",
      ],
    },
    results: {
      es: ["Una sola fuente por préstamo, sin duplicados.", "Base de los reportes para inversionistas, banco y agencias calificadoras."],
      en: ["A single source per loan, with no duplicates.", "The foundation of investor, bank and rating-agency reports."],
    },
    stack: ["SQL (CTE, window functions)", "Zoho Analytics"],
  },
  {
    id: "collateral",
    title: { es: "Estratificaciones para agencias calificadoras", en: "Collateral strats for rating agencies" },
    tag: { es: "Riesgo · Auditoría", en: "Risk · Audit" },
    problem: {
      es: "Para presentar la cartera a agencias calificadoras había que llenar un resumen con unas 17 tablas de estratificación, exacto y auditable al corte.",
      en: "Presenting the portfolio to rating agencies required a summary with about 17 stratification tables, exact and auditable as of the cut-off date.",
    },
    actions: {
      es: [
        "Generador en Python que corre 12 consultas SQL agregadas por API y arma el Excel; otro cierre solo requiere cambiar la fecha.",
        "Excel auditable: totales crudos, cálculos con fórmulas, el SQL exacto y una hoja de supuestos; cada tabla verifica que suma el saldo total.",
        "Documenté problemas de calidad (scores vacíos, valores por defecto, una tasa negativa) sin corregirlos por mi cuenta, con las dos versiones para que decidiera el negocio.",
        "Llené la presentación editando solo los valores en el XML, sin alterar el diseño.",
      ],
      en: [
        "Python generator that runs 12 aggregated SQL queries through the API and builds the workbook; another month-end only needs a new date.",
        "Auditable workbook: raw totals, formula-driven calculations, the exact SQL and an assumptions sheet; every table checks that it adds up to the total balance.",
        "Documented data-quality issues (missing scores, default values, a negative rate) without fixing them on my own, with both versions so the business could decide.",
        "Filled the presentation by editing only the values in its XML, without touching the design.",
      ],
    },
    results: {
      es: ["Cifras 100 % conciliadas al corte.", "Excel recalculado con 0 errores de fórmula."],
      en: ["Figures 100% reconciled at the cut-off.", "Workbook recalculated with 0 formula errors."],
    },
    stack: ["Python", "SQL", "Zoho Analytics API", "Excel", "PowerPoint (OOXML)"],
  },
  {
    id: "roll-rate",
    title: { es: "Modelo de roll rate", en: "Roll rate model" },
    tag: { es: "Riesgo de crédito", en: "Credit risk" },
    problem: {
      es: "La gerencia no veía cómo migraban los préstamos entre tramos de mora de un mes a otro.",
      en: "Management could not see how loans migrated between delinquency buckets from one month to the next.",
    },
    actions: {
      es: [
        "Modelo estrella: hechos por préstamo y mes; dimensiones de tramo, tipo de movimiento y portafolio.",
        "Migración de SQL Server a PostgreSQL con vistas en capas: detalle, porcentajes, matriz y tendencia.",
        "Dashboard con matriz de 6 tramos, por conteo o por saldo, y tendencia mensual por tramo.",
      ],
      en: [
        "Star schema: facts per loan and month; bucket, movement-type and portfolio dimensions.",
        "Migration from SQL Server to PostgreSQL with layered views: detail, percentages, matrix and trend.",
        "Dashboard with a 6-bucket matrix, by count or by balance, plus a monthly trend per bucket.",
      ],
    },
    results: {
      es: ["Forward roll rate por tramo y mes, filtrable por 7 portafolios.", "Modelo replicable desde cero con su documentación."],
      en: ["Forward roll rate by bucket and month, filterable across 7 portfolios.", "Model reproducible from scratch with its documentation."],
    },
    stack: ["SQL Server", "PostgreSQL (Supabase)", "Next.js", "Vercel"],
  },
  {
    id: "call-center",
    title: { es: "Analítica de call center", en: "Call center analytics" },
    tag: { es: "Análisis operativo", en: "Operations analytics" },
    problem: {
      es: "La información de llamadas vivía en exportaciones CSV; la gerencia no sabía cuántas llamadas se perdían, a qué hora ni quién cubría cada franja.",
      en: "Call data lived in CSV exports; management did not know how many calls were missed, when, or who covered each time slot.",
    },
    actions: {
      es: [
        "Modelo estrella con una fila por llamada, deduplicada, y un ETL en Python que reconoce 9 tipos de archivo.",
        "Corregí sesgos antes de concluir: llamadas internas, abandonos reales, atribución por deduplicación y un tiempo de conexión inflado (uso de la mediana).",
        "Detección automática de días sin datos, para no confundir un fallo de carga con un día sin llamadas.",
      ],
      en: [
        "Star schema with one deduplicated row per call, and a Python ETL that recognizes 9 file types.",
        "Fixed biases before drawing conclusions: internal calls, true abandons, attribution after deduplication, and inflated login time (using the median).",
        "Automatic detection of days without data, so a failed load is not mistaken for a day without calls.",
      ],
    },
    results: {
      es: [
        "Nivel de servicio de ~94 % en 20 segundos o menos, pero ocupación de 5–16 %: el problema no era falta de personal sino presencia en horas pico.",
        "El 45 % de las llamadas perdidas ocurría antes de las 9 a. m., base de una recomendación de cobertura por franja.",
      ],
      en: [
        "~94% service level within 20 seconds but 5–16% occupancy: the issue was not understaffing but presence at peak hours.",
        "45% of missed calls happened before 9 a.m., the basis for a coverage recommendation by time slot.",
      ],
    },
    stack: ["Python", "PostgreSQL (Supabase)", "Power Automate", "Next.js"],
  },
  {
    id: "letters",
    title: { es: "Cartas de cobro automatizadas y control de direcciones", en: "Automated collection letters and address control" },
    tag: { es: "Automatización · Calidad", en: "Automation · Quality" },
    problem: {
      es: "Las cartas de cobro iban a un proveedor de impresión; nadie reaccionaba a las devueltas y la misma dirección que había fallado se volvía a usar.",
      en: "Collection letters went to a print vendor; nobody acted on returned mail, and the same failed address was used again.",
    },
    actions: {
      es: [
        "Conexión SFTP y despacho diario automático, con reintentos y evidencia archivada.",
        "Analizador de direcciones según la norma postal de EE. UU. (USPS Pub. 28), cubierto por 15 pruebas; una falla a propósito si alguien rompe una regla clave.",
        "Regla de similitud: si la dirección nueva se parece demasiado a la que rebotó, la cuenta sigue retenida; liberarla siempre es una decisión humana.",
        "Encontré la causa raíz de días sin envío (el despacho corría antes de que terminara la sincronización) y la corregí.",
      ],
      en: [
        "SFTP connection and automated daily dispatch, with retries and archived evidence.",
        "Address parser following the USPS Pub. 28 standard, covered by 15 tests; one fails on purpose if someone breaks a key rule.",
        "Similarity rule: if the new address is too close to the one that bounced, the account stays on hold; releasing it is always a human decision.",
        "Found the root cause of days with no dispatch (it ran before the data sync finished) and fixed it.",
      ],
    },
    results: {
      es: [
        "0 cartas enviadas a cuentas retenidas desde que el filtro está activo.",
        "96.5 % de cobertura de evidencia documental, carta por carta.",
        "No presenté la caída de devoluciones como mejora: tardan 5–6 semanas en reportarse y la cifra aún no había madurado.",
      ],
      en: [
        "0 letters sent to held accounts since the filter went live.",
        "96.5% documentary evidence coverage, letter by letter.",
        "I did not report the drop in returned mail as an improvement: returns take 5–6 weeks to arrive and the figure had not matured yet.",
      ],
    },
    stack: ["Python", "SFTP (paramiko)", "PostgreSQL (Supabase)", "SQL"],
  },
  {
    id: "powerbi-finance",
    title: { es: "Dashboard financiero en Power BI", en: "Financial dashboard in Power BI" },
    tag: { es: "Power BI · Finanzas", en: "Power BI · Finance" },
    problem: {
      es: "La rentabilidad de una subsidiaria se revisaba en reportes contables exportados que se armaban a mano en cada cierre.",
      en: "A subsidiary's profitability was reviewed in exported accounting reports assembled by hand at every month-end.",
    },
    actions: {
      es: [
        "Modelo estrella (Kimball): saldos contables como hechos; cuenta, fecha y subsidiaria como dimensiones.",
        "Carga por carpeta en Power Query: el periodo sale del nombre de cada archivo.",
        "Medidas DAX validadas con contabilidad: margen de interés neto, ROA, ROE, gasto operativo sobre ingresos y uso de la línea de crédito.",
        "Corregí un error de fondo: los saldos acumulados del año se sumaban entre meses e inflaban los totales.",
      ],
      en: [
        "Star schema (Kimball): account balances as facts; account, date and subsidiary as dimensions.",
        "Folder load in Power Query: the period comes from each file name.",
        "DAX measures validated with accounting: net interest margin, ROA, ROE, operating expense ratio and credit-line utilization.",
        "Fixed a root issue: year-to-date balances were being summed across months, inflating totals.",
      ],
    },
    results: {
      es: ["Agregar un mes nuevo es soltar el archivo y actualizar.", "Cada KPI cuadra con las cifras oficiales del cierre."],
      en: ["Adding a new month means dropping the file and refreshing.", "Every KPI matches the official month-end figures."],
    },
    stack: ["Power BI", "DAX", "Power Query", "NetSuite"],
  },
];

const DEMOS = {
  approval: {
    title: { es: "Aprobación por canal: ¿riesgo más estricto o cambió la mezcla?", en: "Approval by channel: stricter risk, or a different mix?" },
    read: {
      es: "En abril entra WhatsApp y la aprobación total baja de 59.5 % a 50.7 %, mientras Web y App siguen cerca de 60 %. La caída de ese mes es mezcla de solicitantes, no un cambio de política.",
      en: "WhatsApp launches in April and total approval drops from 59.5% to 50.7%, while Web and App stay near 60%. That month's drop is applicant mix, not a policy change.",
    },
    sql: "sql/01_aprobacion_por_canal.sql",
    note: {
      es: "Tasa de aprobación = solicitudes aprobadas ÷ solicitudes del mes. Total = todas las solicitudes del mes. Fuente: datos ficticios (data/fintech_demo.db).",
      en: "Approval rate = approved applications ÷ applications in the month. Total = all applications in the month. Source: fictional data (data/fintech_demo.db).",
    },
  },
  vintage: {
    title: { es: "Mora 30+ por cosecha: solo se comparan cosechas maduras", en: "30+ DPD by vintage: compare only mature vintages" },
    read: {
      es: "En la cosecha de abril, WhatsApp tiene 12.3 % de mora 30+ contra ~7.5 % de Web y App: 1.6 veces más. Mayo y junio aún no han madurado; una cifra baja ahí no es una mejora.",
      en: "In the April vintage, WhatsApp shows 12.3% 30+ DPD versus ~7.5% for Web and App: 1.6 times higher. May and June have not matured yet; a low figure there is not an improvement.",
    },
    sql: "sql/02_mora_por_cosecha.sql",
    note: {
      es: "Mora 30+ = saldo con más de 30 días de mora al 30-jun-2026 ÷ monto desembolsado de la cosecha. Cosecha = mes de desembolso. Las cosechas sin madurar se muestran atenuadas. Fuente: datos ficticios.",
      en: "30+ DPD = balance more than 30 days past due on 2026-06-30 ÷ amount disbursed in the vintage. Vintage = disbursement month. Immature vintages are dimmed. Source: fictional data.",
    },
  },
  roll: {
    title: { es: "Roll rate: de un cierre de mes al siguiente", en: "Roll rate: from one month-end to the next" },
    read: {
      es: "Cada fila suma 100 %. 'Pagado' son los préstamos que salieron de la cartera: con un INNER JOIN se perderían y el roll rate quedaría sesgado.",
      en: "Each row adds up to 100%. 'Paid off' are loans that left the portfolio: an INNER JOIN would drop them and bias the roll rate.",
    },
    sql: "sql/03_roll_rate.sql",
    note: {
      es: "Filas: tramo de mora al cierre de origen. Columnas: tramo al cierre siguiente. Cada fila suma 100 % por conteo o por saldo. Rojo = empeoró; verde = mejoró o pagó. Fuente: datos ficticios.",
      en: "Rows: delinquency bucket at the starting month-end. Columns: bucket at the next month-end. Each row adds up to 100% by count or balance. Red = worsened; green = improved or paid off. Source: fictional data.",
    },
  },
  quality: {
    title: { es: "Calidad de datos: antes de publicar una cifra", en: "Data quality: before publishing a figure" },
    read: {
      es: "Cada chequeo debería dar 0. El saldo recalculado no cuadra en 30 préstamos con los pagos crudos y cuadra en todos al quitar los pagos duplicados: la diferencia tiene causa.",
      en: "Every check should return 0. The recomputed balance misses on 30 loans with raw payments and matches all of them once duplicate payments are removed: the difference has a cause.",
    },
    sql: "sql/04_calidad_de_datos.sql",
    sql2: "sql/05_conciliacion_saldos.sql",
    note: {
      es: "Conciliación: saldo recalculado (monto − pagos) contra la foto oficial del cierre de junio. Fuente: datos ficticios con errores introducidos a propósito.",
      en: "Reconciliation: recomputed balance (amount − payments) against the official June month-end snapshot. Source: fictional data with deliberately introduced errors.",
    },
    labels: {
      es: { pagos_duplicados: "Pagos duplicados", pagos_antes_del_desembolso: "Pagos antes del desembolso", prestamos_con_solicitud_rechazada: "Préstamos con solicitud rechazada", ciudades: "Ciudades: valores crudos → normalizados", conc: "Préstamos que no cuadran: pagos crudos → sin duplicados" },
      en: { pagos_duplicados: "Duplicate payments", pagos_antes_del_desembolso: "Payments before disbursement", prestamos_con_solicitud_rechazada: "Loans with a rejected application", ciudades: "Cities: raw values → normalized", conc: "Loans that do not reconcile: raw payments → deduplicated" },
    },
  },
};

const CODE = [
  { path: "sql/01_aprobacion_por_canal.sql", es: "Aprobación por mes y canal.", en: "Approval by month and channel." },
  { path: "sql/02_mora_por_cosecha.sql", es: "Mora 30+ por cosecha y canal, con el canal traído desde la solicitud.", en: "30+ DPD by vintage and channel, with channel joined from the application." },
  { path: "sql/03_roll_rate.sql", es: "Matriz de roll rate entre cierres con LEFT JOIN y salida 'Pagado'.", en: "Roll-rate matrix between month-ends with a LEFT JOIN and a 'Paid off' exit." },
  { path: "sql/04_calidad_de_datos.sql", es: "Chequeos que deben dar 0 antes de publicar.", en: "Checks that must return 0 before publishing." },
  { path: "sql/05_conciliacion_saldos.sql", es: "Saldo recalculado contra la foto oficial del cierre.", en: "Recomputed balance against the official month-end snapshot." },
  { path: "python/generar_datos.py", es: "Genera la fintech ficticia, con problemas de calidad a propósito.", en: "Generates the fictional fintech, with deliberate quality issues." },
  { path: "python/exportar_demos.py", es: "Corre el SQL y publica los datos del sitio, con un control que bloquea si no cuadra.", en: "Runs the SQL and publishes the site data, with a control that blocks on mismatch." },
  { path: "python/reporte_kpis.py", es: "pandas: limpieza, conciliación, Excel de KPIs y un control que se puede romper a propósito.", en: "pandas: cleaning, reconciliation, a KPI workbook and a control you can break on purpose." },
  { path: "powerbi/medidas.dax", es: "Medidas DAX del dashboard de riesgo.", en: "DAX measures for the risk dashboard." },
];

// Palabras que aparecen en la portada (herramientas del CV y conceptos de ingeniería y riesgo)
const KEYWORDS = (() => {
  const comunes = ["SQL", "Python", "Power BI", "DAX", "Power Query", "Excel", "PostgreSQL", "SQL Server", "MySQL",
    "SuiteQL", "CTEs", "Window functions", "Kimball", "ETL", "pandas", "REST APIs", "OData", "GitHub Actions",
    "Supabase", "n8n", "Zoho Analytics", "Tableau", "AWS S3", "AWS Glue", "Athena", "Lambda", "Claude Code", "Git",
    "Roll rates", "DPD", "NPL", "PAR", "UPB", "Covenants", "NIM", "ROA", "ROE", "First-payment default"];
  return {
    es: [...comunes, "Cosechas", "Calidad de datos", "Riesgo de crédito", "Modelado dimensional", "Ingeniería de producción", "Automatización", "Conciliación"],
    en: [...comunes, "Vintages", "Data quality", "Credit risk", "Dimensional modeling", "Production engineering", "Automation", "Reconciliation"],
  };
})();

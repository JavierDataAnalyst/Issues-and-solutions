-- Mora 30+ por cosecha (mes de desembolso) y canal, al corte del 30-jun-2026
-- 30+ days-past-due by vintage (disbursement month) and channel, as of 2026-06-30
--
-- Mora 30+ = saldo con más de 30 días de mora al corte / monto desembolsado de la cosecha.
-- Ojo: una cosecha reciente todavía no ha madurado; con créditos de hasta 30 días,
-- solo es comparable cuando han pasado al menos 60 días desde el fin del mes.
WITH prestamos_canal AS (
    SELECT p.prestamo_id,
           p.monto,
           s.canal,
           strftime('%Y-%m', p.fecha_desembolso) AS cosecha
    FROM prestamos p
    JOIN solicitudes s USING (solicitud_id)          -- el canal vive en la solicitud
),
mora_30 AS (
    SELECT prestamo_id, saldo
    FROM cartera_cierre
    WHERE fecha_corte = '2026-06-30'
      AND dias_mora > 30
)
SELECT pc.cosecha,
       pc.canal,
       COUNT(*)                                                    AS prestamos,
       SUM(pc.monto)                                               AS desembolsado,
       COALESCE(SUM(m.saldo), 0)                                   AS saldo_mora_30,
       ROUND(1.0 * COALESCE(SUM(m.saldo), 0) / SUM(pc.monto), 4)   AS mora_30
FROM prestamos_canal pc
LEFT JOIN mora_30 m USING (prestamo_id)
GROUP BY pc.cosecha, pc.canal
ORDER BY pc.cosecha, pc.canal;

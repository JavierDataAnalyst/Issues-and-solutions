-- Conciliación: saldo recalculado (monto - pagos) vs. foto oficial del cierre de junio
-- Reconciliation: recomputed balance (amount - payments) vs. the official June month-end snapshot
--
-- Con los pagos crudos no cuadra; sin los pagos duplicados, cuadra. La diferencia tiene causa.
WITH pagado_crudo AS (
    SELECT prestamo_id, SUM(valor) AS pagado
    FROM pagos
    GROUP BY prestamo_id
),
pagado_limpio AS (
    SELECT prestamo_id, SUM(valor) AS pagado
    FROM (SELECT DISTINCT * FROM pagos)
    GROUP BY prestamo_id
),
foto AS (
    SELECT prestamo_id, saldo
    FROM cartera_cierre
    WHERE fecha_corte = '2026-06-30'
)
SELECT COUNT(*)                                                           AS prestamos,
       SUM(p.monto - COALESCE(c.pagado, 0) <> COALESCE(f.saldo, 0))       AS no_cuadran_con_pagos_crudos,
       SUM(p.monto - COALESCE(l.pagado, 0) <> COALESCE(f.saldo, 0))       AS no_cuadran_sin_duplicados
FROM prestamos p
LEFT JOIN pagado_crudo  c USING (prestamo_id)
LEFT JOIN pagado_limpio l USING (prestamo_id)
LEFT JOIN foto          f USING (prestamo_id);

-- Roll rate: tramo de mora de cada préstamo al cierre de un mes vs. el cierre del mes siguiente
-- Roll rate: each loan's delinquency bucket at one month-end vs. the next month-end
--
-- 1. Una foto por préstamo y mes (cartera_cierre ya trae una fila por préstamo y corte;
--    con registros diarios usaría ROW_NUMBER() OVER (PARTITION BY prestamo, mes ORDER BY fecha DESC) = 1).
-- 2. LEFT JOIN contra el cierre del mes siguiente. LEFT y no INNER: los préstamos que se
--    pagan desaparecen de la foto siguiente; si se pierden, el roll rate queda sesgado.
-- 3. Los que no aparecen el mes siguiente quedan como 'Pagado', para que cada fila sume 100 %.
SELECT d.fecha_corte                              AS corte_desde,
       h_corte.corte_hasta,
       d.tramo_mora                               AS tramo_desde,
       COALESCE(h.tramo_mora, 'Pagado')           AS tramo_hasta,
       COUNT(*)                                   AS prestamos,
       SUM(d.saldo)                               AS saldo_desde
FROM cartera_cierre d
JOIN (SELECT DISTINCT fecha_corte AS corte_hasta FROM cartera_cierre) h_corte
  ON h_corte.corte_hasta = date(d.fecha_corte, 'start of month', '+2 months', '-1 day')
LEFT JOIN cartera_cierre h
  ON  h.prestamo_id = d.prestamo_id
  AND h.fecha_corte = h_corte.corte_hasta
GROUP BY d.fecha_corte, h_corte.corte_hasta, d.tramo_mora, COALESCE(h.tramo_mora, 'Pagado')
ORDER BY corte_desde, tramo_desde, tramo_hasta;

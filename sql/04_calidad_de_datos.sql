-- Chequeos de calidad de datos antes de publicar cualquier cifra
-- Data-quality checks before publishing any figure
--
-- Cada chequeo debería dar 0. Si no da 0, la cifra no se publica hasta explicar la diferencia.
SELECT
    (SELECT COUNT(*) - COUNT(DISTINCT pago_id) FROM pagos)            AS pagos_duplicados,
    (SELECT COUNT(*)
       FROM pagos g
       JOIN prestamos p USING (prestamo_id)
      WHERE g.fecha_pago < p.fecha_desembolso)                        AS pagos_antes_del_desembolso,
    (SELECT COUNT(*)
       FROM prestamos p
       JOIN solicitudes s USING (solicitud_id)
      WHERE s.decision = 'Rechazada')                                 AS prestamos_con_solicitud_rechazada,
    (SELECT COUNT(DISTINCT ciudad) FROM clientes)                     AS valores_distintos_de_ciudad;

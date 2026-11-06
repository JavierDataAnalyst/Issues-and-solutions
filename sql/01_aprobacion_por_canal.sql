-- Tasa de aprobación por mes y canal
-- Approval rate by month and channel
--
-- Pregunta de negocio: si la aprobación total cae, ¿riesgo se volvió más estricto
-- o cambió la mezcla de solicitantes? Abrir por canal responde eso.
SELECT strftime('%Y-%m', fecha_solicitud)                    AS mes,
       canal,
       COUNT(*)                                              AS solicitudes,
       SUM(decision = 'Aprobada')                            AS aprobadas,
       ROUND(1.0 * SUM(decision = 'Aprobada') / COUNT(*), 4) AS tasa_aprobacion
FROM solicitudes
GROUP BY mes, canal
ORDER BY mes, canal;

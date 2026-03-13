/* ============================================================================================================================
   PROYECTO    : Análisis de Riesgo Crediticio
   ARCHIVO     : 02_limpieza.sql
   DESCRIPCIÓN : Capa MART - Vistas limpias construidas sobre las tablas RAW
                 Aplica todas las reglas identificadas en 01_exploratorio.sql
                 Las vistas NO almacenan datos, consultan en tiempo real desde RAW
   AUTOR       : Diego L. Villavicencio
   FECHA       : 2026-03-05
   VERSIÓN     : 1.1 - Eliminadas año_desembolso y mes_desembolso (cubiertas por DIM_calendario en DAX)
                       Eliminadas columnas año y mes de MART_T2_kpis (misma razón)
                       Corregido tramo_mora para calcularse sobre el valor ya corregido de dias_atraso

   VISTAS CREADAS:
       MART_T1_creditos       - Créditos limpios + columnas calculadas (~8000 filas)
       MART_T2_kpis           - KPIs mensuales tipificados (24 filas)
       MART_T3_clientes       - Clientes limpios (390 filas)
       MART_T4_cosechas       - Cosechas tipificadas (222 filas)
       MART_T5_calidad_datos  - Log de calidad para Página 4 del dashboard

   REGLAS APLICADAS:
       - Excluir: proceso judicial, crédito empleado, monto negativo,
                  tasa inválida, fecha inconsistente, duplicados
       - Corregir: días de atraso negativos - 0
       - Normalizar: sector económico fuera de catálogo
       - Segmentar: score inválido/nulo - Sin Score
       - Imputar: garantía nula - Sin Información
       - Tipificar: todas las columnas de VARCHAR(MAX) a tipos correctos
       - Calcular: cosecha_mes, tramo_mora, tramo_score, tramo_endeudamiento,
                   flag_deficit_provision, tasa_supervivencia_pct, castigos_netos
============================================================================================================================ */

USE RiesgoCrediticioProyecto;
GO

/* ============================================================================================================================
   MART 1 - CRÉDITOS
   Fuente  : T1_creditos_riesgo_crediticio_RAW
   Uso     : Páginas 1, 2 y 3 del dashboard
   Nota    : ROW_NUMBER() elimina duplicados priorizando registros sin flag_error
============================================================================================================================ */

DROP VIEW IF EXISTS MART_T1_creditos;
GO

CREATE VIEW MART_T1_creditos AS

-- PASO 1: Deduplicación estructural
WITH creditos_unicos AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id_snapshot
            ORDER BY
                CASE WHEN ISNULL(flag_error,'') = 'ERROR_REGISTRO_DUPLICADO' THEN 1 ELSE 0 END ASC,
                id_snapshot ASC
        ) AS nro_fila
    FROM T1_creditos_riesgo_crediticio_RAW
),
-- PASO 2: Corrección de días de atraso - se hace en CTE para que tramo_mora
--         lo consuma ya corregido y no haya inconsistencia entre ambas columnas
creditos_corregidos AS (
    SELECT *,
        CASE
            WHEN CAST(dias_atraso AS INT) < 0 THEN 0
            ELSE CAST(dias_atraso AS INT)
        END AS dias_atraso_corregido
    FROM creditos_unicos
    WHERE
        nro_fila            = 1
        AND ISNULL(flag_error,'') != 'EXCLUIR_PROCESO_JUDICIAL'
        AND ISNULL(flag_error,'') != 'EXCLUIR_CREDITO_EMPLEADO'
        AND CAST(saldo_capital AS DECIMAL(18,2)) >= 0
        AND CAST(tasa_nominal_anual_pct AS DECIMAL(8,4)) BETWEEN 0.1 AND 100
        AND CAST(fecha_vencimiento AS DATE) > CAST(fecha_desembolso AS DATE)
)
-- PASO 3: Cuerpo principal - todas las columnas limpias y tipificadas
SELECT
    -- IDENTIFICACIÓN
    id_snapshot,
    id_cliente,
    tipo_persona,
    segmento,
    -- Normalización sector económico - cubre los 8 valores del catálogo oficial (diccionario var. 5)
    CASE
        WHEN sector_economico IN ('comercio','COMERCIO','Comerc.')              THEN 'Comercio'
        WHEN sector_economico IN ('Ind.','industria','INDUSTRIA')               THEN 'Industria'
        WHEN sector_economico IN ('serv','S/I','servicios')                     THEN 'Servicios'
        WHEN sector_economico IN ('construccion','CONSTRUCCION','Construccion') THEN 'Construcción'
        WHEN sector_economico IN ('Agro','agro','AGRO')                         THEN 'Agroindustria'
        WHEN sector_economico IN ('transp.','Transp.','TRANSPORTE')             THEN 'Transporte'
        WHEN sector_economico IN ('tecnologia','TECNOLOGIA','Tecnologia','Tech') THEN 'Tecnología'
        WHEN sector_economico IN ('salud','SALUD','Health')                     THEN 'Salud'
        WHEN sector_economico IN ('N/A','sin sector','OTROS',
                                  'otro','?') OR
             sector_economico IS NULL OR sector_economico = ''                  THEN 'Sin Clasificar'
        ELSE sector_economico
    END                                                             AS sector_economico,
    zona_geografica,
    oficial_credito,
-- PRODUCTO---------------------------------------------------------------------
    producto_crediticio,
    moneda,
-- FECHAS tipificadas-----------------------------------------------------------
    CAST(fecha_desembolso  AS DATE)                                 AS fecha_desembolso,
    CAST(fecha_vencimiento AS DATE)                                 AS fecha_vencimiento,
    CAST(fecha_corte       AS DATE)                                 AS fecha_corte,
    CAST(plazo_meses       AS INT)                                  AS plazo_meses,
    -- Llave de relación con DIM_calendario y MART_T4_cosechas en Power BI
    FORMAT(CAST(fecha_desembolso AS DATE), 'yyyy-MM')               AS cosecha_mes,
-- MONTOS tipificados-----------------------------------------------------------
    CAST(monto_aprobado           AS DECIMAL(18,2))                 AS monto_aprobado,
    CAST(monto_desembolsado       AS DECIMAL(18,2))                 AS monto_desembolsado,
    CAST(saldo_capital            AS DECIMAL(18,2))                 AS saldo_capital,
    CAST(saldo_interes            AS DECIMAL(18,2))                 AS saldo_interes,
    CAST(saldo_mora               AS DECIMAL(18,2))                 AS saldo_mora,
    CAST(saldo_total_exposicion   AS DECIMAL(18,2))                 AS saldo_total_exposicion,
-- TASAS tipificadas------------------------------------------------------------
    CAST(tasa_nominal_anual_pct   AS DECIMAL(8,4))                  AS tasa_nominal_anual_pct,
    CAST(tasa_efectiva_anual_pct  AS DECIMAL(8,4))                  AS tasa_efectiva_anual_pct,
    CAST(spread_pct               AS DECIMAL(8,4))                  AS spread_pct,
-- MORA Y CALIFICACIÓN----------------------------------------------------------
    -- Corrección ya aplicada en CTE anterior: días negativos - 0
    dias_atraso_corregido                                           AS dias_atraso,
    CAST(numero_cuotas_vencidas AS INT)                             AS numero_cuotas_vencidas,
    calificacion_sbs,
    -- Tramo de mora calculado sobre el valor ya corregido (consistente con dias_atraso)
    CASE
        WHEN dias_atraso_corregido = 0   THEN 'Normal (0 días)'
        WHEN dias_atraso_corregido <= 8  THEN 'CPP (1-8 días)'
        WHEN dias_atraso_corregido <= 30 THEN 'Deficiente (9-30 días)'
        WHEN dias_atraso_corregido <= 60 THEN 'Dudoso (31-60 días)'
        ELSE                                  'Pérdida (60+ días)'
    END                                                             AS tramo_mora,
-- PROVISIONES tipificadas------------------------------------------------------
    CAST(tasa_provision_pct           AS DECIMAL(8,4))              AS tasa_provision_pct,
    CAST(provision_requerida          AS DECIMAL(18,2))             AS provision_requerida,
    CAST(provision_constituida        AS DECIMAL(18,2))             AS provision_constituida,
    CAST(deficit_superavit_provision  AS DECIMAL(18,2))             AS deficit_superavit_provision,
    -- Flag de alerta: 1 = crédito sub-provisionado (alerta regulatoria)
    CASE
        WHEN CAST(deficit_superavit_provision AS DECIMAL(18,2)) < 0 THEN 1
        ELSE 0
    END                                                             AS flag_deficit_provision,
-- GARANTÍAS tipificadas--------------------------------------------------------
    -- Imputación: nulo = Sin Información
    CASE
        WHEN tipo_garantia IS NULL OR tipo_garantia = '' THEN 'Sin Información'
        ELSE tipo_garantia
    END                                                             AS tipo_garantia,
    CAST(valor_garantia           AS DECIMAL(18,2))                 AS valor_garantia,
    CAST(cobertura_garantia_ratio AS DECIMAL(8,4))                  AS cobertura_garantia_ratio,
    CAST(ltv_loan_to_value        AS DECIMAL(8,4))                  AS ltv_loan_to_value,
-- MODELOS INTERNOS tipificados-------------------------------------------------
    -- Score: valores fuera de [200-850] se tratan como nulos
    CASE
        WHEN TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NULL              THEN NULL
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850  THEN NULL
        ELSE CAST(score_crediticio AS DECIMAL(8,2))
    END                                                             AS score_crediticio,
    -- Flag explícito para identificar créditos sin score válido
    CASE
        WHEN TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NULL              THEN 'Sin Score'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850  THEN 'Sin Score'
        ELSE 'Con Score'
    END                                                             AS flag_score,
    -- Tramo de score para distribución en gráficos
    CASE
        WHEN TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NULL                    THEN 'Sin Score'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850        THEN 'Sin Score'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) BETWEEN 200 AND 399            THEN 'Muy Alto Riesgo (200-399)'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) BETWEEN 400 AND 549            THEN 'Alto Riesgo (400-549)'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) BETWEEN 550 AND 649            THEN 'Riesgo Medio (550-649)'
        WHEN CAST(score_crediticio AS DECIMAL(8,2)) BETWEEN 650 AND 749            THEN 'Riesgo Bajo (650-749)'
        ELSE                                                               'Muy Bajo Riesgo (750-850)'
    END                                                             AS tramo_score,
    CAST(pd_probabilidad_default  AS DECIMAL(8,4))                  AS pd_probabilidad_default,
    CAST(lgd_loss_given_default   AS DECIMAL(8,4))                  AS lgd_loss_given_default,
    CAST(ead_exposure_at_default  AS DECIMAL(18,2))                 AS ead_exposure_at_default,
    CAST(el_expected_loss         AS DECIMAL(18,2))                 AS el_expected_loss,
-- RATIOS FINANCIEROS tipificados-----------------------------------------------
    CAST(dscr_cobertura_deuda       AS DECIMAL(8,4))                AS dscr_cobertura_deuda,
    CAST(ratio_endeudamiento        AS DECIMAL(8,4))                AS ratio_endeudamiento,
    CAST(ratio_liquidez_corriente   AS DECIMAL(8,4))                AS ratio_liquidez_corriente,
    CAST(ratio_cobertura_garantia   AS DECIMAL(8,4))                AS ratio_cobertura_garantia,
    CAST(ingresos_anuales           AS DECIMAL(18,2))               AS ingresos_anuales,
    CAST(cuota_mensual              AS DECIMAL(18,2))               AS cuota_mensual,
-- COMPORTAMIENTO tipificado----------------------------------------------------
    CAST(numero_refinanciaciones    AS INT)                         AS numero_refinanciaciones,
    CAST(es_refinanciado            AS INT)                         AS es_refinanciado,
    CAST(veces_pago_anticipado      AS INT)                         AS veces_pago_anticipado,
    CAST(numero_creditos_en_entidad AS INT)                         AS numero_creditos_en_entidad,
    CAST(numero_creditos_en_sistema AS INT)                         AS numero_creditos_en_sistema,
    CAST(antiguedad_cliente_meses   AS INT)                         AS antiguedad_cliente_meses,
    CAST(edad_empresa_anos          AS DECIMAL(6,1))                AS edad_empresa_anos,
-- CASTIGOS tipificados---------------------------------------------------------
    CAST(en_castigo      AS INT)                                    AS en_castigo,
    CAST(monto_castigado AS DECIMAL(18,2))                          AS monto_castigado,
-- TRAZABILIDAD - preservado para auditoría-------------------------------------
    ISNULL(flag_error, '')                                          AS flag_error

FROM creditos_corregidos;
GO

/* ============================================================================================================================
   MART 2 - KPIs MENSUALES
   Fuente  : T2_kpis_mensuales_riesgo_RAW
   Uso     : Página 1 - Resumen Ejecutivo (Director)
   Nota    : No tiene errores de calidad, solo tipificación y columna calculada
             año y mes eliminados - DIM_calendario los cubre en Power BI
============================================================================================================================ */

DROP VIEW IF EXISTS MART_T2_kpis;
GO

CREATE VIEW MART_T2_kpis AS
SELECT
    -- Período - fecha_mes es la llave de relación con DIM_calendario en Power BI
    CAST(fecha_mes AS DATE)                                         AS fecha_mes,
-- Cartera----------------------------------------------------------------------
    CAST(cartera_bruta_total           AS DECIMAL(18,2))            AS cartera_bruta_total,
    CAST(cartera_vigente               AS DECIMAL(18,2))            AS cartera_vigente,
    CAST(cartera_mora                  AS DECIMAL(18,2))            AS cartera_mora,
    CAST(tasa_mora_pct                 AS DECIMAL(8,4))             AS tasa_mora_pct,
-- Provisiones------------------------------------------------------------------
    CAST(provision_total_constituida   AS DECIMAL(18,2))            AS provision_total_constituida,
    CAST(ratio_cobertura_provision     AS DECIMAL(8,4))             AS ratio_cobertura_provision,
-- Originación y castigos-------------------------------------------------------
    CAST(nuevos_creditos_desembolsados AS INT)                      AS nuevos_creditos_desembolsados,
    CAST(creditos_castigados_mes       AS DECIMAL(18,2))            AS creditos_castigados_mes,
    CAST(recuperaciones_castigos       AS DECIMAL(18,2))            AS recuperaciones_castigos,
-- Rentabilidad-----------------------------------------------------------------
    CAST(roe_retorno_patrimonio        AS DECIMAL(8,4))             AS roe_retorno_patrimonio,
    CAST(roa_retorno_activos           AS DECIMAL(8,4))             AS roa_retorno_activos,
    CAST(nim_margen_interes_neto       AS DECIMAL(8,4))             AS nim_margen_interes_neto,
    CAST(costo_riesgo                  AS DECIMAL(18,2))            AS costo_riesgo,
-- Eficiencia y solvencia-------------------------------------------------------
    CAST(ratio_eficiencia              AS DECIMAL(8,4))             AS ratio_eficiencia,
    CAST(ratio_capital_tier1           AS DECIMAL(8,4))             AS ratio_capital_tier1,
-- Columna calculada: pérdida neta real del período-----------------------------
    CAST(creditos_castigados_mes AS DECIMAL(18,2))
    - CAST(recuperaciones_castigos AS DECIMAL(18,2))                AS castigos_netos

FROM T2_kpis_mensuales_riesgo_RAW;
GO

/* ============================================================================================================================
   MART 3 - CLIENTES
   Fuente  : T3_clientes_riesgo_crediticio_RAW
   Uso     : Páginas 2 y 3 del dashboard
   Nota    : No tiene errores de calidad, solo tipificación y columnas calculadas
============================================================================================================================ */

DROP VIEW IF EXISTS MART_T3_clientes;
GO

CREATE VIEW MART_T3_clientes AS
SELECT
    -- Identificación
    id_cliente,
    tipo_persona,
    segmento,
    sector_economico,
    zona_geografica,
-- Relación comercial-----------------------------------------------------------
    CAST(fecha_alta               AS DATE)                          AS fecha_alta,
    CAST(numero_productos_activos AS INT)                           AS numero_productos_activos,
-- Riesgo consolidado-----------------------------------------------------------
    CAST(score_crediticio_actual AS DECIMAL(8,2))                       AS score_crediticio_actual,
    CAST(saldo_total_deuda            AS DECIMAL(18,2))             AS saldo_total_deuda,
    CAST(maximo_dias_atraso_historico AS INT)                       AS maximo_dias_atraso_historico,
    calificacion_consolidada,
    -- Tramo de score del cliente para segmentación
    CASE
        WHEN CAST(score_crediticio_actual AS DECIMAL(8,2)) BETWEEN 200 AND 399 THEN 'Muy Alto Riesgo (200-399)'
        WHEN CAST(score_crediticio_actual AS DECIMAL(8,2)) BETWEEN 400 AND 549 THEN 'Alto Riesgo (400-549)'
        WHEN CAST(score_crediticio_actual AS DECIMAL(8,2)) BETWEEN 550 AND 649 THEN 'Riesgo Medio (550-649)'
        WHEN CAST(score_crediticio_actual AS DECIMAL(8,2)) BETWEEN 650 AND 749 THEN 'Riesgo Bajo (650-749)'
        ELSE                                                               'Muy Bajo Riesgo (750-850)'
    END                                                             AS tramo_score_cliente,
-- Capacidad de pago------------------------------------------------------------
    CAST(ingreso_mensual_declarado       AS DECIMAL(18,2))          AS ingreso_mensual_declarado,
    CAST(nivel_endeudamiento_sistema_pct AS DECIMAL(8,4))           AS nivel_endeudamiento_sistema_pct,
    -- Tramo de endeudamiento para alertas de sobreendeudamiento
    CASE
        WHEN CAST(nivel_endeudamiento_sistema_pct AS DECIMAL(8,4)) <= 0.30 THEN 'Bajo (≤30%)'
        WHEN CAST(nivel_endeudamiento_sistema_pct AS DECIMAL(8,4)) <= 0.50 THEN 'Moderado (31-50%)'
        WHEN CAST(nivel_endeudamiento_sistema_pct AS DECIMAL(8,4)) <= 0.70 THEN 'Alto (51-70%)'
        ELSE                                                                     'Crítico (>70%)'
    END                                                             AS tramo_endeudamiento,
-- Segmentación comercial-------------------------------------------------------
    CAST(es_cliente_preferente AS INT)                              AS es_cliente_preferente,
    oficial_credito_asignado

FROM T3_clientes_riesgo_crediticio_RAW;
GO

/* ============================================================================================================================
   MART 4 - COSECHAS / VINTAGE
   Fuente  : T4_cosechas_vintage_riesgo_RAW
   Uso     : Página 3 - Curvas de cosecha
   Nota    : No tiene errores de calidad, solo tipificación y columna calculada
============================================================================================================================ */

DROP VIEW IF EXISTS MART_T4_cosechas;
GO

CREATE VIEW MART_T4_cosechas AS
SELECT
    -- cosecha_mes es la llave de relación con DIM_calendario en Power BI
    cosecha_mes,
    CAST(año_cosecha             AS INT)                             AS año_cosecha,
    CAST(mes_vida                AS INT)                             AS mes_vida,
    CAST(numero_creditos_cosecha AS INT)                             AS numero_creditos_cosecha,
    CAST(monto_original_cosecha  AS DECIMAL(18,2))                  AS monto_original_cosecha,
-- Performance------------------------------------------------------------------
    CAST(tasa_default_acumulada_pct AS DECIMAL(8,4))                AS tasa_default_acumulada_pct,
    CAST(saldo_vigente_periodo      AS DECIMAL(18,2))               AS saldo_vigente_periodo,
    CAST(numero_defaults_acumulados AS INT)                         AS numero_defaults_acumulados,
    -- Columna calculada: lectura positiva de la curva de vintage
    100 - CAST(tasa_default_acumulada_pct AS DECIMAL(8,4))          AS tasa_supervivencia_pct

FROM T4_cosechas_vintage_riesgo_RAW;
GO

/* ============================================================================================================================
   MART 5 - LOG DE CALIDAD
   Fuente  : LOG_calidad_datos
   Uso     : Página 4 - Notas Metodológicas
   Nota    : Expone el log con columnas calculadas de severidad y acción para Power BI
============================================================================================================================ */

DROP VIEW IF EXISTS MART_T5_calidad_datos;
GO

CREATE VIEW MART_T5_calidad_datos AS
SELECT
    id_log,
    fecha_analisis,
    tabla_origen,
    dimension,
    campo_afectado,
    tipo_error,
    total_registros,
    registros_con_error,
    pct_impacto,
    decision_tomada,
    analista,
    -- Semáforo de severidad para visualización en Power BI
    CASE
        WHEN pct_impacto >= 5.0 THEN 'Alta'
        WHEN pct_impacto >= 1.0 THEN 'Media'
        ELSE                         'Baja'
    END                                                             AS severidad,
    -- Categoría de la decisión para filtro rápido
    CASE
        WHEN decision_tomada LIKE 'Excluir%'    THEN 'Excluido'
        WHEN decision_tomada LIKE 'Corregir%'   THEN 'Corregido'
        WHEN decision_tomada LIKE 'Segmentar%'  THEN 'Segmentado'
        WHEN decision_tomada LIKE 'Normalizar%' THEN 'Normalizado'
        WHEN decision_tomada LIKE 'Cruzar%'     THEN 'Pendiente'
        ELSE                                         'Documentado'
    END                                                             AS accion_tomada,
    total_registros - registros_con_error                           AS registros_aptos

FROM LOG_calidad_datos;
GO

/* ============================================================================================================================
   VERIFICACIÓN FINAL - Las 5 vistas deben existir y devolver registros
============================================================================================================================ */

SELECT 'MART_T1_creditos'   AS vista, COUNT(*) AS registros FROM MART_T1_creditos   
UNION ALL
SELECT 'MART_T2_kpis',               COUNT(*) FROM MART_T2_kpis                    
UNION ALL
SELECT 'MART_T3_clientes',           COUNT(*) FROM MART_T3_clientes                
UNION ALL
SELECT 'MART_T4_cosechas',           COUNT(*) FROM MART_T4_cosechas                
UNION ALL
SELECT 'MART_T5_calidad_datos',      COUNT(*) FROM MART_T5_calidad_datos;          

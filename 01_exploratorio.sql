/* ============================================================================================================================
   PROYECTO    : Análisis de Riesgo Crediticio
   ARCHIVO     : 01_exploratorio.sql
   DESCRIPCIÓN : Análisis exploratorio sobre tablas RAW
                 Detecta nulos, duplicados, valores fuera de rango, inconsistencias
                 y guarda evidencia en LOG_calidad_datos para trazabilidad
   AUTOR       : Diego L. Villavicencio
   FECHA       : 2026-03-05
   VERSIÓN     : 1.0

   ORDEN DE EJECUCIÓN:
       PASO 1 - Crear tabla LOG_calidad_datos
       PASO 2 - Radiografía general (filas, nulos, duplicados)
       PASO 3 - Variables categóricas (valores distintos vs catálogo)
       PASO 4 - Variables numéricas (mínimos, máximos, promedios)
       PASO 5 -  Variables de fecha (rangos, inconsistencias lógicas)
       PASO 6 - Consistencia entre columnas relacionadas
       PASO 7 - Exclusiones por política
       PASO 8 - Reporte final consolidado
============================================================================================================================ */

USE RiesgoCrediticioProyecto;
GO

/* ============================================================================================================================
   PASO 1 - CREAR TABLA DE LOG
   Registra permanentemente todos los problemas encontrados con su impacto y decisión
============================================================================================================================ */

DROP TABLE IF EXISTS LOG_calidad_datos;

CREATE TABLE LOG_calidad_datos (
    id_log               INT IDENTITY(1,1) PRIMARY KEY,
    fecha_analisis       DATETIME          DEFAULT GETDATE(),
    tabla_origen         VARCHAR(100),
    dimension            VARCHAR(50),       -- Completitud | Validez | Consistencia | Unicidad | Exclusión por política
    campo_afectado       VARCHAR(100),
    tipo_error           VARCHAR(300),
    total_registros      INT,
    registros_con_error  INT,
    pct_impacto          DECIMAL(5,2),
    decision_tomada      VARCHAR(300),
    analista             VARCHAR(100)
);
GO

/* ============================================================================================================================
   PASO 2. RADIOGRAFÍA GENERAL
   Ejecuta primero como SELECT para ver resultados en pantalla
   Luego inserta en LOG los problemas confirmados
============================================================================================================================ */

-- 2.1 Conteo general por tabla
SELECT 'T1_creditos'  AS tabla, COUNT(*) AS total_filas FROM T1_creditos_riesgo_crediticio_RAW
UNION ALL
SELECT 'T2_kpis',              COUNT(*) FROM T2_kpis_mensuales_riesgo_RAW
UNION ALL
SELECT 'T3_clientes',          COUNT(*) FROM T3_clientes_riesgo_crediticio_RAW
UNION ALL
SELECT 'T4_cosechas',          COUNT(*) FROM T4_cosechas_vintage_riesgo_RAW;

-- 2.2 Nulos por columna - T1
SELECT
    COUNT(*)                                                                          AS total,
    SUM(CASE WHEN id_snapshot         IS NULL OR id_snapshot         = '' THEN 1 ELSE 0 END) AS nulos_id_snapshot,
    SUM(CASE WHEN id_cliente         IS NULL OR id_cliente         = '' THEN 1 ELSE 0 END) AS nulos_id_cliente,
    SUM(CASE WHEN tipo_persona       IS NULL OR tipo_persona       = '' THEN 1 ELSE 0 END) AS nulos_tipo_persona,
    SUM(CASE WHEN segmento           IS NULL OR segmento           = '' THEN 1 ELSE 0 END) AS nulos_segmento,
    SUM(CASE WHEN sector_economico   IS NULL OR sector_economico   = '' THEN 1 ELSE 0 END) AS nulos_sector,
    SUM(CASE WHEN zona_geografica    IS NULL OR zona_geografica    = '' THEN 1 ELSE 0 END) AS nulos_zona,
    SUM(CASE WHEN saldo_capital      IS NULL OR saldo_capital      = '' THEN 1 ELSE 0 END) AS nulos_saldo_capital,
    SUM(CASE WHEN calificacion_sbs   IS NULL OR calificacion_sbs   = '' THEN 1 ELSE 0 END) AS nulos_calificacion,
    SUM(CASE WHEN tipo_garantia      IS NULL OR tipo_garantia      = '' THEN 1 ELSE 0 END) AS nulos_garantia,
    SUM(CASE WHEN score_crediticio   IS NULL OR score_crediticio   = '' THEN 1 ELSE 0 END) AS nulos_score,
    SUM(CASE WHEN oficial_credito    IS NULL OR oficial_credito    = '' THEN 1 ELSE 0 END) AS nulos_oficial,
    SUM(CASE WHEN producto_crediticio IS NULL OR producto_crediticio = '' THEN 1 ELSE 0 END) AS nulos_producto,
    SUM(CASE WHEN fecha_desembolso   IS NULL OR fecha_desembolso   = '' THEN 1 ELSE 0 END) AS nulos_fecha_desembolso,
    SUM(CASE WHEN fecha_vencimiento  IS NULL OR fecha_vencimiento  = '' THEN 1 ELSE 0 END) AS nulos_fecha_vencimiento
FROM T1_creditos_riesgo_crediticio_RAW;

-- 2.3 Duplicados — T1
SELECT
    id_snapshot,
    COUNT(*) AS veces
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY id_snapshot
HAVING COUNT(*) > 1
ORDER BY veces DESC;

-- Detalle de los registros duplicados para comparar diferencias
SELECT *
FROM T1_creditos_riesgo_crediticio_RAW
WHERE id_snapshot IN (
    SELECT id_snapshot
    FROM T1_creditos_riesgo_crediticio_RAW
    GROUP BY id_snapshot
    HAVING COUNT(*) > 1
)
ORDER BY id_snapshot;

-- 2.4 Duplicados — T3
SELECT
    id_cliente,
    COUNT(*) AS veces
FROM T3_clientes_riesgo_crediticio_RAW
GROUP BY id_cliente
HAVING COUNT(*) > 1
ORDER BY veces DESC;

-- ── INSERTAR EN LOG ──────────────────────────────────────────────────────────

-- Nulos: sector_economico
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Completitud', 'sector_economico',
    'Nulo o vacío',
    COUNT(*),
    SUM(CASE WHEN sector_economico IS NULL OR sector_economico = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN sector_economico IS NULL OR sector_economico = '' THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Cruzar con base tributaria. Si irrecuperable → registrar como Sin Clasificar.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Nulos: tipo_garantia
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Completitud', 'tipo_garantia',
    'Nulo o vacío',
    COUNT(*),
    SUM(CASE WHEN tipo_garantia IS NULL OR tipo_garantia = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN tipo_garantia IS NULL OR tipo_garantia = '' THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Registrar como Sin Información. Incluir en análisis con nota.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Nulos: score_crediticio
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Completitud', 'score_crediticio',
    'Nulo o vacío',
    COUNT(*),
    SUM(CASE WHEN score_crediticio IS NULL OR score_crediticio = '' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN score_crediticio IS NULL OR score_crediticio = '' THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Segmentar como Sin Score. Incluir en dashboard con etiqueta separada.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Duplicados: id_snapshot
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Unicidad', 'id_snapshot',
    'Registros duplicados',
    (SELECT COUNT(*) FROM T1_creditos_riesgo_crediticio_RAW),
    (SELECT COUNT(*) FROM T1_creditos_riesgo_crediticio_RAW)
        - (SELECT COUNT(DISTINCT id_snapshot) FROM T1_creditos_riesgo_crediticio_RAW),
    ROUND(100.0 *
        ((SELECT COUNT(*) FROM T1_creditos_riesgo_crediticio_RAW)
            - (SELECT COUNT(DISTINCT id_snapshot) FROM T1_creditos_riesgo_crediticio_RAW))
        / (SELECT COUNT(*) FROM T1_creditos_riesgo_crediticio_RAW), 2),
    'Conservar primer registro por id_credito. Eliminar copia con ROW_NUMBER().',
    'Diego L. Villavicencio';

/* ============================================================================================================================
   PASO 3 — VARIABLES CATEGÓRICAS
   Detecta valores fuera de catálogo, variantes de escritura, abreviaciones
============================================================================================================================ */

-- 3.1 Sector económico — ver todos los valores distintos
SELECT sector_economico, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY sector_economico
ORDER BY frecuencia DESC;

-- 3.2 Calificación SBS — comparar contra catálogo regulatorio
-- Valores válidos: Normal, CPP, Deficiente, Dudoso, Pérdida
SELECT calificacion_sbs, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY calificacion_sbs
ORDER BY frecuencia DESC;

-- 3.3 Tipo garantía
SELECT tipo_garantia, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY tipo_garantia
ORDER BY frecuencia DESC;

-- 3.4 Moneda — solo debe haber PEN y USD
SELECT moneda, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY moneda
ORDER BY frecuencia DESC;

-- 3.5 Segmento
SELECT segmento, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY segmento
ORDER BY frecuencia DESC;

-- 3.6 Producto crediticio
SELECT producto_crediticio, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY producto_crediticio
ORDER BY frecuencia DESC;

-- 3.7 Tipo persona
SELECT tipo_persona, COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY tipo_persona
ORDER BY frecuencia DESC;

-- ── INSERTAR EN LOG ──────────────────────────────────────────────────────────

-- Sector fuera de catálogo
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Validez', 'sector_economico',
    'Valor fuera del catálogo oficial',
    COUNT(*),
    SUM(CASE WHEN sector_economico NOT IN (
            'Comercio','Industria','Servicios','Construcción',
            'Agroindustria','Transporte','Tecnología','Salud')
             AND sector_economico IS NOT NULL AND sector_economico != ''
        THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN sector_economico NOT IN (
            'Comercio','Industria','Servicios','Construcción',
            'Agroindustria','Transporte','Tecnología','Salud')
             AND sector_economico IS NOT NULL AND sector_economico != ''
        THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Normalizar con tabla de mapeo CASE WHEN. Irrecuperables → Sin Clasificar.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

/* ============================================================================================================================
   PASO 4 — VARIABLES NUMÉRICAS
   Detecta negativos imposibles, máximos absurdos, valores fuera de rango
============================================================================================================================ */

-- 4.1 Estadísticas generales de campos numéricos clave
SELECT
    -- Montos
    MIN(TRY_CAST(saldo_capital            AS DECIMAL(18,2))) AS min_saldo_capital,
    MAX(TRY_CAST(saldo_capital            AS DECIMAL(18,2))) AS max_saldo_capital,
    AVG(TRY_CAST(saldo_capital            AS DECIMAL(18,2))) AS avg_saldo_capital,
    -- Tasas
    MIN(TRY_CAST(tasa_nominal_anual_pct   AS DECIMAL(8,4)))  AS min_tasa,
    MAX(TRY_CAST(tasa_nominal_anual_pct   AS DECIMAL(8,4)))  AS max_tasa,
    AVG(TRY_CAST(tasa_nominal_anual_pct   AS DECIMAL(8,4)))  AS avg_tasa,
    -- Mora
    MIN(TRY_CAST(dias_atraso              AS INT))            AS min_dias_atraso,
    MAX(TRY_CAST(dias_atraso              AS INT))            AS max_dias_atraso,
    -- Score
    MIN(TRY_CAST(score_crediticio AS DECIMAL(8,2)))            AS min_score,
    MAX(TRY_CAST(score_crediticio AS DECIMAL(8,2)))            AS max_score,
    -- PD
    MIN(TRY_CAST(pd_probabilidad_default  AS DECIMAL(8,4)))  AS min_pd,
    MAX(TRY_CAST(pd_probabilidad_default  AS DECIMAL(8,4)))  AS max_pd
FROM T1_creditos_riesgo_crediticio_RAW;

-- 4.2 Distribución de días de atraso (ver si hay negativos y qué tan frecuentes)
SELECT
    CASE
        WHEN TRY_CAST(dias_atraso AS INT) < 0   THEN 'Negativo (error)'
        WHEN TRY_CAST(dias_atraso AS INT) = 0   THEN 'Normal (0 días)'
        WHEN TRY_CAST(dias_atraso AS INT) <= 8  THEN '1-8 días'
        WHEN TRY_CAST(dias_atraso AS INT) <= 30 THEN '9-30 días'
        WHEN TRY_CAST(dias_atraso AS INT) <= 60 THEN '31-60 días'
        ELSE                                         '60+ días'
    END AS tramo,
    COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY
    CASE
        WHEN TRY_CAST(dias_atraso AS INT) < 0   THEN 'Negativo (error)'
        WHEN TRY_CAST(dias_atraso AS INT) = 0   THEN 'Normal (0 días)'
        WHEN TRY_CAST(dias_atraso AS INT) <= 8  THEN '1-8 días'
        WHEN TRY_CAST(dias_atraso AS INT) <= 30 THEN '9-30 días'
        WHEN TRY_CAST(dias_atraso AS INT) <= 60 THEN '31-60 días'
        ELSE                                         '60+ días'
    END
ORDER BY MIN(TRY_CAST(dias_atraso AS INT));

-- 4.3 Score fuera de rango válido [200-850]
SELECT
    score_crediticio,
    COUNT(*) AS frecuencia
FROM T1_creditos_riesgo_crediticio_RAW
WHERE TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NOT NULL
  AND CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850
GROUP BY score_crediticio
ORDER BY frecuencia DESC;

-- ── INSERTAR EN LOG ──────────────────────────────────────────────────────────

-- Saldo capital negativo
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Validez', 'saldo_capital',
    'Monto negativo',
    COUNT(*),
    SUM(CASE WHEN TRY_CAST(saldo_capital AS DECIMAL(18,2)) < 0 THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN TRY_CAST(saldo_capital AS DECIMAL(18,2)) < 0 THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Excluir del dashboard. Registrar en tabla de excepciones.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Tasa fuera de rango
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Validez', 'tasa_nominal_anual_pct',
    'Tasa fuera de rango válido (0.1% - 100%)',
    COUNT(*),
    SUM(CASE WHEN TRY_CAST(tasa_nominal_anual_pct AS DECIMAL(8,4)) NOT BETWEEN 0.1 AND 100 THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN TRY_CAST(tasa_nominal_anual_pct AS DECIMAL(8,4)) NOT BETWEEN 0.1 AND 100 THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Excluir del dashboard.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Días de atraso negativos
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Consistencia', 'dias_atraso',
    'Días de atraso negativos',
    COUNT(*),
    SUM(CASE WHEN TRY_CAST(dias_atraso AS INT) < 0 THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN TRY_CAST(dias_atraso AS INT) < 0 THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Corregir a 0. Documentar como crédito al día.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

-- Score fuera de rango
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Validez', 'score_crediticio',
    'Score fuera de rango válido [200-850]',
    COUNT(*),
    SUM(CASE WHEN TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NOT NULL
                  AND CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850 THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN TRY_CAST(score_crediticio AS DECIMAL(8,2)) IS NOT NULL
                  AND CAST(score_crediticio AS DECIMAL(8,2)) NOT BETWEEN 200 AND 850 THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Tratar como nulo. Segmentar como Sin Score.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

/* ============================================================================================================================
   PASO 5 — VARIABLES DE FECHA
   Detecta fechas fuera de rango, inconsistencias lógicas entre fechas
============================================================================================================================ */

-- 5.1 Rango de fechas — confirmar período esperado 2024-2025
SELECT
    MIN(TRY_CAST(fecha_desembolso  AS DATE)) AS primera_fecha_desembolso,
    MAX(TRY_CAST(fecha_desembolso  AS DATE)) AS ultima_fecha_desembolso,
    MIN(TRY_CAST(fecha_vencimiento AS DATE)) AS primera_fecha_vencimiento,
    MAX(TRY_CAST(fecha_vencimiento AS DATE)) AS ultima_fecha_vencimiento,
    MIN(TRY_CAST(fecha_corte       AS DATE)) AS primer_corte,
    MAX(TRY_CAST(fecha_corte       AS DATE)) AS ultimo_corte
FROM T1_creditos_riesgo_crediticio_RAW;

-- 5.2 Fechas futuras de desembolso (imposible)
SELECT COUNT(*) AS desembolsos_futuros
FROM T1_creditos_riesgo_crediticio_RAW
WHERE TRY_CAST(fecha_desembolso AS DATE) > GETDATE();

-- 5.3 Vencimiento anterior o igual al desembolso (imposible)
SELECT COUNT(*) AS fechas_inconsistentes
FROM T1_creditos_riesgo_crediticio_RAW
WHERE TRY_CAST(fecha_vencimiento AS DATE) <= TRY_CAST(fecha_desembolso AS DATE);

-- Ver el detalle de los registros con fechas inconsistentes
SELECT id_credito, fecha_desembolso, fecha_vencimiento, plazo_meses
FROM T1_creditos_riesgo_crediticio_RAW
WHERE TRY_CAST(fecha_vencimiento AS DATE) <= TRY_CAST(fecha_desembolso AS DATE);

-- ── INSERTAR EN LOG ──────────────────────────────────────────────────────────

INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Consistencia', 'fecha_vencimiento vs fecha_desembolso',
    'Fecha vencimiento anterior o igual al desembolso',
    COUNT(*),
    SUM(CASE WHEN TRY_CAST(fecha_vencimiento AS DATE) <= TRY_CAST(fecha_desembolso AS DATE) THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN TRY_CAST(fecha_vencimiento AS DATE) <= TRY_CAST(fecha_desembolso AS DATE) THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Excluir. Verificar con expediente físico para corrección en origen.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

/* ============================================================================================================================
   PASO 6 — CONSISTENCIA ENTRE COLUMNAS RELACIONADAS
   Detecta contradicciones lógicas entre campos que deben ser coherentes entre sí
============================================================================================================================ */

-- 6.1 Calificación SBS inconsistente con días de atraso
-- Regla SBS Perú (Res. 11356-2008): Normal = 0d | CPP = 1-8d | Deficiente = 9-30d | Dudoso = 31-60d | Pérdida > 60d
-- Se verifica en ambas direcciones: calificación demasiado buena O demasiado mala vs días reales
SELECT
    calificacion_sbs,
    CAST(dias_atraso AS INT) AS dias_atraso,
    COUNT(*) AS casos
FROM T1_creditos_riesgo_crediticio_RAW
WHERE
    -- Calificación mejor de lo que corresponde por días
    (calificacion_sbs = 'Normal'     AND TRY_CAST(dias_atraso AS INT) > 0)
 OR (calificacion_sbs = 'CPP'        AND (TRY_CAST(dias_atraso AS INT) > 8
                                      OR  TRY_CAST(dias_atraso AS INT) < 1))
 OR (calificacion_sbs = 'Deficiente' AND (TRY_CAST(dias_atraso AS INT) > 30
                                      OR  TRY_CAST(dias_atraso AS INT) < 9))
 OR (calificacion_sbs = 'Dudoso'     AND (TRY_CAST(dias_atraso AS INT) > 60
                                      OR  TRY_CAST(dias_atraso AS INT) < 31))
    -- Calificación peor de lo que corresponde por días
 OR (calificacion_sbs = 'Pérdida'    AND TRY_CAST(dias_atraso AS INT) <= 60)
GROUP BY calificacion_sbs, CAST(dias_atraso AS INT)
ORDER BY casos DESC;

-- 6.2 Saldo mora > 0 cuando días de atraso <= 0 (contradicción)
-- Incluye negativos porque un pago anticipado tampoco debería tener mora acumulada
SELECT COUNT(*) AS contradicciones_mora
FROM T1_creditos_riesgo_crediticio_RAW
WHERE TRY_CAST(dias_atraso AS INT) <= 0
  AND TRY_CAST(saldo_mora AS DECIMAL(18,2)) > 0;

-- 6.3 Provisión requerida vs cálculo teórico (saldo * tasa / 100)
-- Diferencia > 1 sol indica error de cálculo
SELECT
    id_credito,
    CAST(saldo_capital       AS DECIMAL(18,2))                             AS saldo_capital,
    CAST(tasa_provision_pct  AS DECIMAL(8,4))                              AS tasa_prov,
    CAST(provision_requerida AS DECIMAL(18,2))                             AS prov_registrada,
    ROUND(CAST(saldo_capital AS DECIMAL(18,2))
        * CAST(tasa_provision_pct AS DECIMAL(8,4)) / 100, 2)              AS prov_calculada,
    ABS(CAST(provision_requerida AS DECIMAL(18,2))
        - ROUND(CAST(saldo_capital AS DECIMAL(18,2))
        * CAST(tasa_provision_pct AS DECIMAL(8,4)) / 100, 2))             AS diferencia
FROM T1_creditos_riesgo_crediticio_RAW
WHERE ABS(CAST(provision_requerida AS DECIMAL(18,2))
        - ROUND(CAST(saldo_capital AS DECIMAL(18,2))
        * CAST(tasa_provision_pct AS DECIMAL(8,4)) / 100, 2)) > 1
ORDER BY diferencia DESC;

/* ============================================================================================================================
   PASO 7 — EXCLUSIONES POR POLÍTICA
   Créditos que deben salir del dashboard por razones regulatorias o de negocio
============================================================================================================================ */

-- 7.1 Ver distribución del flag_error
SELECT
    ISNULL(flag_error, 'Sin error') AS flag_error,
    COUNT(*) AS cantidad
FROM T1_creditos_riesgo_crediticio_RAW
GROUP BY flag_error
ORDER BY cantidad DESC;

-- ── INSERTAR EN LOG ──────────────────────────────────────────────────────────

-- Proceso judicial
INSERT INTO LOG_calidad_datos (tabla_origen, dimension, campo_afectado, tipo_error,
    total_registros, registros_con_error, pct_impacto, decision_tomada, analista)
SELECT 'T1_creditos_riesgo_crediticio_RAW', 'Exclusión por política', 'flag_error',
    'Crédito en proceso judicial — no debe consolidarse con cartera operativa',
    COUNT(*),
    SUM(CASE WHEN ISNULL(flag_error,'') = 'EXCLUIR_PROCESO_JUDICIAL' THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN ISNULL(flag_error,'') = 'EXCLUIR_PROCESO_JUDICIAL' THEN 1 ELSE 0 END) / COUNT(*), 2),
    'Excluir del dashboard. Reportar al área legal en informe separado.',
    'Diego L. Villavicencio'
FROM T1_creditos_riesgo_crediticio_RAW;

/* ============================================================================================================================
   PASO 8 — REPORTE FINAL CONSOLIDADO
   Vista completa de todos los problemas encontrados ordenados por severidad
============================================================================================================================ */

SELECT
    id_log,
    tabla_origen,
    dimension,
    campo_afectado,
    tipo_error,
    total_registros,
    registros_con_error,
    pct_impacto,
    CASE
        WHEN pct_impacto >= 5.0 THEN 'Alta'
        WHEN pct_impacto >= 1.0 THEN 'Media'
        ELSE                         'Baja'
    END                             AS severidad,
    CASE
        WHEN decision_tomada LIKE 'Excluir%'    THEN 'Excluido'
        WHEN decision_tomada LIKE 'Corregir%'   THEN 'Corregido'
        WHEN decision_tomada LIKE 'Segmentar%'  THEN 'Segmentado'
        WHEN decision_tomada LIKE 'Normalizar%' THEN 'Normalizado'
        WHEN decision_tomada LIKE 'Cruzar%'     THEN 'Pendiente verificación'
        ELSE                                         'Documentado'
    END                             AS accion_tomada,
    decision_tomada,
    analista,
    CONVERT(VARCHAR(16), fecha_analisis, 120) AS fecha_analisis
FROM LOG_calidad_datos
ORDER BY
    CASE dimension
        WHEN 'Exclusión por política' THEN 1
        WHEN 'Unicidad'               THEN 2
        WHEN 'Validez'                THEN 3
        WHEN 'Consistencia'           THEN 4
        WHEN 'Completitud'            THEN 5
    END,
    pct_impacto DESC;

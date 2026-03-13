/* ============================================================================================================================
   PROYECTO    : Análisis de Riesgo Crediticio
   BASE DATOS  : RiesgoCrediticioProyecto
   DESCRIPCIÓN : Creación de base de datos, tablas RAW e importación de archivos fuente
   AUTOR       : Diego L. Villavicencio
   FECHA       : 2026-03-03
   VERSIÓN     : 1.0
   
   TABLAS RAW:
       T1_creditos_riesgo_crediticio_RAW   
       T2_kpis_mensuales_riesgo_RAW        
       T3_clientes_riesgo_crediticio_RAW   
       T4_cosechas_vintage_riesgo_RAW      
       
   NOTA: Todas las columnas se cargan como VARCHAR(MAX) para preservar los datos
         originales sin conversión. La tipificación ocurre en la capa MART.
   FUENTE: C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\
============================================================================================================================ */

-- ============================================================================================================================
-- 0. INICIALIZACIÓN
-- ============================================================================================================================

CREATE DATABASE RiesgoCrediticioProyecto;
GO

USE RiesgoCrediticioProyecto;
GO

-- Ruta base de archivos fuente
-- C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\

-- ============================================================================================================================
-- 1. T1 - CRÉDITOS
--    Granularidad : Operación crediticia individual
--    Período      : 2024 – 2025
--    Errores RAW  : Montos negativos, fechas inválidas, tasas imposibles,
--                   sectores fuera de catálogo, nulos estructurales, duplicados,
--                   créditos judiciales y de empleados (excluir del dashboard)
-- ============================================================================================================================

DROP TABLE IF EXISTS T1_creditos_riesgo_crediticio_RAW;

CREATE TABLE T1_creditos_riesgo_crediticio_RAW (
    -- Identificación
    id_credito                    VARCHAR(MAX),
    id_cliente                    VARCHAR(MAX),
    tipo_persona                  VARCHAR(MAX),
    segmento                      VARCHAR(MAX),
    sector_economico              VARCHAR(MAX),
    zona_geografica               VARCHAR(MAX),
    oficial_credito               VARCHAR(MAX),
    -- Producto
    producto_crediticio           VARCHAR(MAX),
    moneda                        VARCHAR(MAX),
    -- Fechas
    fecha_desembolso              VARCHAR(MAX),
    fecha_vencimiento             VARCHAR(MAX),
    fecha_corte                   VARCHAR(MAX),
    plazo_meses                   VARCHAR(MAX),
    -- Montos
    monto_aprobado                VARCHAR(MAX),
    monto_desembolsado            VARCHAR(MAX),
    saldo_capital                 VARCHAR(MAX),
    saldo_interes                 VARCHAR(MAX),
    saldo_mora                    VARCHAR(MAX),
    saldo_total_exposicion        VARCHAR(MAX),
    -- Tasas
    tasa_nominal_anual_pct        VARCHAR(MAX),
    tasa_efectiva_anual_pct       VARCHAR(MAX),
    spread_pct                    VARCHAR(MAX),
    -- Mora y calificación
    dias_atraso                   VARCHAR(MAX),
    numero_cuotas_vencidas        VARCHAR(MAX),
    calificacion_sbs              VARCHAR(MAX),
    -- Provisiones
    tasa_provision_pct            VARCHAR(MAX),
    provision_requerida           VARCHAR(MAX),
    provision_constituida         VARCHAR(MAX),
    deficit_superavit_provision   VARCHAR(MAX),
    -- Garantías
    tipo_garantia                 VARCHAR(MAX),
    valor_garantia                VARCHAR(MAX),
    cobertura_garantia_ratio      VARCHAR(MAX),
    ltv_loan_to_value             VARCHAR(MAX),
    -- Modelos internos (Basilea II/III)
    score_crediticio              VARCHAR(MAX),
    pd_probabilidad_default       VARCHAR(MAX),
    lgd_loss_given_default        VARCHAR(MAX),
    ead_exposure_at_default       VARCHAR(MAX),
    el_expected_loss              VARCHAR(MAX),
    -- Ratios financieros
    dscr_cobertura_deuda          VARCHAR(MAX),
    ratio_endeudamiento           VARCHAR(MAX),
    ratio_liquidez_corriente      VARCHAR(MAX),
    ratio_cobertura_garantia      VARCHAR(MAX),
    ingresos_anuales              VARCHAR(MAX),
    cuota_mensual                 VARCHAR(MAX),
    -- Comportamiento
    numero_refinanciaciones       VARCHAR(MAX),
    es_refinanciado               VARCHAR(MAX),
    veces_pago_anticipado         VARCHAR(MAX),
    numero_creditos_en_entidad    VARCHAR(MAX),
    numero_creditos_en_sistema    VARCHAR(MAX),
    antiguedad_cliente_meses      VARCHAR(MAX),
    edad_empresa_anos             VARCHAR(MAX),
    -- Castigos
    en_castigo                    VARCHAR(MAX),
    monto_castigado               VARCHAR(MAX),
    -- Control de calidad
    flag_error                    VARCHAR(MAX)
);

BULK INSERT T1_creditos_riesgo_crediticio_RAW
FROM 'C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\TABLA_1_creditos_riesgo_crediticio_RAW.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ';',
    ROWTERMINATOR   = '\n',
    TABLOCK,
    CODEPAGE        = '65001'
);

ALTER TABLE T1_creditos_riesgo_crediticio_RAW
ADD id_snapshot AS (id_credito + '-' + REPLACE(fecha_corte, '-', '')) PERSISTED;

select * from T1_creditos_riesgo_crediticio_RAW


-- Verificación T1
SELECT
    COUNT(*)                   AS total_filas,       
    COUNT(DISTINCT id_credito) AS creditos_unicos,   
    COUNT(DISTINCT id_cliente) AS clientes_unicos,   
    SUM(CASE WHEN flag_error != '' AND flag_error IS NOT NULL 
             THEN 1 ELSE 0 END) AS registros_con_error
FROM T1_creditos_riesgo_crediticio_RAW;
GO

-- ============================================================================================================================
-- 2. T2 - KPIs MENSUALES
--    Granularidad : Mes calendario agregado (nivel portafolio)
--    Período      : Enero 2024 – Diciembre 2025
--    Nota         : Tabla independiente, no se relaciona con T1/T3/T4
-- ============================================================================================================================

DROP TABLE IF EXISTS T2_kpis_mensuales_riesgo_RAW;

CREATE TABLE T2_kpis_mensuales_riesgo_RAW (
    -- Período
    fecha_mes                       VARCHAR(MAX),
    año                             VARCHAR(MAX),
    mes                             VARCHAR(MAX),
    -- Cartera
    cartera_bruta_total             VARCHAR(MAX),
    cartera_vigente                 VARCHAR(MAX),
    cartera_mora                    VARCHAR(MAX),
    tasa_mora_pct                   VARCHAR(MAX),
    -- Provisiones
    provision_total_constituida     VARCHAR(MAX),
    ratio_cobertura_provision       VARCHAR(MAX),
    -- Originación y castigos
    nuevos_creditos_desembolsados   VARCHAR(MAX),
    creditos_castigados_mes         VARCHAR(MAX),
    recuperaciones_castigos         VARCHAR(MAX),
    -- Rentabilidad
    roe_retorno_patrimonio          VARCHAR(MAX),
    roa_retorno_activos             VARCHAR(MAX),
    nim_margen_interes_neto         VARCHAR(MAX),
    costo_riesgo                    VARCHAR(MAX),
    -- Eficiencia y solvencia
    ratio_eficiencia                VARCHAR(MAX),
    ratio_capital_tier1             VARCHAR(MAX)
);

BULK INSERT T2_kpis_mensuales_riesgo_RAW
FROM 'C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\TABLA_2_kpis_mensuales_riesgo_RAW.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '\n',
    TABLOCK,
    CODEPAGE        = '65001'
);

-- Verificación T2
SELECT
    COUNT(*)       AS total_filas,    -- Esperado: 24
    MIN(fecha_mes) AS primer_mes,     -- Esperado: 2024-01-31
    MAX(fecha_mes) AS ultimo_mes      -- Esperado: 2025-12-31
FROM T2_kpis_mensuales_riesgo_RAW;
GO

-- ============================================================================================================================
-- 3. T3 - CLIENTES
--    Granularidad : Cliente consolidado (1 registro por cliente)
--    Clave        : id_cliente → relaciona con T1 (1 cliente : N créditos)
-- ============================================================================================================================

DROP TABLE IF EXISTS T3_clientes_riesgo_crediticio_RAW;

CREATE TABLE T3_clientes_riesgo_crediticio_RAW (
    -- Identificación
    id_cliente                      VARCHAR(MAX),
    tipo_persona                    VARCHAR(MAX),
    segmento                        VARCHAR(MAX),
    sector_economico                VARCHAR(MAX),
    zona_geografica                 VARCHAR(MAX),
    -- Relación comercial
    fecha_alta                      VARCHAR(MAX),
    numero_productos_activos        VARCHAR(MAX),
    -- Riesgo consolidado
    score_crediticio_actual         VARCHAR(MAX),
    saldo_total_deuda               VARCHAR(MAX),
    maximo_dias_atraso_historico    VARCHAR(MAX),
    calificacion_consolidada        VARCHAR(MAX),
    -- Capacidad de pago
    ingreso_mensual_declarado       VARCHAR(MAX),
    nivel_endeudamiento_sistema_pct VARCHAR(MAX),
    -- Segmentación comercial
    es_cliente_preferente           VARCHAR(MAX),
    oficial_credito_asignado        VARCHAR(MAX)
);

BULK INSERT T3_clientes_riesgo_crediticio_RAW
FROM 'C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\TABLA_3_clientes_riesgo_crediticio_RAW.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '\n',
    TABLOCK,
    CODEPAGE        = '65001'
);

-- Verificación T3
SELECT
    COUNT(*)                   AS total_filas,      -- Esperado: 390
    COUNT(DISTINCT id_cliente) AS clientes_unicos   -- Esperado: 390
FROM T3_clientes_riesgo_crediticio_RAW;
GO

-- ============================================================================================================================
-- 4. T4 - COSECHAS / VINTAGE
--    Granularidad : Cohorte por mes de desembolso × mes de vida
--    Clave        : cosecha_mes → se deriva de FORMAT(fecha_desembolso, 'yyyy-MM') en T1
--    Nota         : Cosechas 2025 tienen menos períodos por right-censoring (corte dic-2025)
-- ============================================================================================================================

DROP TABLE IF EXISTS T4_cosechas_vintage_riesgo_RAW;

CREATE TABLE T4_cosechas_vintage_riesgo_RAW (
    -- Cosecha
    cosecha_mes                 VARCHAR(MAX),
    año_cosecha                 VARCHAR(MAX),
    mes_vida                    VARCHAR(MAX),
    numero_creditos_cosecha     VARCHAR(MAX),
    monto_original_cosecha      VARCHAR(MAX),
    -- Performance
    tasa_default_acumulada_pct  VARCHAR(MAX),
    saldo_vigente_periodo       VARCHAR(MAX),
    numero_defaults_acumulados  VARCHAR(MAX)
);

BULK INSERT T4_cosechas_vintage_riesgo_RAW
FROM 'C:\Users\Usuario iTC\Desktop\SQL Curso\Proyectos\3. Riesgo crediticio\Base de datos\TABLA_4_cosechas_vintage_riesgo_RAW.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '\n',
    TABLOCK,
    CODEPAGE        = '65001'
);

-- Verificación T4
SELECT
    COUNT(*)                    AS total_filas,       -- Esperado: 222
    COUNT(DISTINCT cosecha_mes) AS cosechas_unicas,   -- Esperado: 24
    MAX(CAST(mes_vida AS INT))  AS max_meses_vida     -- Esperado: 12
FROM T4_cosechas_vintage_riesgo_RAW;

-- Distribución por cosecha (right-censoring visible en cosechas 2025)
SELECT
    cosecha_mes,
    COUNT(*)           AS periodos_disponibles,
    MAX(CAST(mes_vida AS INT)) AS ultimo_mes_vida
FROM T4_cosechas_vintage_riesgo_RAW
GROUP BY cosecha_mes
ORDER BY cosecha_mes;
GO

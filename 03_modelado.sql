/* ============================================================================================================================
   PROYECTO    : Análisis de Riesgo Crediticio
   ARCHIVO     : 03_modelado.sql
   DESCRIPCIÓN : Capa Star Schema — Tablas físicas DIM y FCT para Power BI
                 Lee exclusivamente desde las vistas MART (nunca desde RAW)
                 Genera el modelo dimensional listo para conectar a Power BI
   AUTOR       : Diego L. Villavicencio
   FECHA       : 2026-03-05
   VERSIÓN     : 1.2 — Añadidas DIM_riesgo_credito, DIM_comportamiento, DIM_exposicion
                        DIM_calificacion simplificada (tramo_score movido a DIM_riesgo_credito)

   MODELO 1 — Star Schema de Créditos (Páginas 1, 2, 3)
       DIM_geografia       → zonas geográficas únicas
       DIM_segmento        → combinaciones tipo_persona + segmento
       DIM_sector          → sectores económicos normalizados
       DIM_producto        → combinaciones producto + moneda
       DIM_calificacion    → calificación SBS + tramo_mora  [simplificada en v1.2]
       DIM_garantia        → tipos de garantía
       DIM_oficial         → oficiales de crédito
       DIM_riesgo_credito  → tramo_score + tramo_pd + tramo_lgd  [NUEVO v1.2]
       DIM_comportamiento  → perfil_refinanciacion + flag_castigo_activo  [NUEVO v1.2]
       DIM_exposicion      → tramo_plazo + tramo_ltv + tramo_spread  [NUEVO v1.2]
       FCT_creditos        → tabla de hechos con métricas + llaves foráneas
       MART_T3_clientes    → satélite conectado por id_cliente
       MART_T4_cosechas    → satélite conectado a DIM_calendario por cosecha_mes

   MODELO 2 — Star Schema de KPIs (Página 1)
       DIM_calendario      → se crea en Power BI con DAX (CALENDAR function)
       MART_T2_kpis        → tabla de KPIs conectada a DIM_calendario por fecha_mes

   NOTA: DIM_calendario NO se crea en SQL. Se genera en Power BI con DAX y
         centraliza todas las relaciones temporales incluyendo cosecha_mes de T4.
         DIM_cosecha eliminada — DIM_calendario cubre su función al incluir
         una columna cosecha_mes = FORMAT([Date], "yyyy-MM")

   CAMBIOS v1.2 vs v1.1:
       — DIM_calificacion    : eliminado tramo_score — reducía combinaciones de ~25 a ~10
                                tramo_score se mueve a DIM_riesgo_credito para poder
                                filtrar score y calificación regulatoria de forma independiente
       — DIM_riesgo_credito  : NUEVA — modelo interno Basilea completo
                                tramo_score (score del cliente por bandas [200-850])
                                tramo_pd    (probabilidad de default: 5 bandas operativas)
                                tramo_lgd   (severidad de pérdida: 3 bandas)
       — DIM_comportamiento  : NUEVA — historial de refinanciaciones + estado de castigo
                                perfil_refinanciacion (sin refinanciación / una vez /
                                reincidente / problemático)
                                flag_castigo_activo (etiqueta legible de en_castigo)
       — DIM_exposicion      : NUEVA — estructura del crédito en tres ejes
                                tramo_plazo  (corto / mediano / largo / muy largo)
                                tramo_ltv    (bajo / moderado / alto / crítico)
                                tramo_spread (negativo / bajo / normal / alto)
       — FCT_creditos        : añadidas 3 llaves foráneas nuevas. Los campos numéricos
                                (score, pd, lgd, numero_refinanciaciones, ltv, spread,
                                plazo_meses) y los flags binarios (flag_score, es_refinanciado,
                                en_castigo) se conservan en FCT para cálculos DAX
============================================================================================================================ */

USE RiesgoCrediticioProyecto;
GO

/* ============================================================================================================================
   MODELO 1 — DIMENSIONES EXISTENTES
   DIM 1 a DIM 7 sin cambios, excepto DIM_calificacion que elimina tramo_score
============================================================================================================================ */

-- ── DIM 1: GEOGRAFÍA ─────────────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_geografia;

SELECT
    ROW_NUMBER() OVER (ORDER BY zona_geografica)             AS id_geografia,
    combinaciones.zona_geografica
INTO DIM_geografia
FROM (
    SELECT DISTINCT zona_geografica
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 2: SEGMENTO ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_segmento;

SELECT
    ROW_NUMBER() OVER (ORDER BY tipo_persona, segmento)      AS id_segmento,
    combinaciones.tipo_persona,
    combinaciones.segmento
INTO DIM_segmento
FROM (
    SELECT DISTINCT tipo_persona, segmento
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 3: SECTOR ECONÓMICO ──────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_sector;

SELECT
    ROW_NUMBER() OVER (ORDER BY sector_economico)            AS id_sector,
    combinaciones.sector_economico
INTO DIM_sector
FROM (
    SELECT DISTINCT sector_economico
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 4: PRODUCTO ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_producto;

SELECT
    ROW_NUMBER() OVER (ORDER BY producto_crediticio, moneda) AS id_producto,
    combinaciones.producto_crediticio,
    combinaciones.moneda
INTO DIM_producto
FROM (
    SELECT DISTINCT producto_crediticio, moneda
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 5: CALIFICACIÓN SBS + TRAMO MORA  [simplificada en v1.2] ─────────────
-- tramo_score eliminado — ahora vive en DIM_riesgo_credito
-- Esto reduce las combinaciones de ~25 a ~10 y permite filtrar
-- la calificación regulatoria y el scoring interno de forma independiente
DROP TABLE IF EXISTS DIM_calificacion;

SELECT
    ROW_NUMBER() OVER (ORDER BY calificacion_sbs, tramo_mora) AS id_calificacion,
    combinaciones.calificacion_sbs,
    combinaciones.tramo_mora
INTO DIM_calificacion
FROM (
    SELECT DISTINCT calificacion_sbs, tramo_mora
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 6: GARANTÍA ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_garantia;

SELECT
    ROW_NUMBER() OVER (ORDER BY tipo_garantia)               AS id_garantia,
    combinaciones.tipo_garantia
INTO DIM_garantia
FROM (
    SELECT DISTINCT tipo_garantia
    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 7: OFICIAL DE CRÉDITO ────────────────────────────────────────────────
DROP TABLE IF EXISTS DIM_oficial;

SELECT
    ROW_NUMBER() OVER (ORDER BY oficial_credito)             AS id_oficial,
    combinaciones.oficial_credito
INTO DIM_oficial
FROM (
    SELECT DISTINCT oficial_credito
    FROM MART_T1_creditos
) AS combinaciones;

/* ============================================================================================================================
   MODELO 1 — DIMENSIONES NUEVAS v1.2
   Tres DIM derivadas de métricas numéricas presentes en MART_T1_creditos
   Los valores numéricos brutos se conservan en FCT para cálculos DAX
   Las DIM solo contienen tramos/etiquetas para slicers y agrupadores
============================================================================================================================ */

-- ── DIM 8: RIESGO DE CRÉDITO (MODELO INTERNO BASILEA) ────────────────────────
-- Agrupa los tres ejes del modelo interno: score del cliente, PD y LGD
-- Permite cruzar el perfil de riesgo interno con la calificación regulatoria
-- SBS sin que interfieran en la misma dimensión
--
-- tramo_score : 5 bandas del score interno [200-850] — viene desde MART_T1
-- tramo_pd    : probabilidad de default en 5 bandas operativas
-- tramo_lgd   : severidad de pérdida esperada en 3 bandas
DROP TABLE IF EXISTS DIM_riesgo_credito;

SELECT
    ROW_NUMBER() OVER (ORDER BY tramo_score, tramo_pd, tramo_lgd) AS id_riesgo_credito,
    combinaciones.tramo_score,
    combinaciones.tramo_pd,
    combinaciones.tramo_lgd
INTO DIM_riesgo_credito
FROM (
    SELECT DISTINCT
        -- tramo_score ya viene calculado desde MART_T1_creditos
        tramo_score,

        -- tramo_pd — 5 bandas operativas de probabilidad de default
        CASE
            WHEN pd_probabilidad_default IS NULL               THEN 'Sin PD'
            WHEN pd_probabilidad_default <  0.05               THEN 'Muy baja (<5%)'
            WHEN pd_probabilidad_default <  0.15               THEN 'Baja (5-15%)'
            WHEN pd_probabilidad_default <  0.30               THEN 'Media (15-30%)'
            WHEN pd_probabilidad_default <  0.50               THEN 'Alta (30-50%)'
            ELSE                                                     'Muy alta (>=50%)'
        END AS tramo_pd,

        -- tramo_lgd — 3 bandas de severidad de pérdida en caso de default
        CASE
            WHEN lgd_loss_given_default IS NULL                THEN 'Sin LGD'
            WHEN lgd_loss_given_default <  0.35                THEN 'Baja (<35%)'
            WHEN lgd_loss_given_default <  0.65                THEN 'Media (35-65%)'
            ELSE                                                     'Alta (>=65%)'
        END AS tramo_lgd

    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 9: COMPORTAMIENTO CREDITICIO ─────────────────────────────────────────
-- Captura el historial de refinanciaciones y el estado de castigo
-- Permite identificar el perfil conductual del crédito sin cruzar
-- múltiples flags en cada visual de Power BI
--
-- perfil_refinanciacion : 4 niveles — sin refinanciación, una vez, reincidente, problemático
-- flag_castigo_activo   : etiqueta legible de en_castigo (0/1)
DROP TABLE IF EXISTS DIM_comportamiento;

SELECT
    ROW_NUMBER() OVER (ORDER BY perfil_refinanciacion, flag_castigo_activo) AS id_comportamiento,
    combinaciones.perfil_refinanciacion,
    combinaciones.flag_castigo_activo
INTO DIM_comportamiento
FROM (
    SELECT DISTINCT
        -- perfil_refinanciacion — 4 niveles de reincidencia
        CASE
            WHEN es_refinanciado = 0                             THEN 'Sin refinanciación'
            WHEN numero_refinanciaciones = 1                     THEN 'Una vez'
            WHEN numero_refinanciaciones BETWEEN 2 AND 3         THEN 'Reincidente (2-3)'
            ELSE                                                       'Problemático (>3)'
        END AS perfil_refinanciacion,

        -- flag_castigo_activo — etiqueta legible para slicers en Power BI
        CASE
            WHEN en_castigo = 1                                  THEN 'En castigo'
            ELSE                                                       'Activo'
        END AS flag_castigo_activo

    FROM MART_T1_creditos
) AS combinaciones;

-- ── DIM 10: EXPOSICIÓN ESTRUCTURAL ───────────────────────────────────────────
-- Tramos sobre las tres métricas que definen la estructura del crédito:
-- plazo (horizonte temporal), LTV (cobertura de garantía), spread (rentabilidad)
-- Permite segmentar la cartera por perfil estructural sin cálculos adicionales en DAX
--
-- tramo_plazo  : horizonte temporal del crédito (4 tramos)
-- tramo_ltv    : cobertura de la garantía vs. saldo (4 bandas, umbral crítico = 100%)
-- tramo_spread : rentabilidad ajustada por riesgo (4 bandas)
DROP TABLE IF EXISTS DIM_exposicion;

SELECT
    ROW_NUMBER() OVER (ORDER BY tramo_plazo, tramo_ltv, tramo_spread) AS id_exposicion,
    combinaciones.tramo_plazo,
    combinaciones.tramo_ltv,
    combinaciones.tramo_spread
INTO DIM_exposicion
FROM (
    SELECT DISTINCT
        -- tramo_plazo — horizonte temporal en meses
        CASE
            WHEN plazo_meses IS NULL         THEN 'Sin dato'
            WHEN plazo_meses <=  12          THEN 'Corto (<=12m)'
            WHEN plazo_meses <=  36          THEN 'Mediano (13-36m)'
            WHEN plazo_meses <=  60          THEN 'Largo (37-60m)'
            ELSE                                   'Muy largo (>60m)'
        END AS tramo_plazo,

        -- tramo_ltv — cobertura garantía vs saldo
        -- LTV > 100% indica que la garantía ya no cubre la deuda expuesta
        CASE
            WHEN ltv_loan_to_value IS NULL   THEN 'Sin dato'
            WHEN ltv_loan_to_value <   50    THEN 'Bajo (<50%)'
            WHEN ltv_loan_to_value <   80    THEN 'Moderado (50-80%)'
            WHEN ltv_loan_to_value <= 100    THEN 'Alto (80-100%)'
            ELSE                                   'Crítico (>100%)'
        END AS tramo_ltv,

        -- tramo_spread — rentabilidad ajustada por riesgo
        -- Spread negativo puede ser error de datos o política de crédito especial
        CASE
            WHEN spread_pct IS NULL          THEN 'Sin dato'
            WHEN spread_pct <    0           THEN 'Negativo (<0%)'
            WHEN spread_pct <    2           THEN 'Bajo (0-2%)'
            WHEN spread_pct <    5           THEN 'Normal (2-5%)'
            ELSE                                   'Alto (>=5%)'
        END AS tramo_spread

    FROM MART_T1_creditos
) AS combinaciones;

/* ============================================================================================================================
   MODELO 1 — TABLA DE HECHOS  [actualizada v1.2]
   Añadidas tres llaves foráneas: id_riesgo_credito, id_comportamiento, id_exposicion
   Los campos numéricos brutos y flags binarios se conservan en FCT para cálculos DAX
   JOIN de las nuevas DIM replica el CASE WHEN de cada dimensión para garantizar match
============================================================================================================================ */

DROP TABLE IF EXISTS FCT_creditos;

SELECT
    -- Llave primaria del hecho
    t.id_snapshot,

    -- Llave foránea a MART_T3_clientes (satélite)
    t.id_cliente,

    -- Llaves foráneas a dimensiones existentes
    g.id_geografia,
    s.id_segmento,
    se.id_sector,
    p.id_producto,
    c.id_calificacion,
    ga.id_garantia,
    o.id_oficial,

    -- Llaves foráneas a dimensiones nuevas v1.2
    r.id_riesgo_credito,
    co.id_comportamiento,
    ex.id_exposicion,

    -- Fechas — llaves de conexión con DIM_calendario en Power BI
    t.fecha_desembolso,    -- relaciona con DIM_calendario.Date (análisis temporal)
    t.cosecha_mes,         -- relaciona con DIM_calendario.cosecha_mes (puente hacia T4)
    t.fecha_vencimiento,
    t.plazo_meses,
	t.fecha_corte,

    -- Métricas de cartera
    t.saldo_capital,
    t.saldo_mora,
    t.saldo_total_exposicion,

    -- Tasas
    t.tasa_nominal_anual_pct,
    t.tasa_efectiva_anual_pct,
    t.spread_pct,

    -- Mora
    t.dias_atraso,
    t.numero_cuotas_vencidas,

    -- Provisiones
    t.provision_requerida,
    t.provision_constituida,
    t.deficit_superavit_provision,
    t.flag_deficit_provision,

    -- Garantías
    t.valor_garantia,
    t.ltv_loan_to_value,
    t.cobertura_garantia_ratio,

    -- Modelos internos Basilea (métricas brutas conservadas para cálculos DAX)
    t.score_crediticio,
    t.flag_score,
    t.pd_probabilidad_default,
    t.lgd_loss_given_default,
    t.el_expected_loss,

    -- Comportamiento (flags binarios conservados para filtros DAX rápidos)
    t.es_refinanciado,
    t.numero_refinanciaciones,

    -- Castigos
    t.en_castigo,
    t.monto_castigado,

    -- Trazabilidad
    t.flag_error

INTO FCT_creditos
FROM MART_T1_creditos AS t

-- Dimensiones existentes
LEFT JOIN DIM_geografia      AS g   ON  t.zona_geografica      = g.zona_geografica
LEFT JOIN DIM_segmento       AS s   ON  t.tipo_persona          = s.tipo_persona
                                    AND t.segmento              = s.segmento
LEFT JOIN DIM_sector         AS se  ON  t.sector_economico      = se.sector_economico
LEFT JOIN DIM_producto       AS p   ON  t.producto_crediticio   = p.producto_crediticio
                                    AND t.moneda                = p.moneda
LEFT JOIN DIM_calificacion   AS c   ON  t.calificacion_sbs      = c.calificacion_sbs
                                    AND t.tramo_mora             = c.tramo_mora
LEFT JOIN DIM_garantia       AS ga  ON  t.tipo_garantia         = ga.tipo_garantia
LEFT JOIN DIM_oficial        AS o   ON  t.oficial_credito       = o.oficial_credito

-- DIM_riesgo_credito — JOIN replica los CASE WHEN de la DIM
LEFT JOIN DIM_riesgo_credito AS r   ON  t.tramo_score           = r.tramo_score
                                    AND CASE
                                            WHEN t.pd_probabilidad_default IS NULL      THEN 'Sin PD'
                                            WHEN t.pd_probabilidad_default <  0.05      THEN 'Muy baja (<5%)'
                                            WHEN t.pd_probabilidad_default <  0.15      THEN 'Baja (5-15%)'
                                            WHEN t.pd_probabilidad_default <  0.30      THEN 'Media (15-30%)'
                                            WHEN t.pd_probabilidad_default <  0.50      THEN 'Alta (30-50%)'
                                            ELSE                                              'Muy alta (>=50%)'
                                        END                     = r.tramo_pd
                                    AND CASE
                                            WHEN t.lgd_loss_given_default IS NULL       THEN 'Sin LGD'
                                            WHEN t.lgd_loss_given_default <  0.35       THEN 'Baja (<35%)'
                                            WHEN t.lgd_loss_given_default <  0.65       THEN 'Media (35-65%)'
                                            ELSE                                              'Alta (>=65%)'
                                        END                     = r.tramo_lgd

-- DIM_comportamiento — JOIN replica los CASE WHEN de la DIM
LEFT JOIN DIM_comportamiento AS co  ON  CASE
                                            WHEN t.es_refinanciado = 0                  THEN 'Sin refinanciación'
                                            WHEN t.numero_refinanciaciones = 1          THEN 'Una vez'
                                            WHEN t.numero_refinanciaciones BETWEEN 2 AND 3 THEN 'Reincidente (2-3)'
                                            ELSE                                             'Problemático (>3)'
                                        END                     = co.perfil_refinanciacion
                                    AND CASE
                                            WHEN t.en_castigo = 1                       THEN 'En castigo'
                                            ELSE                                              'Activo'
                                        END                     = co.flag_castigo_activo

-- DIM_exposicion — JOIN replica los CASE WHEN de la DIM
LEFT JOIN DIM_exposicion     AS ex  ON  CASE
                                            WHEN t.plazo_meses IS NULL                  THEN 'Sin dato'
                                            WHEN t.plazo_meses <=  12                   THEN 'Corto (<=12m)'
                                            WHEN t.plazo_meses <=  36                   THEN 'Mediano (13-36m)'
                                            WHEN t.plazo_meses <=  60                   THEN 'Largo (37-60m)'
                                            ELSE                                              'Muy largo (>60m)'
                                        END                     = ex.tramo_plazo
                                    AND CASE
                                            WHEN t.ltv_loan_to_value IS NULL            THEN 'Sin dato'
                                            WHEN t.ltv_loan_to_value <   50             THEN 'Bajo (<50%)'
                                            WHEN t.ltv_loan_to_value <   80             THEN 'Moderado (50-80%)'
                                            WHEN t.ltv_loan_to_value <= 100             THEN 'Alto (80-100%)'
                                            ELSE                                              'Crítico (>100%)'
                                        END                     = ex.tramo_ltv
                                    AND CASE
                                            WHEN t.spread_pct IS NULL                   THEN 'Sin dato'
                                            WHEN t.spread_pct <    0                    THEN 'Negativo (<0%)'
                                            WHEN t.spread_pct <    2                    THEN 'Bajo (0-2%)'
                                            WHEN t.spread_pct <    5                    THEN 'Normal (2-5%)'
                                            ELSE                                              'Alto (>=5%)'
                                        END                     = ex.tramo_spread;

/* ============================================================================================================================
   VERIFICACIÓN FINAL
   Confirma que todas las tablas del modelo tienen los registros esperados
   y que ningún JOIN produjo valores nulos en las llaves foráneas
============================================================================================================================ */

-- Conteo general
SELECT 'DIM_geografia'       AS tabla, COUNT(*) AS registros FROM DIM_geografia       
UNION ALL SELECT 'DIM_segmento',       COUNT(*) FROM DIM_segmento                     
UNION ALL SELECT 'DIM_sector',         COUNT(*) FROM DIM_sector                       
UNION ALL SELECT 'DIM_producto',       COUNT(*) FROM DIM_producto                     
UNION ALL SELECT 'DIM_calificacion',   COUNT(*) FROM DIM_calificacion                 
UNION ALL SELECT 'DIM_garantia',       COUNT(*) FROM DIM_garantia                   
UNION ALL SELECT 'DIM_oficial',        COUNT(*) FROM DIM_oficial                     
UNION ALL SELECT 'DIM_riesgo_credito', COUNT(*) FROM DIM_riesgo_credito               
UNION ALL SELECT 'DIM_comportamiento', COUNT(*) FROM DIM_comportamiento    
UNION ALL SELECT 'DIM_exposicion',     COUNT(*) FROM DIM_exposicion                 
UNION ALL SELECT 'FCT_creditos',       COUNT(*) FROM FCT_creditos;                    

-- Verificar que ningún JOIN quedó sin match (todos deben ser 0)
SELECT
    SUM(CASE WHEN id_geografia      IS NULL THEN 1 ELSE 0 END) AS sin_geografia,
    SUM(CASE WHEN id_segmento       IS NULL THEN 1 ELSE 0 END) AS sin_segmento,
    SUM(CASE WHEN id_sector         IS NULL THEN 1 ELSE 0 END) AS sin_sector,
    SUM(CASE WHEN id_producto       IS NULL THEN 1 ELSE 0 END) AS sin_producto,
    SUM(CASE WHEN id_calificacion   IS NULL THEN 1 ELSE 0 END) AS sin_calificacion,
    SUM(CASE WHEN id_garantia       IS NULL THEN 1 ELSE 0 END) AS sin_garantia,
    SUM(CASE WHEN id_oficial        IS NULL THEN 1 ELSE 0 END) AS sin_oficial,
    SUM(CASE WHEN id_riesgo_credito IS NULL THEN 1 ELSE 0 END) AS sin_riesgo_credito,
    SUM(CASE WHEN id_comportamiento IS NULL THEN 1 ELSE 0 END) AS sin_comportamiento,
    SUM(CASE WHEN id_exposicion     IS NULL THEN 1 ELSE 0 END) AS sin_exposicion
FROM FCT_creditos;

/* ============================================================================================================================
   RELACIONES A CONFIGURAR EN POWER BI
   (referencia para cuando conectes las tablas)

   MODELO 1 — Star Schema Créditos:
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │  DIM_geografia      .id_geografia      → FCT_creditos.id_geografia           │ 1:N
   │  DIM_segmento       .id_segmento       → FCT_creditos.id_segmento            │ 1:N
   │  DIM_sector         .id_sector         → FCT_creditos.id_sector              │ 1:N
   │  DIM_producto       .id_producto       → FCT_creditos.id_producto            │ 1:N
   │  DIM_calificacion   .id_calificacion   → FCT_creditos.id_calificacion        │ 1:N
   │  DIM_garantia       .id_garantia       → FCT_creditos.id_garantia            │ 1:N
   │  DIM_oficial        .id_oficial        → FCT_creditos.id_oficial             │ 1:N
   │  DIM_riesgo_credito .id_riesgo_credito → FCT_creditos.id_riesgo_credito      │ 1:N  [NUEVO]
   │  DIM_comportamiento .id_comportamiento → FCT_creditos.id_comportamiento      │ 1:N  [NUEVO]
   │  DIM_exposicion     .id_exposicion     → FCT_creditos.id_exposicion          │ 1:N  [NUEVO]
   │  DIM_calendario     .Date              → FCT_creditos.fecha_desembolso       │ 1:N
   │  DIM_calendario     .cosecha_mes       → MART_T4_cosechas.cosecha_mes        │ 1:N
   │  MART_T3_clientes   .id_cliente        → FCT_creditos.id_cliente             │ 1:N
   └──────────────────────────────────────────────────────────────────────────────┘

   MODELO 2 — Star Schema KPIs:
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │  DIM_calendario .Date → MART_T2_kpis.fecha_mes                              │ 1:N
   └──────────────────────────────────────────────────────────────────────────────┘

   DIM_calendario en Power BI (DAX):
       DIM_calendario   = CALENDAR(DATE(2024,1,1), DATE(2025,12,31))
       Año              = YEAR([Date])
       Mes              = MONTH([Date])
       Nombre mes       = FORMAT([Date], "MMMM")
       Mes abreviado    = FORMAT([Date], "MMM")
       Trimestre        = "Q" & QUARTER([Date])
       cosecha_mes      = FORMAT([Date], "yyyy-MM")   ← llave para MART_T4_cosechas
============================================================================================================================ */

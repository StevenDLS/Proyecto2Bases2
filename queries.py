import json
from datetime import date as _date, datetime as _datetime

def _valor_json(valor):
    if isinstance(valor, _datetime):
        return valor.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(valor, _date):
        return valor.strftime("%Y-%m-%d")
    return valor

def ejecutar(spark, sql, fileName):
    filas = spark.sql(sql).collect()
    dataArray = [
        {k: _valor_json(v) for k, v in r.asDict(recursive=True).items()}
        for r in filas
    ]
    with open("processed_jsons/" + fileName, 'w') as file:
        formattedJSON = json.dumps(dataArray, indent = 4)
        file.write(formattedJSON)

# ============================================================
# 1. MAPA DE CALOR DE INTENSIDAD DE CONFLICTOS POR PAÍS POR DÍA
# ============================================================

def mapa_calor_intensidad_conflictos(spark):
    sql = """
        SELECT
            date_format(to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha,
            ActionGeo_CountryCode AS pais,
            region_from_geo(
                ActionGeo_CountryCode,
                AVG(ActionGeo_Lat),
                AVG(ActionGeo_Long)
            ) AS region,
            ROUND(AVG(ActionGeo_Lat), 4) AS latitud,
            ROUND(AVG(ActionGeo_Long), 4) AS longitud,
            COUNT(DISTINCT GLOBALEVENTID) AS total_eventos_conflicto,
            ROUND(AVG(ABS(CAST(GoldsteinScale AS DOUBLE))), 2) AS intensidad_promedio_goldstein,
            ROUND(SUM(ABS(CAST(GoldsteinScale AS DOUBLE))), 2) AS intensidad_total_goldstein
        FROM events
        WHERE ActionGeo_CountryCode IS NOT NULL
          AND SQLDATE IS NOT NULL
          AND GoldsteinScale IS NOT NULL
          AND CAST(QuadClass AS INT) IN (3, 4)
        GROUP BY
            date_format(to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd'),
            ActionGeo_CountryCode
        ORDER BY fecha DESC, intensidad_total_goldstein DESC
    """
    return ejecutar(spark, sql, "mapa_calor_intensidad_conflictos.json")


# ============================================================
# 2. TOP 10 PAÍSES QUE GENERAN MÁS EVENTOS NOTICIOSOS POR DÍA
# ============================================================

def top_10_paises_eventos_por_dia(spark):
    sql = """
        WITH diarios AS (
            SELECT
                date_format(to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha,
                ActionGeo_CountryCode AS pais,
                COUNT(DISTINCT GLOBALEVENTID) AS total_eventos,
                SUM(CAST(NumMentions AS INT)) AS total_menciones,
                SUM(CAST(NumArticles AS INT)) AS total_articulos
            FROM events
            WHERE ActionGeo_CountryCode IS NOT NULL
              AND SQLDATE IS NOT NULL
            GROUP BY
                to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'),
                ActionGeo_CountryCode
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY fecha
                    ORDER BY total_eventos DESC
                ) AS ranking
            FROM diarios
        )
        SELECT *
        FROM ranked
        WHERE ranking <= 10
        ORDER BY fecha DESC, ranking
    """
    return ejecutar(spark, sql, "top_10_paises_eventos_por_dia.json")


# ============================================================
# 3. CORRELACIÓN ENTRE AVGTONE Y NÚMERO DE FUENTES
# ============================================================

def correlacion_avg_tone_fuentes(spark):
    sql = """
        SELECT
            COUNT(*) AS eventos_analizados,
            ROUND(CORR(CAST(AvgTone AS DOUBLE), CAST(NumSources AS DOUBLE)), 4)
                AS correlacion_avgTone_numSources,
            ROUND(AVG(CAST(AvgTone AS DOUBLE)), 2) AS promedio_tono,
            ROUND(AVG(CAST(NumSources AS DOUBLE)), 2) AS promedio_fuentes
        FROM events
        WHERE AvgTone IS NOT NULL
          AND NumSources IS NOT NULL
    """
    return ejecutar(spark, sql, "correlacion_avg_tone_fuentes.json")


# ============================================================
# 4. DISTRIBUCIÓN DE TIPOS DE EVENTOS CAMEO POR REGIÓN DEL MUNDO
# ============================================================

def distribucion_cameo_por_region(spark):
    sql = """
        WITH base AS (
            SELECT
                region_from_geo(
                    ActionGeo_CountryCode,
                    ActionGeo_Lat,
                    ActionGeo_Long
                ) AS region,
                EventRootCode,
                EventBaseCode,
                EventCode,
                COALESCE(CAMEOCodeDescription, 'Sin descripción') AS descripcion_cameo,
                GLOBALEVENTID
            FROM events
            WHERE EventCode IS NOT NULL
              AND ActionGeo_CountryCode IS NOT NULL
        ),
        conteo AS (
            SELECT
                COALESCE(region, 'SIN_REGION') AS region,
                COALESCE(EventRootCode, 'SIN_ROOT_CODE') AS EventRootCode,
                COALESCE(EventBaseCode, 'SIN_BASE_CODE') AS EventBaseCode,
                COALESCE(EventCode, 'SIN_EVENT_CODE') AS EventCode,
                descripcion_cameo,
                COUNT(DISTINCT GLOBALEVENTID) AS total_eventos
            FROM base
            GROUP BY
                COALESCE(region, 'SIN_REGION'),
                COALESCE(EventRootCode, 'SIN_ROOT_CODE'),
                COALESCE(EventBaseCode, 'SIN_BASE_CODE'),
                COALESCE(EventCode, 'SIN_EVENT_CODE'),
                descripcion_cameo
        ),
        calculado AS (
            SELECT
                region,
                EventRootCode,
                EventBaseCode,
                EventCode,
                descripcion_cameo,
                total_eventos,
                ROUND(
                    100.0 * total_eventos / SUM(total_eventos) OVER (PARTITION BY region),
                    2
                ) AS porcentaje_region
            FROM conteo
        ),
        ordenado AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    ORDER BY region ASC, total_eventos DESC
                ) AS orden_salida
            FROM calculado
        )
        SELECT
            CAST(region AS STRING) AS region,
            CAST(EventRootCode AS STRING) AS EventRootCode,
            CAST(EventBaseCode AS STRING) AS EventBaseCode,
            CAST(EventCode AS STRING) AS EventCode,
            CAST(descripcion_cameo AS STRING) AS descripcion_cameo,
            CAST(total_eventos AS STRING) AS total_eventos,
            CAST(porcentaje_region AS STRING) AS porcentaje_region
        FROM ordenado
        ORDER BY orden_salida
    """
    return ejecutar(spark, sql, "distribucion_cameo_por_region.json")


# ============================================================
# 5. MATRIZ DE INTERACCIÓN ENTRE TIPOS DE ACTORES
# ============================================================

def matriz_interaccion_actores(spark):
    sql = """
        WITH base AS (
            SELECT
                GLOBALEVENTID,
                concat_ws(',', Actor1Type1Code, Actor1Type2Code, Actor1Type3Code) AS actor1_types,
                concat_ws(',', Actor2Type1Code, Actor2Type2Code, Actor2Type3Code) AS actor2_types,
                CAST(QuadClass AS INT) AS QuadClass,
                CAST(GoldsteinScale AS DOUBLE) AS GoldsteinScale
            FROM events
        ),
        clasificado AS (
            SELECT
                GLOBALEVENTID,
                CASE
                    WHEN actor1_types RLIKE '(^|,)(GOV|LEG|JUD|COP|SPY)(,|$)' THEN 'Gobierno'
                    WHEN actor1_types RLIKE '(^|,)(MIL)(,|$)' THEN 'Militar'
                    WHEN actor1_types RLIKE '(^|,)(REB|INS|SEP)(,|$)' THEN 'Grupo rebelde'
                    ELSE NULL
                END AS actor1_categoria,
                CASE
                    WHEN actor2_types RLIKE '(^|,)(GOV|LEG|JUD|COP|SPY)(,|$)' THEN 'Gobierno'
                    WHEN actor2_types RLIKE '(^|,)(MIL)(,|$)' THEN 'Militar'
                    WHEN actor2_types RLIKE '(^|,)(REB|INS|SEP)(,|$)' THEN 'Grupo rebelde'
                    ELSE NULL
                END AS actor2_categoria,
                QuadClass,
                GoldsteinScale
            FROM base
        )
        SELECT
            actor1_categoria,
            actor2_categoria,
            COUNT(DISTINCT GLOBALEVENTID) AS frecuencia,
            SUM(CASE WHEN QuadClass IN (3, 4) THEN 1 ELSE 0 END) AS eventos_conflicto,
            ROUND(AVG(GoldsteinScale), 2) AS goldstein_promedio
        FROM clasificado
        WHERE actor1_categoria IS NOT NULL
          AND actor2_categoria IS NOT NULL
        GROUP BY actor1_categoria, actor2_categoria
        ORDER BY frecuencia DESC
    """
    return ejecutar(spark, sql, "matriz_interaccion_actores.json")


# ============================================================
# 6. PAÍSES CON MAYOR COBERTURA MEDIÁTICA POR EVENTO
# ============================================================

def paises_mayor_cobertura_mediatica(spark):
    sql = """
        WITH eventos AS (
            SELECT DISTINCT
                GLOBALEVENTID,
                ActionGeo_CountryCode AS pais
            FROM events
            WHERE GLOBALEVENTID IS NOT NULL
              AND ActionGeo_CountryCode IS NOT NULL
        ),
        menciones_evento AS (
            SELECT
                GLOBALEVENTID,
                COUNT(*) AS menciones,
                COUNT(DISTINCT MentionSourceName) AS fuentes_distintas
            FROM mentions
            WHERE GLOBALEVENTID IS NOT NULL
            GROUP BY GLOBALEVENTID
        )
        SELECT
            e.pais,
            COUNT(DISTINCT e.GLOBALEVENTID) AS total_eventos,
            SUM(COALESCE(m.menciones, 0)) AS total_menciones,
            SUM(COALESCE(m.fuentes_distintas, 0)) AS total_fuentes_distintas_evento,
            ROUND(
                SUM(COALESCE(m.menciones, 0)) / COUNT(DISTINCT e.GLOBALEVENTID),
                2
            ) AS razon_menciones_por_evento
        FROM eventos e
        LEFT JOIN menciones_evento m
            ON e.GLOBALEVENTID = m.GLOBALEVENTID
        GROUP BY e.pais
        HAVING total_eventos >= 5
        ORDER BY razon_menciones_por_evento DESC
    """
    return ejecutar(spark, sql, "paises_mayor_cobertura_mediatica.json")


# ============================================================
# 7. TENDENCIA DE SENTIMIENTO POR PAÍS
# ============================================================

def tendencia_sentimiento_pais(spark):
    sql = """
        WITH diarios AS (
            SELECT
                date_format(to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha,
                ActionGeo_CountryCode AS pais,
                COUNT(DISTINCT GLOBALEVENTID) AS total_eventos,
                ROUND(AVG(CAST(AvgTone AS DOUBLE)), 2) AS tono_promedio_dia
            FROM events
            WHERE SQLDATE IS NOT NULL
              AND ActionGeo_CountryCode IS NOT NULL
              AND AvgTone IS NOT NULL
            GROUP BY
                to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'),
                ActionGeo_CountryCode
        )
        SELECT
            fecha,
            pais,
            total_eventos,
            tono_promedio_dia,
            ROUND(
                AVG(tono_promedio_dia) OVER (
                    PARTITION BY pais
                    ORDER BY fecha
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ),
                2
            ) AS promedio_movil_7_dias
        FROM diarios
        ORDER BY pais, fecha
    """
    return ejecutar(spark, sql, "tendencia_sentimiento_pais.json")


# ============================================================
# 8. CONFLICTOS ENTRE ACTORES: PARES DE PAÍSES MÁS FRECUENTES
# ============================================================

def conflictos_pares_paises(spark):
    sql = """
        WITH base AS (
            SELECT
                LEAST(Actor1CountryCode, Actor2CountryCode) AS pais_a,
                GREATEST(Actor1CountryCode, Actor2CountryCode) AS pais_b,
                GLOBALEVENTID,
                CAST(GoldsteinScale AS DOUBLE) AS GoldsteinScale,
                CAST(AvgTone AS DOUBLE) AS AvgTone
            FROM events
            WHERE Actor1CountryCode IS NOT NULL
              AND Actor2CountryCode IS NOT NULL
              AND TRIM(Actor1CountryCode) <> ''
              AND TRIM(Actor2CountryCode) <> ''
              AND Actor1CountryCode <> Actor2CountryCode
              AND CAST(QuadClass AS INT) IN (3, 4)
        )
        SELECT
            pais_a,
            pais_b,
            COUNT(DISTINCT GLOBALEVENTID) AS total_conflictos,
            ROUND(AVG(GoldsteinScale), 2) AS goldstein_promedio,
            ROUND(AVG(AvgTone), 2) AS tono_promedio
        FROM base
        GROUP BY pais_a, pais_b
        ORDER BY total_conflictos DESC
        LIMIT 50
    """
    return ejecutar(spark, sql, "conflictos_pares_paises.json")


# ============================================================ 
# 9. DETECCIÓN DE ESCALADA DE EVENTOS EN 24 HORAS
# ============================================================

def escalada_eventos_menciones_24h(spark, min_menciones_24h=1):
    sql = f"""
        WITH mention_times AS (
            SELECT
                GLOBALEVENTID,
                to_timestamp(
                    lpad(CAST(CAST(MentionTimeDate AS DECIMAL(14,0)) AS STRING), 14, '0'),
                    'yyyyMMddHHmmss'
                ) AS mention_ts
            FROM mentions
            WHERE GLOBALEVENTID IS NOT NULL
              AND MentionTimeDate IS NOT NULL
        ),
        mention_times_valid AS (
            SELECT *
            FROM mention_times
            WHERE mention_ts IS NOT NULL
        ),
        hourly AS (
            SELECT
                GLOBALEVENTID,
                date_trunc('hour', mention_ts) AS hora,
                unix_timestamp(date_trunc('hour', mention_ts)) AS hora_unix,
                COUNT(*) AS menciones_hora
            FROM mention_times_valid
            GROUP BY
                GLOBALEVENTID,
                date_trunc('hour', mention_ts)
        ),
        rolling AS (
            SELECT
                GLOBALEVENTID,
                hora,
                hora_unix,
                menciones_hora,
                SUM(menciones_hora) OVER (
                    PARTITION BY GLOBALEVENTID
                    ORDER BY hora_unix
                    RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
                ) AS menciones_ultimas_24h
            FROM hourly
        ),
        aceleracion AS (
            SELECT
                *,
                LAG(menciones_ultimas_24h) OVER (
                    PARTITION BY GLOBALEVENTID
                    ORDER BY hora_unix
                ) AS menciones_24h_previas
            FROM rolling
        ),
        events_dedup AS (
            SELECT *
            FROM (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY GLOBALEVENTID
                        ORDER BY SQLDATE DESC
                    ) AS rn
                FROM events e
                WHERE GLOBALEVENTID IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            a.GLOBALEVENTID,
            date_format(to_date(CAST(e.SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha_evento,
            e.ActionGeo_CountryCode AS pais,
            e.EventCode,
            e.CAMEOCodeDescription,
            date_format(a.hora, 'yyyy-MM-dd HH:mm:ss') AS hora,
            a.menciones_hora,
            a.menciones_ultimas_24h,
            COALESCE(a.menciones_24h_previas, 0) AS menciones_24h_previas,
            a.menciones_ultimas_24h - COALESCE(a.menciones_24h_previas, 0)
                AS aceleracion_menciones,
            e.SOURCEURL
        FROM aceleracion a
        LEFT JOIN events_dedup e
            ON a.GLOBALEVENTID = e.GLOBALEVENTID
        WHERE a.menciones_ultimas_24h >= {min_menciones_24h}
          AND a.menciones_ultimas_24h - COALESCE(a.menciones_24h_previas, 0) > 0
        ORDER BY aceleracion_menciones DESC
        LIMIT 50
    """
    return ejecutar(spark, sql, "escalada_eventos_menciones_24h.json")


# ============================================================1 row only
# 10. AGRUPAMIENTO DE CONFLICTOS BASADOS EN RELIGIÓN POR REGIÓN
# ============================================================

def conflictos_religion_region(spark):
    sql = """
        WITH base AS (
            SELECT
                region_from_geo(
                    ActionGeo_CountryCode,
                    ActionGeo_Lat,
                    ActionGeo_Long
                ) AS region,
                COALESCE(Actor1Religion1Code, Actor1Religion2Code, 'SIN_RELIGION') AS religion_actor1,
                COALESCE(Actor2Religion1Code, Actor2Religion2Code, 'SIN_RELIGION') AS religion_actor2,
                GLOBALEVENTID,
                CAST(GoldsteinScale AS DOUBLE) AS GoldsteinScale,
                CAST(AvgTone AS DOUBLE) AS AvgTone
            FROM events
            WHERE CAST(QuadClass AS INT) IN (3, 4)
              AND (
                    Actor1Religion1Code IS NOT NULL
                 OR Actor1Religion2Code IS NOT NULL
                 OR Actor2Religion1Code IS NOT NULL
                 OR Actor2Religion2Code IS NOT NULL
              )
        )
        SELECT
            region,
            religion_actor1,
            religion_actor2,
            COUNT(DISTINCT GLOBALEVENTID) AS total_conflictos,
            ROUND(AVG(GoldsteinScale), 2) AS goldstein_promedio,
            ROUND(AVG(AvgTone), 2) AS tono_promedio
        FROM base
        GROUP BY region, religion_actor1, religion_actor2
        ORDER BY total_conflictos DESC
    """
    return ejecutar(spark, sql, "conflictos_religion_region.json")


# ============================================================
# 11. PRINCIPALES TEMAS EXTRAÍDOS POR GKG POR CONTINENTE POR AÑO
# ============================================================

def temas_gkg_continente_anio(spark):
    sql = """
        WITH locs_v2_raw AS (
            SELECT
                GKGRECORDID,
                CAST(SUBSTR(CAST(`DATE` AS STRING), 1, 4) AS INT) AS anio,
                element_at(split(loc, '#'), 3) AS pais,
                TRIM(element_at(split(loc, '#'), 6)) AS lat_raw,
                TRIM(element_at(split(loc, '#'), 7)) AS lon_raw
            FROM gkg
            LATERAL VIEW explode(split(V2Locations, ';')) lv AS loc
            WHERE V2Locations IS NOT NULL
              AND V2Locations <> ''
        ),
        locs_v2 AS (
            SELECT
                GKGRECORDID,
                anio,
                pais,
                CASE
                    WHEN lat_raw IS NULL OR lower(lat_raw) = 'null' OR lat_raw = '' THEN NULL
                    WHEN lat_raw RLIKE '^-?[0-9]+(\\\\.[0-9]+)?$' THEN CAST(lat_raw AS DOUBLE)
                    ELSE NULL
                END AS lat,
                CASE
                    WHEN lon_raw IS NULL OR lower(lon_raw) = 'null' OR lon_raw = '' THEN NULL
                    WHEN lon_raw RLIKE '^-?[0-9]+(\\\\.[0-9]+)?$' THEN CAST(lon_raw AS DOUBLE)
                    ELSE NULL
                END AS lon
            FROM locs_v2_raw
        ),

        locs_old_raw AS (
            SELECT
                GKGRECORDID,
                CAST(SUBSTR(CAST(`DATE` AS STRING), 1, 4) AS INT) AS anio,
                element_at(split(loc, '#'), 3) AS pais,
                TRIM(element_at(split(loc, '#'), 5)) AS lat_raw,
                TRIM(element_at(split(loc, '#'), 6)) AS lon_raw
            FROM gkg
            LATERAL VIEW explode(split(Locations, ';')) lv AS loc
            WHERE (V2Locations IS NULL OR V2Locations = '')
              AND Locations IS NOT NULL
              AND Locations <> ''
        ),
        locs_old AS (
            SELECT
                GKGRECORDID,
                anio,
                pais,
                CASE
                    WHEN lat_raw IS NULL OR lower(lat_raw) = 'null' OR lat_raw = '' THEN NULL
                    WHEN lat_raw RLIKE '^-?[0-9]+(\\\\.[0-9]+)?$' THEN CAST(lat_raw AS DOUBLE)
                    ELSE NULL
                END AS lat,
                CASE
                    WHEN lon_raw IS NULL OR lower(lon_raw) = 'null' OR lon_raw = '' THEN NULL
                    WHEN lon_raw RLIKE '^-?[0-9]+(\\\\.[0-9]+)?$' THEN CAST(lon_raw AS DOUBLE)
                    ELSE NULL
                END AS lon
            FROM locs_old_raw
        ),

        locs AS (
            SELECT * FROM locs_v2
            UNION ALL
            SELECT * FROM locs_old
        ),

        temas AS (
            SELECT
                GKGRECORDID,
                REGEXP_EXTRACT(TRIM(theme_raw), '^([^,]+)', 1) AS tema
            FROM gkg
            LATERAL VIEW explode(split(COALESCE(V2Themes, Themes), ';')) tv AS theme_raw
            WHERE COALESCE(V2Themes, Themes) IS NOT NULL
              AND TRIM(theme_raw) <> ''
        ),

        base AS (
            SELECT DISTINCT
                region_from_geo(l.pais, l.lat, l.lon) AS continente,
                l.anio,
                t.tema,
                l.GKGRECORDID
            FROM locs l
            JOIN temas t
                ON l.GKGRECORDID = t.GKGRECORDID
            WHERE t.tema IS NOT NULL
              AND t.tema <> ''
        ),

        conteo AS (
            SELECT
                continente,
                anio,
                tema,
                COUNT(DISTINCT GKGRECORDID) AS total_documentos
            FROM base
            WHERE continente IS NOT NULL
              AND continente <> ''
            GROUP BY continente, anio, tema
        ),

        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY continente, anio
                    ORDER BY total_documentos DESC
                ) AS ranking
            FROM conteo
        )

        SELECT *
        FROM ranked
        WHERE ranking <= 20
        ORDER BY anio DESC, continente, ranking
    """
    return ejecutar(spark, sql, "temas_gkg_continente_anio.json")


# ============================================================
# 12. ORGANIZACIONES MÁS MENCIONADAS GLOBALMENTE POR DÍA
# ============================================================

def organizaciones_mas_mencionadas_por_dia(spark):
    sql = """
        WITH orgs AS (
            SELECT
                date_format(to_date(SUBSTR(CAST(`DATE` AS STRING), 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha,
                LOWER(TRIM(org_raw)) AS organizacion,
                GKGRECORDID
            FROM gkg
            LATERAL VIEW explode(split(Organizations, ';')) ov AS org_raw
            WHERE Organizations IS NOT NULL
              AND TRIM(org_raw) <> ''
        ),
        conteo AS (
            SELECT
                fecha,
                organizacion,
                COUNT(DISTINCT GKGRECORDID) AS total_documentos
            FROM orgs
            GROUP BY fecha, organizacion
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY fecha
                    ORDER BY total_documentos DESC
                ) AS ranking
            FROM conteo
        )
        SELECT *
        FROM ranked
        WHERE ranking <= 20
        ORDER BY fecha DESC, ranking
    """
    return ejecutar(spark, sql, "organizaciones_mas_mencionadas_por_dia.json")


# ============================================================
# 13. ANÁLISIS DE REZAGO:
# ¿EL TONO DE HOY PREDICE CONFLICTOS DE MAÑANA?
# ============================================================

def analisis_rezago_tono_conflicto(spark):
    sql = """
        WITH diario AS (
            SELECT
                date_format(to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha,
                ActionGeo_CountryCode AS pais,
                ROUND(AVG(CAST(AvgTone AS DOUBLE)), 4) AS tono_promedio_hoy,
                SUM(
                    CASE WHEN CAST(QuadClass AS INT) IN (3, 4)
                    THEN 1 ELSE 0 END
                ) AS conflictos_hoy
            FROM events
            WHERE SQLDATE IS NOT NULL
              AND ActionGeo_CountryCode IS NOT NULL
              AND AvgTone IS NOT NULL
            GROUP BY
                to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd'),
                ActionGeo_CountryCode
        ),
        rezago AS (
            SELECT
                fecha,
                pais,
                tono_promedio_hoy,
                conflictos_hoy,
                LEAD(conflictos_hoy) OVER (
                    PARTITION BY pais
                    ORDER BY fecha
                ) AS conflictos_manana
            FROM diario
        )
        SELECT
            pais,
            COUNT(*) AS dias_analizados,
            ROUND(CORR(tono_promedio_hoy, conflictos_manana), 4)
                AS correlacion_tono_hoy_conflicto_manana,
            ROUND(AVG(tono_promedio_hoy), 2) AS tono_promedio,
            ROUND(AVG(conflictos_manana), 2) AS conflictos_promedio_manana
        FROM rezago
        WHERE conflictos_manana IS NOT NULL
        GROUP BY pais
        HAVING dias_analizados >= 3
        ORDER BY ABS(correlacion_tono_hoy_conflicto_manana) DESC
    """
    return ejecutar(spark, sql, "analisis_rezago_tono_conflicto.json")


# ============================================================
# 14. GRAFO DE INTERACCIONES DIPLOMÁTICAS VS CONFLICTOS ENTRE PAÍSES
# ============================================================

def grafo_diplomacia_vs_conflicto(spark):
    sql = """
        WITH pares AS (
            SELECT
                LEAST(Actor1CountryCode, Actor2CountryCode) AS pais_a,
                GREATEST(Actor1CountryCode, Actor2CountryCode) AS pais_b,
                CAST(QuadClass AS INT) AS QuadClass,
                GLOBALEVENTID
            FROM events
            WHERE Actor1CountryCode IS NOT NULL
              AND Actor2CountryCode IS NOT NULL
              AND TRIM(Actor1CountryCode) <> ''
              AND TRIM(Actor2CountryCode) <> ''
              AND Actor1CountryCode <> Actor2CountryCode
        ),
        conteo AS (
            SELECT
                pais_a,
                pais_b,
                SUM(CASE WHEN QuadClass IN (1, 2) THEN 1 ELSE 0 END)
                    AS interacciones_diplomaticas,
                SUM(CASE WHEN QuadClass IN (3, 4) THEN 1 ELSE 0 END)
                    AS interacciones_conflicto,
                COUNT(DISTINCT GLOBALEVENTID) AS total_interacciones
            FROM pares
            GROUP BY pais_a, pais_b
        )
        SELECT
            pais_a,
            pais_b,
            interacciones_diplomaticas,
            interacciones_conflicto,
            total_interacciones,
            ROUND(
                interacciones_conflicto / total_interacciones,
                4
            ) AS proporcion_conflicto
        FROM conteo
        WHERE total_interacciones > 0
        ORDER BY total_interacciones DESC
        LIMIT 100
    """
    return ejecutar(spark, sql, "grafo_diplomacia_vs_conflicto.json")


# ============================================================
# 15. ÍNDICE DE DIVERSIDAD DE FUENTES POR PAÍS
# ============================================================

def indice_diversidad_fuentes_pais(spark):
    sql = """
        WITH joined AS (
            SELECT
                e.ActionGeo_CountryCode AS pais,
                m.MentionSourceName AS fuente
            FROM events e
            JOIN mentions m
                ON e.GLOBALEVENTID = m.GLOBALEVENTID
            WHERE e.ActionGeo_CountryCode IS NOT NULL
              AND m.MentionSourceName IS NOT NULL
              AND TRIM(m.MentionSourceName) <> ''
        ),
        source_counts AS (
            SELECT
                pais,
                fuente,
                COUNT(*) AS n
            FROM joined
            GROUP BY pais, fuente
        ),
        totals AS (
            SELECT
                pais,
                SUM(n) AS total_menciones,
                COUNT(DISTINCT fuente) AS medios_distintos
            FROM source_counts
            GROUP BY pais
        )
        SELECT
            sc.pais,
            t.medios_distintos,
            t.total_menciones,
            ROUND(
                -SUM((sc.n / t.total_menciones) * LOG(sc.n / t.total_menciones)),
                4
            ) AS indice_shannon_fuentes
        FROM source_counts sc
        JOIN totals t
            ON sc.pais = t.pais
        GROUP BY sc.pais, t.medios_distintos, t.total_menciones
        ORDER BY indice_shannon_fuentes DESC
    """
    return ejecutar(spark, sql, "indice_diversidad_fuentes_pais.json")


# ============================================================ 1 row
# 16. FRECUENCIA DE CONFLICTOS POR ETNIA DE LOS ACTORES
# ============================================================

def frecuencia_conflictos_por_etnia(spark):
    sql = """
        WITH etnias AS (
            SELECT
                Actor1EthnicCode AS etnia,
                GLOBALEVENTID,
                CAST(GoldsteinScale AS DOUBLE) AS GoldsteinScale,
                CAST(AvgTone AS DOUBLE) AS AvgTone
            FROM events
            WHERE CAST(QuadClass AS INT) IN (3, 4)
              AND Actor1EthnicCode IS NOT NULL

            UNION ALL

            SELECT
                Actor2EthnicCode AS etnia,
                GLOBALEVENTID,
                CAST(GoldsteinScale AS DOUBLE) AS GoldsteinScale,
                CAST(AvgTone AS DOUBLE) AS AvgTone
            FROM events
            WHERE CAST(QuadClass AS INT) IN (3, 4)
              AND Actor2EthnicCode IS NOT NULL
        )
        SELECT
            etnia,
            COUNT(*) AS apariciones,
            COUNT(DISTINCT GLOBALEVENTID) AS eventos_distintos,
            ROUND(AVG(GoldsteinScale), 2) AS goldstein_promedio,
            ROUND(AVG(AvgTone), 2) AS tono_promedio
        FROM etnias
        GROUP BY etnia
        ORDER BY apariciones DESC
    """
    return ejecutar(spark, sql, "frecuencia_conflictos_por_etnia.json")


# ============================================================
# 17. DETECCIÓN DE NOTICIAS DE ÚLTIMA HORA:
# EVENTOS CON MÁS DE 100 MENCIONES EN MENOS DE 1 HORA
# ============================================================

def noticias_ultima_hora(spark, min_menciones=1):
    sql = f"""
        WITH mention_times AS (
            SELECT
                GLOBALEVENTID,
                MentionSourceName,
                to_timestamp(
                    substring(
                        lpad(CAST(CAST(MentionTimeDate AS DECIMAL(20,0)) AS STRING), 14, '0'),
                        1,
                        14
                    ),
                    'yyyyMMddHHmmss'
                ) AS mention_ts
            FROM mentions
            WHERE GLOBALEVENTID IS NOT NULL
              AND MentionTimeDate IS NOT NULL
        ),
        mention_times_valid AS (
            SELECT *
            FROM mention_times
            WHERE mention_ts IS NOT NULL
        ),
        firsts AS (
            SELECT
                GLOBALEVENTID,
                MIN(mention_ts) AS primera_mencion
            FROM mention_times_valid
            GROUP BY GLOBALEVENTID
        ),
        primera_hora AS (
            SELECT
                mt.GLOBALEVENTID,
                COUNT(*) AS menciones_primera_hora,
                COUNT(DISTINCT COALESCE(mt.MentionSourceName, 'SIN_FUENTE')) AS fuentes_primera_hora
            FROM mention_times_valid mt
            JOIN firsts f
                ON mt.GLOBALEVENTID = f.GLOBALEVENTID
            WHERE mt.mention_ts >= f.primera_mencion
              AND mt.mention_ts < f.primera_mencion + INTERVAL 1 HOUR
            GROUP BY mt.GLOBALEVENTID
        ),
        events_dedup AS (
            SELECT *
            FROM (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY GLOBALEVENTID
                        ORDER BY SQLDATE DESC
                    ) AS rn
                FROM events e
                WHERE GLOBALEVENTID IS NOT NULL
            )
            WHERE rn = 1
        )
        SELECT
            ph.GLOBALEVENTID,
            date_format(f.primera_mencion, 'yyyy-MM-dd HH:mm:ss') AS primera_mencion,
            date_format(to_date(CAST(e.SQLDATE AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd') AS fecha_evento,
            e.ActionGeo_CountryCode AS pais,
            e.EventCode,
            e.CAMEOCodeDescription,
            ph.menciones_primera_hora,
            ph.fuentes_primera_hora,
            e.SOURCEURL
        FROM primera_hora ph
        JOIN firsts f
            ON ph.GLOBALEVENTID = f.GLOBALEVENTID
        LEFT JOIN events_dedup e
            ON ph.GLOBALEVENTID = e.GLOBALEVENTID
        WHERE ph.menciones_primera_hora >= {min_menciones}
        ORDER BY ph.menciones_primera_hora DESC
        LIMIT 50
    """
    return ejecutar(spark, sql, "noticias_ultima_hora.json")


# ============================================================
# IDEA 1:
# ACTORES MÁS ASOCIADOS A EVENTOS NEGATIVOS
# ============================================================

def actores_mas_asociados_eventos_negativos(spark):
    sql = """
        WITH actores AS (
            SELECT
                'Actor1' AS rol_actor,
                Actor1Name AS nombre_actor,
                Actor1Code AS codigo_actor,
                Actor1CountryCode AS pais_actor,
                Actor1Type1Code AS tipo_actor,
                GLOBALEVENTID,
                CAST(AvgTone AS DOUBLE) AS avg_tone,
                CAST(GoldsteinScale AS DOUBLE) AS goldstein,
                CAST(QuadClass AS INT) AS quad_class,
                EventCode,
                CAMEOCodeDescription
            FROM events
            WHERE Actor1Name IS NOT NULL
              AND TRIM(Actor1Name) <> ''

            UNION ALL

            SELECT
                'Actor2' AS rol_actor,
                Actor2Name AS nombre_actor,
                Actor2Code AS codigo_actor,
                Actor2CountryCode AS pais_actor,
                Actor2Type1Code AS tipo_actor,
                GLOBALEVENTID,
                CAST(AvgTone AS DOUBLE) AS avg_tone,
                CAST(GoldsteinScale AS DOUBLE) AS goldstein,
                CAST(QuadClass AS INT) AS quad_class,
                EventCode,
                CAMEOCodeDescription
            FROM events
            WHERE Actor2Name IS NOT NULL
              AND TRIM(Actor2Name) <> ''
        ),
        negativos AS (
            SELECT *
            FROM actores
            WHERE avg_tone < 0
               OR goldstein < 0
               OR quad_class IN (3, 4)
        ),
        agregados AS (
            SELECT
                nombre_actor,
                COALESCE(codigo_actor, 'SIN_CODIGO') AS codigo_actor,
                COALESCE(pais_actor, 'SIN_PAIS') AS pais_actor,
                COALESCE(tipo_actor, 'SIN_TIPO') AS tipo_actor,
                rol_actor,
                COUNT(*) AS apariciones,
                COUNT(DISTINCT GLOBALEVENTID) AS eventos_distintos,
                ROUND(AVG(avg_tone), 2) AS tono_promedio,
                ROUND(AVG(goldstein), 2) AS goldstein_promedio,
                SUM(CASE WHEN quad_class IN (3, 4) THEN 1 ELSE 0 END) AS eventos_conflicto,
                ROUND(
                    100.0 * SUM(CASE WHEN quad_class IN (3, 4) THEN 1 ELSE 0 END) / COUNT(*),
                    2
                ) AS porcentaje_conflicto
            FROM negativos
            GROUP BY
                nombre_actor,
                COALESCE(codigo_actor, 'SIN_CODIGO'),
                COALESCE(pais_actor, 'SIN_PAIS'),
                COALESCE(tipo_actor, 'SIN_TIPO'),
                rol_actor
        ),
        ordenado AS (
            SELECT *
            FROM agregados
            ORDER BY apariciones DESC, eventos_distintos DESC
            LIMIT 100
        )
        SELECT
            CAST(nombre_actor AS STRING) AS nombre_actor,
            CAST(codigo_actor AS STRING) AS codigo_actor,
            CAST(pais_actor AS STRING) AS pais_actor,
            CAST(tipo_actor AS STRING) AS tipo_actor,
            CAST(rol_actor AS STRING) AS rol_actor,
            CAST(apariciones AS STRING) AS apariciones,
            CAST(eventos_distintos AS STRING) AS eventos_distintos,
            CAST(tono_promedio AS STRING) AS tono_promedio,
            CAST(goldstein_promedio AS STRING) AS goldstein_promedio,
            CAST(eventos_conflicto AS STRING) AS eventos_conflicto,
            CAST(porcentaje_conflicto AS STRING) AS porcentaje_conflicto
        FROM ordenado
    """
    return ejecutar(spark, sql, "actores_mas_asociados_eventos_negativos.json")


# ============================================================
# IDEA 2:
# EVENTOS POSITIVOS MÁS CUBIERTOS POR PAÍS
# ============================================================

def eventos_positivos_mas_cubiertos_por_pais(spark):
    sql = """
        WITH eventos_positivos AS (
            SELECT
                GLOBALEVENTID,
                ActionGeo_CountryCode AS pais,
                ActionGeo_FullName AS ubicacion,
                EventCode,
                EventBaseCode,
                EventRootCode,
                CAMEOCodeDescription,
                CAST(AvgTone AS DOUBLE) AS avg_tone,
                CAST(GoldsteinScale AS DOUBLE) AS goldstein,
                CAST(NumMentions AS INT) AS num_mentions_event,
                CAST(NumSources AS INT) AS num_sources_event,
                CAST(NumArticles AS INT) AS num_articles_event,
                SOURCEURL,
                to_date(CAST(SQLDATE AS STRING), 'yyyyMMdd') AS fecha_evento
            FROM events
            WHERE ActionGeo_CountryCode IS NOT NULL
              AND GLOBALEVENTID IS NOT NULL
              AND GoldsteinScale IS NOT NULL
              AND CAST(GoldsteinScale AS DOUBLE) > 0
              AND CAST(QuadClass AS INT) IN (1, 2)
        ),
        cobertura_mentions AS (
            SELECT
                GLOBALEVENTID,
                COUNT(*) AS total_menciones_reales,
                COUNT(DISTINCT MentionSourceName) AS fuentes_distintas_reales,
                ROUND(AVG(CAST(MentionDocTone AS DOUBLE)), 2) AS tono_promedio_documentos
            FROM mentions
            WHERE GLOBALEVENTID IS NOT NULL
            GROUP BY GLOBALEVENTID
        ),
        joined AS (
            SELECT
                e.*,
                COALESCE(m.total_menciones_reales, 0) AS total_menciones_reales,
                COALESCE(m.fuentes_distintas_reales, 0) AS fuentes_distintas_reales,
                COALESCE(m.tono_promedio_documentos, 0) AS tono_promedio_documentos
            FROM eventos_positivos e
            LEFT JOIN cobertura_mentions m
                ON e.GLOBALEVENTID = m.GLOBALEVENTID
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY pais
                    ORDER BY
                        total_menciones_reales DESC,
                        fuentes_distintas_reales DESC,
                        num_mentions_event DESC,
                        goldstein DESC
                ) AS ranking_pais
            FROM joined
        ),
        filtrado AS (
            SELECT *
            FROM ranked
            WHERE ranking_pais <= 10
            ORDER BY pais, ranking_pais
        )
        SELECT
            CAST(ranking_pais AS STRING) AS ranking_pais,
            CAST(GLOBALEVENTID AS STRING) AS global_event_id,
            date_format(fecha_evento, 'yyyy-MM-dd') AS fecha_evento,
            CAST(pais AS STRING) AS pais,
            CAST(COALESCE(ubicacion, 'SIN_UBICACION') AS STRING) AS ubicacion,
            CAST(EventCode AS STRING) AS event_code,
            CAST(EventBaseCode AS STRING) AS event_base_code,
            CAST(EventRootCode AS STRING) AS event_root_code,
            CAST(COALESCE(CAMEOCodeDescription, 'SIN_DESCRIPCION') AS STRING) AS descripcion_cameo,
            CAST(ROUND(avg_tone, 2) AS STRING) AS tono_evento,
            CAST(ROUND(goldstein, 2) AS STRING) AS goldstein,
            CAST(COALESCE(num_mentions_event, 0) AS STRING) AS num_mentions_event,
            CAST(COALESCE(num_sources_event, 0) AS STRING) AS num_sources_event,
            CAST(COALESCE(num_articles_event, 0) AS STRING) AS num_articles_event,
            CAST(total_menciones_reales AS STRING) AS total_menciones_reales,
            CAST(fuentes_distintas_reales AS STRING) AS fuentes_distintas_reales,
            CAST(tono_promedio_documentos AS STRING) AS tono_promedio_documentos,
            CAST(COALESCE(SOURCEURL, 'SIN_URL') AS STRING) AS source_url
        FROM filtrado
    """
    return ejecutar(spark, sql, "eventos_positivos_mas_cubiertos_por_pais.json")
import json
from flask import Flask, render_template, jsonify, abort
from pymongo import MongoClient

# el host es el nombre del servicio de compose, no localhost, porque
# corremos dentro de la red de docker :)
MONGO_URI = "mongodb://mongodb:27017/"
DATABASE_NAME = "local"

# tope de filas que mandamos al navegador para que la tabla no se vuelva
# eterna con colecciones de miles de documentos
LIMITE_FILAS = 1000

# colecciones internas de mongo que viven en la base local pero no son
# analisis nuestros, las escondemos
COLECCIONES_SISTEMA = {"startup_log"}

app = Flask(__name__)

client = MongoClient(MONGO_URI)
database = client[DATABASE_NAME]

# metadatos de cada analisis: titulo legible, configuracion del grafico de barras
# (que columnas son etiqueta, que columna es el valor, cuantas filas en el top) y
# como colorear (magnitud = escala secuencial, signo = rojo si negativo verde si
# positivo). las colecciones no listadas igual se muestran, solo que sin grafico
ANALISIS = {
    "mapa_calor_intensidad_conflictos": {
        "titulo": "Mapa de calor de intensidad de conflictos por pais por dia",
        "barra": {"label": ["pais"], "valor": "intensidad_total_goldstein", "top": 20},
        "color": "magnitud",
    },
    "top_10_paises_eventos_por_dia": {
        "titulo": "Top 10 paises que generan mas eventos noticiosos por dia",
        "barra": {"label": ["pais"], "valor": "total_eventos", "top": 20},
        "color": "magnitud",
    },
    "correlacion_avg_tone_fuentes": {
        "titulo": "Correlacion entre AvgTone y numero de fuentes noticiosas",
        "barra": None,
        "color": "signo",
    },
    "distribucion_cameo_por_region": {
        "titulo": "Distribucion de tipos de eventos CAMEO por region del mundo",
        "barra": {"label": ["region", "descripcion_cameo"], "valor": "total_eventos", "top": 20},
        "color": "magnitud",
    },
    "matriz_interaccion_actores": {
        "titulo": "Matriz de interaccion entre tipos de actores",
        "barra": {"label": ["actor1_categoria", "actor2_categoria"], "valor": "frecuencia", "top": 20},
        "color": "magnitud",
    },
    "paises_mayor_cobertura_mediatica": {
        "titulo": "Paises con mayor cobertura mediatica por evento",
        "barra": {"label": ["pais"], "valor": "razon_menciones_por_evento", "top": 20},
        "color": "magnitud",
    },
    "tendencia_sentimiento_pais": {
        "titulo": "Tendencia de sentimiento por pais (promedio movil del AvgTone)",
        "barra": None,
        "color": "signo",
    },
    "conflictos_pares_paises": {
        "titulo": "Pares de paises que entran en conflicto con mayor frecuencia",
        "barra": {"label": ["pais_a", "pais_b"], "valor": "total_conflictos", "top": 20},
        "color": "magnitud",
    },
    "escalada_eventos_menciones_24h": {
        "titulo": "Deteccion de escalada de eventos en 24 horas",
        "barra": {"label": ["pais", "EventCode"], "valor": "aceleracion_menciones", "top": 20},
        "color": "magnitud",
    },
    "conflictos_religion_region": {
        "titulo": "Agrupamiento de conflictos basados en religion por region",
        "barra": {"label": ["region", "religion_actor1"], "valor": "total_conflictos", "top": 20},
        "color": "magnitud",
    },
    "temas_gkg_continente_anio": {
        "titulo": "Principales temas extraidos por el GKG por continente por anio",
        "barra": {"label": ["tema"], "valor": "total_documentos", "top": 20},
        "color": "magnitud",
    },
    "organizaciones_mas_mencionadas_por_dia": {
        "titulo": "Organizaciones mas mencionadas a nivel global por dia",
        "barra": {"label": ["organizacion"], "valor": "total_documentos", "top": 20},
        "color": "magnitud",
    },
    "analisis_rezago_tono_conflicto": {
        "titulo": "Analisis de rezago: el tono de hoy predice conflictos de manana?",
        "barra": {"label": ["pais"], "valor": "correlacion_tono_hoy_conflicto_manana", "top": 20},
        "color": "signo",
    },
    "grafo_diplomacia_vs_conflicto": {
        "titulo": "Grafo de interacciones diplomaticas vs conflictos entre paises",
        "barra": {"label": ["pais_a", "pais_b"], "valor": "total_interacciones", "top": 20},
        "color": "magnitud",
    },
    "indice_diversidad_fuentes_pais": {
        "titulo": "Indice de diversidad de fuentes por pais (Shannon)",
        "barra": {"label": ["pais"], "valor": "indice_shannon_fuentes", "top": 20},
        "color": "magnitud",
    },
    "frecuencia_conflictos_por_etnia": {
        "titulo": "Frecuencia de conflictos por etnia de los actores",
        "barra": {"label": ["etnia"], "valor": "apariciones", "top": 20},
        "color": "magnitud",
    },
    "noticias_ultima_hora": {
        "titulo": "Deteccion de noticias de ultima hora (mas de 100 menciones en menos de 1 hora)",
        "barra": {"label": ["pais", "EventCode"], "valor": "menciones_primera_hora", "top": 20},
        "color": "magnitud",
    },
    "actores_mas_asociados_eventos_negativos": {
        "titulo": "Extra: actores mas asociados a eventos negativos",
        "barra": {"label": ["nombre_actor"], "valor": "apariciones", "top": 20},
        "color": "magnitud",
    },
    "eventos_positivos_mas_cubiertos_por_pais": {
        "titulo": "Extra: eventos positivos mas cubiertos por pais",
        "barra": {"label": ["pais"], "valor": "total_menciones_reales", "top": 20},
        "color": "magnitud",
    },
}


def titulo_de(nombre):
    # si no conocemos la coleccion devolvemos el nombre crudo, mejor algo que nada
    if nombre in ANALISIS:
        return ANALISIS[nombre]["titulo"]
    return nombre


def es_analisis(nombre):
    # filtramos colecciones internas de mongo y cualquier system.*
    if nombre in COLECCIONES_SISTEMA:
        return False
    if nombre.startswith("system."):
        return False
    return True


def colecciones_de_analisis():
    nombres = [n for n in database.list_collection_names() if es_analisis(n)]
    # primero las conocidas en el orden del enunciado, luego cualquier extra
    conocidas = [n for n in ANALISIS.keys() if n in nombres]
    extra = sorted(n for n in nombres if n not in ANALISIS)
    return conocidas + extra


def a_numero(valor):
    # los datos a veces vienen como string por los CAST de spark, intentamos
    # convertir y si no se puede devolvemos None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def columnas_de(documentos):
    # juntamos todas las llaves que aparecen, respetando el orden de aparicion
    columnas = []
    for documento in documentos:
        for clave in documento.keys():
            if clave not in columnas:
                columnas.append(clave)
    return columnas


def deduplicar(documentos):
    # el pipeline reinserta los resultados cada ciclo, asi que llegan filas
    # exactamente repetidas. aca colapsamos las copias identicas para que la
    # vista no muestre la misma fila 30 veces. no tocamos la base, solo la
    # presentacion :)
    vistos = set()
    unicos = []
    for documento in documentos:
        firma = json.dumps(documento, sort_keys=True, default=str)
        if firma in vistos:
            continue
        vistos.add(firma)
        unicos.append(documento)
    return unicos


def datos_de(nombre):
    # lectura de una coleccion, sin el _id, con tope de filas y sin duplicados
    coleccion = database[nombre]
    total_bruto = coleccion.count_documents({})
    documentos = list(coleccion.find({}, {"_id": 0}).limit(LIMITE_FILAS))
    unicos = deduplicar(documentos)
    return unicos, total_bruto


@app.route("/")
def index():
    analisis = []
    for nombre in colecciones_de_analisis():
        # contamos filas unicas para que el numero calce con lo que se ve en cada
        # pagina (la base trae repeticiones del pipeline)
        unicos, _ = datos_de(nombre)
        analisis.append(
            {
                "nombre": nombre,
                "titulo": titulo_de(nombre),
                "filas": len(unicos),
                "tiene_grafico": bool(ANALISIS.get(nombre, {}).get("barra")),
            }
        )
    total_filas = sum(item["filas"] for item in analisis)
    return render_template("index.html", analisis=analisis, total_filas=total_filas)


@app.route("/analisis/<nombre>")
def analisis(nombre):
    if nombre not in colecciones_de_analisis():
        abort(404)
    config = ANALISIS.get(nombre, {})
    return render_template(
        "analisis.html",
        nombre=nombre,
        titulo=titulo_de(nombre),
        barra=config.get("barra"),
        color=config.get("color", "magnitud"),
    )


@app.route("/api/analisis/<nombre>")
def api_analisis(nombre):
    # endpoint que consume el frontend (d3) para dibujar y para el refresco en vivo
    if nombre not in colecciones_de_analisis():
        abort(404)
    documentos, total = datos_de(nombre)
    config = ANALISIS.get(nombre, {})
    return jsonify(
        {
            "nombre": nombre,
            "titulo": titulo_de(nombre),
            "columnas": columnas_de(documentos),
            "documentos": documentos,
            "mostradas": len(documentos),
            "total": total,
            "barra": config.get("barra"),
            "color": config.get("color", "magnitud"),
        }
    )


@app.route("/conclusiones")
def conclusiones():
    return render_template("conclusiones.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

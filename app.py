from flask import Flask, render_template, abort
from pymongo import MongoClient

# el host es el nombre del servicio de compose, no localhost, porque
# corremos dentro de la red de docker :)
MONGO_URI = "mongodb://database:27017/"
DATABASE_NAME = "local"

app = Flask(__name__)

client = MongoClient(MONGO_URI)
database = client[DATABASE_NAME]

# metadatos de cada analisis conocido: titulo legible y, si aplica, que columna
# graficar como barras. label puede ser una o varias columnas que se unen con " - "
# las colecciones que no esten aqui igual se muestran, solo que sin grafico
ANALISIS = {
    "mapa_calor_intensidad_conflictos": {
        "titulo": "Mapa de calor de intensidad de conflictos por pais por dia",
        "barra": {"label": ["pais"], "valor": "total_eventos_conflicto", "top": 15},
    },
    "top_10_paises_eventos_por_dia": {
        "titulo": "Top 10 paises que generan mas eventos noticiosos por dia",
        "barra": {"label": ["pais"], "valor": "total_eventos", "top": 15},
    },
    "correlacion_avg_tone_fuentes": {
        "titulo": "Correlacion entre AvgTone y numero de fuentes noticiosas",
        "barra": None,
    },
    "distribucion_cameo_por_region": {
        "titulo": "Distribucion de tipos de eventos CAMEO por region del mundo",
        "barra": {"label": ["region", "descripcion_cameo"], "valor": "total_eventos", "top": 15},
    },
    "matriz_interaccion_actores": {
        "titulo": "Matriz de interaccion entre tipos de actores",
        "barra": {"label": ["actor1_categoria", "actor2_categoria"], "valor": "frecuencia", "top": 15},
    },
    "paises_mayor_cobertura_mediatica": {
        "titulo": "Paises con mayor cobertura mediatica por evento",
        "barra": {"label": ["pais"], "valor": "razon_menciones_por_evento", "top": 15},
    },
    "tendencia_sentimiento_pais": {
        "titulo": "Tendencia de sentimiento por pais (promedio movil del AvgTone)",
        "barra": None,
    },
    "conflictos_pares_paises": {
        "titulo": "Pares de paises que entran en conflicto con mayor frecuencia",
        "barra": {"label": ["pais_a", "pais_b"], "valor": "total_conflictos", "top": 15},
    },
    "escalada_eventos_menciones_24h": {
        "titulo": "Deteccion de escalada de eventos en 24 horas",
        "barra": {"label": ["pais", "EventCode"], "valor": "aceleracion_menciones", "top": 15},
    },
    "conflictos_religion_region": {
        "titulo": "Agrupamiento de conflictos basados en religion por region",
        "barra": {"label": ["region"], "valor": "total_conflictos", "top": 15},
    },
    "temas_gkg_continente_anio": {
        "titulo": "Principales temas extraidos por el GKG por continente por anio",
        "barra": {"label": ["tema"], "valor": "total_documentos", "top": 15},
    },
    "organizaciones_mas_mencionadas_por_dia": {
        "titulo": "Organizaciones mas mencionadas a nivel global por dia",
        "barra": {"label": ["organizacion"], "valor": "total_documentos", "top": 15},
    },
    "analisis_rezago_tono_conflicto": {
        "titulo": "Analisis de rezago: el tono de hoy predice conflictos de manana?",
        "barra": {"label": ["pais"], "valor": "conflictos_promedio_manana", "top": 15},
    },
    "grafo_diplomacia_vs_conflicto": {
        "titulo": "Grafo de interacciones diplomaticas vs conflictos entre paises",
        "barra": {"label": ["pais_a", "pais_b"], "valor": "total_interacciones", "top": 15},
    },
    "indice_diversidad_fuentes_pais": {
        "titulo": "Indice de diversidad de fuentes por pais (Shannon)",
        "barra": {"label": ["pais"], "valor": "indice_shannon_fuentes", "top": 15},
    },
    "frecuencia_conflictos_por_etnia": {
        "titulo": "Frecuencia de conflictos por etnia de los actores",
        "barra": {"label": ["etnia"], "valor": "apariciones", "top": 15},
    },
    "noticias_ultima_hora": {
        "titulo": "Deteccion de noticias de ultima hora (mas de 100 menciones en menos de 1 hora)",
        "barra": {"label": ["pais", "EventCode"], "valor": "menciones_primera_hora", "top": 15},
    },
    "actores_mas_asociados_eventos_negativos": {
        "titulo": "Extra: actores mas asociados a eventos negativos",
        "barra": {"label": ["nombre_actor"], "valor": "apariciones", "top": 15},
    },
    "eventos_positivos_mas_cubiertos_por_pais": {
        "titulo": "Extra: eventos positivos mas cubiertos por pais",
        "barra": {"label": ["pais"], "valor": "total_menciones_reales", "top": 15},
    },
}


def titulo_de(nombre):
    # si no conocemos la coleccion devolvemos el nombre crudo, mejor algo que nada
    if nombre in ANALISIS:
        return ANALISIS[nombre]["titulo"]
    return nombre


def a_numero(valor):
    # los datos vienen a veces como string por los CAST de spark, intentamos
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


def construir_barras(nombre, documentos):
    # arma la lista de barras para el grafico si la coleccion tiene metrica conocida
    config = ANALISIS.get(nombre, {}).get("barra")
    if not config:
        return None

    crudas = []
    for documento in documentos:
        valor = a_numero(documento.get(config["valor"]))
        if valor is None:
            continue
        etiqueta = " - ".join(
            str(documento.get(campo, "")) for campo in config["label"]
        )
        crudas.append({"etiqueta": etiqueta, "valor": valor})

    if not crudas:
        return None

    # ordenamos de mayor a menor y nos quedamos con el top configurado
    crudas.sort(key=lambda fila: fila["valor"], reverse=True)
    crudas = crudas[: config.get("top", 15)]

    # el ancho de la barra es proporcional al valor maximo, ojo si el maximo es 0
    maximo = max(abs(fila["valor"]) for fila in crudas) or 1
    for fila in crudas:
        fila["porcentaje"] = round(abs(fila["valor"]) / maximo * 100, 2)

    return crudas


@app.route("/")
def index():
    # listamos solo las colecciones que existen de verdad en mongo, asi no
    # mostramos analisis que vinieron vacios o que aun no se corrieron
    nombres = sorted(database.list_collection_names())
    analisis = [{"nombre": nombre, "titulo": titulo_de(nombre)} for nombre in nombres]
    return render_template("index.html", analisis=analisis)


@app.route("/analisis/<nombre>")
def analisis(nombre):
    if nombre not in database.list_collection_names():
        abort(404)

    documentos = list(database[nombre].find({}, {"_id": 0}))
    columnas = columnas_de(documentos)
    barras = construir_barras(nombre, documentos)

    return render_template(
        "analisis.html",
        titulo=titulo_de(nombre),
        columnas=columnas,
        documentos=documentos,
        barras=barras,
    )


@app.route("/conclusiones")
def conclusiones():
    return render_template("conclusiones.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

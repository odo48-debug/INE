from fastapi import FastAPI, Query
import requests

app = FastAPI(title="API INE Municipios", version="1.0")

# --- CONFIGURACIÓN ---
TABLAS_MUNICIPALES = {
    "poblacion_municipio": "29005",       
    "indicadores_urbanos": "69303",         
    "hogares_vivienda": "69302",
    "superficie_uso_suelo": "69305"
}

# Palabras clave a excluir (filtro negativo global)
FILTRO_EXCLUIR = ["ocupados", "habitante", "consumo", "Censo", "censo", "persona", "vacías", "convencionales", "Mediana", "cuartil"]

# --- FUNCIONES AUXILIARES ---
def get_series_municipio(tabla_id: str, municipio: str):
    """Busca todas las series de un municipio dentro de una tabla del INE"""
    url = f"https://servicios.ine.es/wstempus/js/ES/SERIES_TABLA/{tabla_id}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    series = resp.json()
    return [s for s in series if municipio.lower() in s.get("Nombre", "").lower()]

def filtrar_series(series, excluir=None):
    """Excluye series por palabras clave negativas"""
    if not excluir:
        return series
    filtradas = []
    for s in series:
        nombre = s.get("Nombre", "").lower()
        if any(p.lower() in nombre for p in excluir):
            continue
        filtradas.append(s)
    return filtradas

def get_datos_serie(codigo: str, n_last: int = 5):
    """Devuelve los datos de una serie específica"""
    url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/{codigo}?nult={n_last}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()

def get_datos_municipio(municipio: str, n_last: int = 5):
    """Obtiene todos los datos disponibles de un municipio en varias tablas"""
    resultados = {}
    for nombre_indicador, tabla_id in TABLAS_MUNICIPALES.items():
        series = get_series_municipio(tabla_id, municipio)
        series = filtrar_series(series, excluir=FILTRO_EXCLUIR)
        for s in series:
            cod = s["COD"]
            nombre = s["Nombre"]
            datos = get_datos_serie(cod, n_last=n_last)
            resultados[f"{nombre_indicador} - {nombre}"] = datos
    return resultados

# --- ENDPOINTS ---
@app.get("/")
def root():
    return {"message": "API INE Municipios en funcionamiento 🚀"}

@app.get("/municipio/{municipio}")
def consulta_municipio(municipio: str, n_last: int = Query(5, description="Número de últimos valores a obtener")):
    try:
        datos = get_datos_municipio(municipio, n_last=n_last)
        if not datos:
            return {"status": "warning", "message": f"No se encontraron series para {municipio}"}
        return {"status": "ok", "municipio": municipio, "n_series": len(datos), "datos": datos}
    except Exception as e:
        return {"status": "error", "message": str(e)}

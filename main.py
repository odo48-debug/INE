from fastapi import FastAPI, Query
import httpx
import asyncio

app = FastAPI(title="API INE Municipios", version="2.0")

# --- CONFIGURACIÓN ---
TABLAS_MUNICIPALES = {
    "poblacion_municipio": "29005",
    "indicadores_urbanos": "69303",
    "hogares_vivienda": "69302",
    "superficie_uso_suelo": "69305"
}

FILTRO_EXCLUIR = [
    "ocupados", "consumo", "Censo", "censo", "vacías",
    "convencionales", "Mediana", "cuartil"
]

# --- FUNCIONES ASÍNCRONAS ---
async def get_json_async(url: str, timeout: int = 15):
    """Devuelve JSON desde una URL, manejando errores y timeouts."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) or isinstance(data, dict) else []

async def get_series_municipio(tabla_id: str, municipio: str):
    """Obtiene todas las series de un municipio dentro de una tabla."""
    url = f"https://servicios.ine.es/wstempus/js/ES/SERIES_TABLA/{tabla_id}"
    data = await get_json_async(url)
    if not isinstance(data, list):
        return []
    return [s for s in data if municipio.lower() in s.get("Nombre", "").lower()]

def filtrar_series(series, excluir=None):
    """Excluye series por palabras clave negativas."""
    if not excluir:
        return series
    return [
        s for s in series
        if not any(p.lower() in s.get("Nombre", "").lower() for p in excluir)
    ]

async def get_datos_serie(codigo: str, n_last: int = 5):
    """Obtiene los últimos valores de una serie concreta."""
    url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/{codigo}?nult={n_last}"
    data = await get_json_async(url)
    return data if data else []

async def get_datos_municipio(municipio: str, n_last: int = 5):
    """Consulta en paralelo todas las tablas del INE para un municipio."""
    resultados = {}
    tareas = []

    for nombre_indicador, tabla_id in TABLAS_MUNICIPALES.items():
        tareas.append(asyncio.create_task(get_series_municipio(tabla_id, municipio)))

    todas_series = await asyncio.gather(*tareas, return_exceptions=True)

    for idx, series in enumerate(todas_series):
        nombre_indicador = list(TABLAS_MUNICIPALES.keys())[idx]
        if isinstance(series, Exception):
            resultados[nombre_indicador] = {"error": str(series)}
            continue

        series_filtradas = filtrar_series(series, FILTRO_EXCLUIR)
        for s in series_filtradas:
            cod = s.get("COD")
            nombre = s.get("Nombre")
            if not cod or not nombre:
                continue
            try:
                datos = await get_datos_serie(cod, n_last=n_last)
                resultados[f"{nombre_indicador} - {nombre}"] = datos
            except Exception as e:
                resultados[f"{nombre_indicador} - {nombre}"] = {"error": str(e)}

    return resultados

# --- ENDPOINTS ---
@app.get("/")
def root():
    return {"message": "API INE Municipios en funcionamiento 🚀"}

@app.get("/municipio/{municipio}")
async def consulta_municipio(municipio: str, n_last: int = Query(5, description="Número de últimos valores a obtener")):
    try:
        datos = await get_datos_municipio(municipio, n_last=n_last)
        if not datos:
            return {"status": "warning", "message": f"No se encontraron series para {municipio}"}
        return {
            "status": "ok",
            "municipio": municipio,
            "n_series": len(datos),
            "datos": datos
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

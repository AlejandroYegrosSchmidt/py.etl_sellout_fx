import pandas as pd
from google import genai
from dotenv import load_dotenv
import json
import os
import io
from tenacity import retry, wait_exponential, stop_after_attempt

# Carga las variables del archivo .env
load_dotenv()

api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

def excel_a_csv_texto(ruta_archivo):
    """
    Convierte la primera hoja de un Excel a un string CSV usando Pandas.
    Pandas es más tolerante a errores de 'drawings' que openpyxl.
    """
    # Leemos el excel (necesitarás instalar openpyxl o pyxlsb si pandas lo pide)
    df = pd.read_excel(ruta_archivo, sheet_name=0, nrows=50)
    output = io.StringIO()
    # Convertimos a CSV plano
    df.to_csv(output, index=False, encoding='utf-8')
    return output.getvalue()

def extraer_parametros_excel(ruta_archivo):
    # 1. Convertimos el Excel a texto para evitar errores de archivos corruptos y MIME types
    contenido_csv = excel_a_csv_texto(ruta_archivo)
    
    nombre_temp = "temp_datos_inspeccion.txt"
    with open(nombre_temp, "w", encoding="utf-8") as f:
        f.write(contenido_csv)

    # 2. Subir el archivo a la API (CORREGIDO: usamos 'file')
    file_upload = client.files.upload(
        file=nombre_temp,
        config={'mime_type': 'text/plain'}
    )

    # Tu Prompt original (se mantiene exactamente igual)
    prompt = """
    Analiza el archivo Excel adjunto y determina su estructura técnica para un proceso de ETL.
    La ruta del nombre del archivo es: {ruta_archivo}

    Tareas:
    1. Identifica el nombre del archivo.
    2. Determina el índice de la fila (empezando en 0) que contiene los nombres de las columnas (headers).
    3. Determina el índice de la fila (empezando en 0) donde comienzan los datos reales.
    4. Mapea la posición (índice 0-based) de las siguientes columnas:
       - cod_producto, producto_name, cod_sucursal, cadena, venta, cantidad, stock.

    Reglas:
    - Si el archivo contiene 'gdu ventas', considera:
        1. El cod_producto es ser la columna que contiene el nombre del producto. En este archivo el cod_producto debe ser el mismo valor que se encuentra en la columna "producto".
    - Si el archivo contiene 'tata ventas ', considera:
        1. El codigo del producto se encuentra en la columan ARTC_ARTC_ID
        2. El codigo de la sucursal se encuentra en la columan GEOG_LOCL_ID
    - Si no encuentras una columna exacta, busca sinónimos o nombres similares.
    - JAMAS ASUMAS QUE UNA COLUMNA EXISTE. POR EJEMPLO, SI NO ENCUENTRAS UNA COLUMNA DE 'STOCK', ASIGNA -1 A STOCK, NO ASUMAS QUE ES LA ÚLTIMA COLUMNA O ALGO ASÍ.
    - Devuelve ESTRICTAMENTE un objeto JSON.

    Formato de salida esperado:
    {
    "fecha": indice de la columna que contiene la fecha,
    "header": valor del índice de la fila del header,
    "cod_producto": valor del índice de la columna cod_producto,
    "producto_name": valor del índice de la columna producto_name, 
    "cod_sucursal": valor del índice de la columna cod_sucursal,
    "cod_cadena": valor del índice de la columna cadena,    
    "venta": valor del índice de la columna venta,
    "cantidad": valor del índice de la columna cantidad,
    "stock": valor del índice de la columna stock
    }
    """

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3)) 
    def generar_con_reintento():
        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=[file_upload, prompt],
            config={'response_mime_type': 'application/json'}
        )
        return json.loads(response.text)

    try:
        resultado = generar_con_reintento()
        
        # AJUSTE PARA EVITAR EL AttributeError:
        # Si el resultado es una lista, tomamos el primer elemento
        if isinstance(resultado, list) and len(resultado) > 0:
            datos_finales = resultado[0]
        else:
            datos_finales = resultado
        print("#########################################################################")
        print(f"--- Estructura detectada para: {os.path.basename(ruta_archivo)} ---\n Las Id de las columnas son:")
        print("#########################################################################")
        
        # Iteramos sobre datos_finales que ahora garantizamos que es un diccionario
        if isinstance(datos_finales, dict):
            for llave, valor in datos_finales.items(): 
                print(f"##... {llave}: {valor}")
            print("#########################################################################")
            print("--- Fin de la estructura detectada ---")
            print("#########################################################################")
        else:
            print("El modelo devolvió un formato inesperado:", datos_finales)
            
        return datos_finales  
    
    finally:
        # Limpieza
        client.files.delete(name=file_upload.name)
        if os.path.exists(nombre_temp):
            os.remove(nombre_temp)

# if __name__ == "__main__":
#     ruta = r"C:\Users\aleja\Desktop\Drive\Clientes\Fortylex\fortylex_sellout_etl\py.etl_sellout_fx\data\gdu_ventas_febrero_26.xlsx"
#     config = extraer_parametros_excel(ruta)
    
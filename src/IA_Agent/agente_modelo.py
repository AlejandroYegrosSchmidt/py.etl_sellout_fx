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
    # Extraemos solo el nombre para que la IA no se pierda en la ruta de carpetas
    nombre_archivo_limpio = os.path.basename(ruta_archivo)

    prompt = f"""
    Analiza el contenido del archivo adjunto y el nombre del archivo proporcionado para determinar la estructura técnica de un proceso ETL.

    CONTEXTO:
    - Nombre del archivo: {nombre_archivo_limpio}
    - Formato del contenido: CSV (procedente de Excel)

    TAREAS:
    1. Identifica la 'base' basándote EXCLUSIVAMENTE en el nombre del archivo ({nombre_archivo_limpio}). 
    - Si contiene 'gdu', la base es GDU.
    - Si contiene 'tata', la base es TATA.
    - Si contiene 'macro', la base es MACRO.
    - Si contiene 'tienda', la base es TIENDA.
    - Si contiene 'polakof', la base es POLAKOF.
    - De lo contrario, usa 'DESCONOCIDA'.

    2. Determina el índice de fila del header (nombres de columnas) y el índice de inicio de datos.

    3. Mapea los índices de las columnas (0-based) siguiendo estas reglas específicas por base:

    SI LA BASE ES 'GDU':
    - cod_producto y producto_name deben apuntar al índice de la columna que contiene el nombre del producto.
    
    SI LA BASE ES 'TATA':
    - fecha: TIEM_DIA_ID
    - cod_producto: ARTC_ARTC_ID
    - producto_name: ARTC_ARTC_DESC
    - cod_sucursal: GEOG_LOCL_ID
    - sucursal_name: GEOG_LOCL_DESC
    - cod_cadena: Asignar -1
    - venta: VNTA_IMPORTE_SIN_IVA
    - cantidad: VNTA_UNIDADES

    REGLAS GENERALES:
    - Si no encuentras una columna exacta, busca sinónimos (ej. 'vta' por 'venta', 'cant' por 'cantidad').
    - Si una columna no existe (como 'stock' en muchos casos), asigna -1. NO inventes índices.
    - Devuelve ESTRICTAMENTE un objeto JSON.

    FORMATO DE SALIDA:
    {{
    "fecha": int,
    "header": int,
    "cod_producto": int,
    "producto_name": int,
    "cod_sucursal": int,
    "sucursal_name": int,
    "cod_cadena": int,
    "venta": int,
    "cantidad": int,
    "stock": int,
    "base": "STRING"
    }}
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
    
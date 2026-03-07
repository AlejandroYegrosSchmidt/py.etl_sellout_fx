import pandas as pd

def transform_data(file):
    # Leer el excel
    df = pd.read_excel(file)
    
    # --- EJEMPLO DE TRANSFORMACIONES ---
    # 1. Quitar espacios en nombres de columnas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    
    # 2. Eliminar filas totalmente vacías
    df = df.dropna(how='all')
    
    # 3. Asegurar que una columna de fecha sea tipo datetime
    # df['fecha'] = pd.to_datetime(df['fecha'])
    
    # 4. Agregar una columna de auditoría (cuándo se cargó esto)
    df['loaded_at'] = pd.Timestamp.now()
    
    return df

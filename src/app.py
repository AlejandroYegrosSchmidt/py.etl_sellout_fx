import streamlit as st
from database import save_to_db, get_engine
from sqlalchemy import text


st.set_page_config(page_title="ETL Sell Out", layout="wide")

st.title("📊 Cargador de Datos Sell Out")
st.markdown("Sube el archivo Excel de la empresa para procesarlo y guardarlo en la DB.")

uploaded_file = st.file_uploader("Seleccionar archivo Excel", type=["xlsx"])

if uploaded_file is not None:
    try:
        with st.spinner('Procesando datos...'):
            # 1. Transformar
            df_cleaned = transform_data(uploaded_file)
            st.write("### Vista previa de datos limpios", df_cleaned.head())
            
            # 2. Guardar
            if st.button("Confirmar carga a Base de Datos"):
                save_to_db(df_cleaned, "sell_out_records")
                st.success("✅ ¡Datos guardados exitosamente en PostgreSQL!")
                
    except Exception as e:
        st.error(f"Hubo un error al procesar el archivo: {e}")
        
st.divider()

# --- SECCIÓN 2: VISTA PREVIA DE LA BASE DE DATOS ---
st.header("2. Últimos 10 registros en Base de Datos")

try:
    engine = get_engine()
    # Usamos una consulta SQL para traer lo más reciente
    # Asumimos que tienes una columna 'fecha' o podrías usar un ID
    query = "SELECT * FROM ventas_sellout ORDER BY index DESC LIMIT 10" 
    
    # Nota: Si no tienes columna 'index', usa otra columna de fecha o ID
    df_ultimos = pd.read_sql(query, engine)
    
    if not df_ultimos.empty:
        st.table(df_ultimos) # 'st.table' es mejor para vistas estáticas de pocos datos
    else:
        st.info("La base de datos está vacía por ahora.")
        
except Exception as e:
    st.warning("Todavía no hay datos cargados o la tabla no existe.")
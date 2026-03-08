import pandas as pd
from IA_Agent import agente_modelo  

class processing_pipeline:
    def __init__(self, ruta):
        self.engine = None
        self.ruta = ruta 
        
    def datavalidation(self, df = None, df_colum_validate = None, tabla_sql= None, columna_sql = None):
        """
        Docstring for datavalidation
        Validamos los datos nuevos que se van a exportar a la base de datos.
        Evitamos duplicados en la base de datos.
        """
        try:
            engine = self.engine
            sql_existentes = pd.read_sql(f"SELECT `{columna_sql}` FROM {tabla_sql}", con=engine)
            df = df[~df[f'{df_colum_validate}'].isin(sql_existentes[f'{columna_sql}'])]
            df.to_sql(name=tabla_sql, con=engine, if_exists="append", index=False)
            print(f"Exportacion a {tabla_sql} realizado, se exportaron {len(df)} registros.")
        except Exception as e:
            print(f"Error en datavalidation: {e}")
      
        return df
    
    def column_name_normalice(self):
        """Limpia los datos eliminando filas con valores nulos y ajustamos el nombde de las columnas"""
        if self.df is not None:
            # Normal el nombre de cada columna
            try:    
                self.df.columns = [col.strip().lower().replace(' ','_').replace('.','').replace('/','_')  for col in self.df.columns]
            except:
                self.df.columns = [col.strip().lower().replace(' ','_').replace('.','') for col in self.df.columns]    
            
            # Eliminar filas con valores nulos en columnas críticas
            self.df.dropna(subset=[self.df.columns[0]], inplace=True)
        else:
            print("No hay datos para limpiar. Carga los datos primero.") 
        return self.df
    
    def create_final_dataframe(self):
            """
            Crea un DF limpio mapeando cada columna deseada individualmente.
            """
            df_config = agente_modelo.extraer_parametros_excel(self.ruta)

            # 1. Carga inicial del Excel
            df_raw = pd.read_excel(self.ruta, header=df_config['header'])
            
            # 2. Diccionario de columnas que queremos (mapeo de la IA)
            # Excluimos metadatos del agente
            target_columns = {k: v for k, v in df_config.items() if k not in ['header', 'sheet_name', 'data_start_row']}

            # 3. Construir el DataFrame final columna por columna
            # Esto evita duplicados y desorden
            df_final = pd.DataFrame()

            for nombre_estandar, indice_excel in target_columns.items():
                if indice_excel >= 0 and indice_excel < len(df_raw.columns):
                    # Extraemos la columna del Excel original por su posición 
                    # y la asignamos con el nombre estándar (fecha, venta, etc.)
                    df_final[nombre_estandar] = df_raw.iloc[:, indice_excel]
                else:
                    # Si la IA no la encontró, la creamos vacía para no romper el esquema SQL
                    df_final[nombre_estandar] = None

            print("#########################################################################")
            print('\n Dataframe preprocesado con éxito (Columnas únicas) \n')           
            print("#########################################################################")
            print(df_final.head())
            
            return df_final
import pandas as pd
import re

class SellOutPipeline:
    def __init__(self, engine):
        self.engine = engine
    
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
    
    def columns_validation(self):
            """
            Crear un df con todas las columnas que necesitamos y sus respectivos id.
            El df es creado en función a la extración de la ID de las columnas mediante IA.
            
            Agregar nuevas columnas:
             Si se desea agregar nuevas columnas es necesario agregarlas en la función extraer_id_columnas del agente modelo.

            """
            if self._df_final is not None:
             return self._df_final

            # Recibimos el df limpio
            df = self.clean_data()
            print('Dataframe recibido, se ajustaron los nombres de las columnas\n')
            print(df.head())
 
            # Diccionario con las id de las columnas
            values_columns_values = agent_modelo.extraer_id_columnas(df)      
            try: 
                # Usamos un agente IA para determinar la Id de cada columna
                # Verificamos que las columnas existan, ajustamos el nombre y si la columna no existe creamos la columna con None
                for key, val in values_columns_values.items():
                    if val != -1:
                        df = df.rename(columns={df.columns[val]: key})
                    else:
                        df[key] = 0
                # Nos quedamos solo con las columnas necesarias
                df = df.loc[:, [key for key in values_columns_values.keys()]]
            except Exception as e: 
                    print(f"Error al extraer las ID de las columnas: {e}")
                    return None
      
            
            df['posicion_arancelaria'] = df['posicion_arancelaria'].astype(str).str.strip()
            df['operacion'] = df['operacion'].astype(str).str.strip()
             
            # Las partidas arancelarias que seran exportadas son
            # 8429 : Maquinarias
            # 8717 : Semiremolques
            # 8701 : Tractores
            # 8702 y 8703 : Autobuses y Automoviles de Turismo
            # 8704 : Vehiculos para el transporte de mercaderias
            # 8705 : Camiones grúa, camiones de bomberos, camiones hormigonera, coches barredera, coches esparcidores, coches taller, coches radiológicos
            # df = df[df['posicion_arancelaria'].str.contains(r'^(8701|8429|8702|8703|8704|8716)', na=False)]
            df = df[df['posicion_arancelaria'].str.contains(r'^(8716)', na=False)]
            df = df[df['operacion'].str.contains('IMPORTACION', case=False, na=False)]
            df['valor_cif'] = df['valor_flete'] + df['valor_seguro'] + df['valor_fob']
            df['cod_ncm'] = df['posicion_arancelaria'].astype(str).str.strip().str[:10]
            
            print('\nDataframe preprocesados \n')           
            print(df.head())

            return df
import pandas as pd
from IA_Agent import agente_modelo  
import warnings
import os
from sqlalchemy import create_engine


# Filtra específicamente la advertencia de estilos de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")

class processing_pipeline:
    def __init__(self, ruta):
        self.engine = None
        self.ruta = ruta 
        self.df_config = agente_modelo.extraer_parametros_excel(self.ruta)
        self.df = None

    def get_engine(self):
        # Buscamos la variable que definiste en el docker-compose
        # Si no la encuentra (ej. corriendo fuera de Docker), usa localhost
        url = os.getenv(
            "DATABASE_URL", 
            "postgresql://user_etl:password_etl@localhost:5432/db_sellout"
        )
        
        # Importante: SQLAlchemy prefiere 'postgresql+psycopg2://' 
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            
        return create_engine(url)

    def datavalidation(self, df = None, df_colum_validate = None, tabla_sql= None, columna_sql = None):
        """
        Docstring for datavalidation
        Validamos los datos nuevos que se van a exportar a la base de datos.
        Evitamos duplicados en la base de datos.
        """
        try:
            engine = self.engine()
            sql_existentes = pd.read_sql(f'SELECT "{columna_sql}" FROM {tabla_sql}', con=engine)
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
            
            # 1. Carga inicial del Excel
            df_raw = pd.read_excel(self.ruta, header=self.df_config['header'])
            
            # 2. Diccionario de columnas que queremos (mapeo de la IA)
            # Excluimos metadatos del agente
            exclude_keys = ['header', 'sheet_name', 'data_start_row','base']  
            target_columns = {key: value for key, value in self.df_config.items() if key not in exclude_keys}

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

            # Agregamos la columna de posting date
            df_final['posting_date'] = pd.to_datetime('today').normalize()

            # Filtramos 0 y nan
            df_final['venta'] = pd.to_numeric(df_final['venta'], errors='coerce').fillna(0).round(2)
            df_final['cantidad'] = pd.to_numeric(df_final['cantidad'], errors='coerce').fillna(0)
            df_final = df_final[df_final['venta'] != 0]
            df_final = df_final[df_final['cantidad'] != 0] 

            print("#########################################################################")
            print('\n Dataframe preprocesado con éxito (Columnas únicas) \n')           
            print("#########################################################################'\n")
            print(df_final.head(3))
            print('\n')
            self.df = df_final
            
            return df_final
    
    def dim_articulos(self):
        """
        Creamos la dimension articulos de cadena
        """
        df = self.df.copy()
        print("#########################################################################")
        print(" Dimension articulos")
        print("#########################################################################\n")

        if self.df_config.get('base') == 'TATA':
            try:
                # Creamos la dimension de articulos
                dim_articulos = df[['cod_producto','producto_name']]
                dim_articulos = dim_articulos.drop_duplicates(subset='cod_producto')
                dim_articulos = dim_articulos[dim_articulos['cod_producto'].notnull()]
                print(dim_articulos)
            except Exception as e:
                print(f"Error en la dimension articulos: {e}")

        elif self.df_config.get('base') == 'GDU':
            try:
                # Creamos la dimension de articulos
                dim_articulos = df[['cod_producto']].copy()
                dim_articulos[['cod_producto','producto_name']] = dim_articulos['cod_producto'].str.split('-', expand=True, n=1)
                dim_articulos[['cod_producto', 'producto_name']] = dim_articulos[['cod_producto', 'producto_name']].apply(lambda x: x.str.strip())
                dim_articulos = dim_articulos.drop_duplicates(subset='cod_producto')
                print(dim_articulos)

            except Exception as e:
                print(f"Error en la dimension sucursal: {e}")
        
        print("#########################################################################")
        print('Resumen')
        print(f"··· Nro de articulos {len(dim_articulos):,.0f}".replace(",", "."))
        print("#########################################################################\n")

        return df
    
    def dim_sucursal(self):
        """
        Creamos la dimesion sucursal de cada cadena
        """
        df = self.df.copy()

        if self.df_config.get('base') == 'TATA':
            try:
                # Creamos la dimension de articulos
                print("#########################################################################")
                print(" Dimension sucursal")
                print("#########################################################################\n")
                dim_sucursales = df[['cod_sucursal','sucursal_name']].copy()
                dim_sucursales = dim_sucursales.drop_duplicates(subset='cod_sucursal')
                dim_sucursales = dim_sucursales[dim_sucursales['cod_sucursal'].notnull()]
                print(dim_sucursales)

            except Exception as e:
                print(f"Error en la dimension sucursal: {e}")

        if self.df_config.get('base') == 'GDU':
            try:
                # Creamos la dimension de articulos
                print("#########################################################################")
                print(" Dimension sucursal")
                print("#########################################################################\n")
                dim_sucursales = df[['cod_sucursal']].copy()
                dim_sucursales[['cod_sucursal','sucursal_name']] = dim_sucursales['cod_sucursal'].str.split('-', expand=True, n=1)
                dim_sucursales[['cod_sucursal', 'sucursal_name']] = dim_sucursales[['cod_sucursal', 'sucursal_name']].apply(lambda x: x.str.strip())
                dim_sucursales = dim_sucursales.drop_duplicates(subset='cod_sucursal')
                print(dim_sucursales)

            except Exception as e:
                print(f"Error en la dimension sucursal: {e}")
        
        print("#########################################################################")
        print('Resumen')
        print(f"··· Nro de sucursales {len(dim_sucursales):,.0f}".replace(",", "."))
        print("#########################################################################\n")

        return df
    
    def ft_ventas(self):
        """
        Creamos la dimension sucursal de cada cadena
        """
        df = self.df.copy()

        if self.df_config.get('base') == 'TATA':
            try:
                # Creamos la dimension de articulos
                print("#########################################################################")
                print(" Fact table ventas")
                print("#########################################################################\n")
                df = df[['fecha','cod_producto','cod_sucursal','venta','cantidad','posting_date']]
                print(df)
            except Exception as e:
                print(f"Error en la fact table ventas: {e}")
        
        elif self.df_config.get('base') == 'GDU':
            try:
                # Creamos la dimension de articulos
                print("#########################################################################")
                print(" Fact table ventas")
                print("#########################################################################\n")
                df = df[['fecha','cod_producto','cod_sucursal','venta','cantidad','posting_date']]
                df[['cod_producto','producto']] = df['cod_producto'].str.split('-', expand=True, n=1)
                df[['cod_sucursal','sucursal']] = df['cod_sucursal'].str.split('-', expand=True, n=1)
                df[['cod_producto', 'cod_sucursal']] = df[['cod_producto', 'cod_sucursal']].apply(lambda x: x.str.strip())
                df = df.drop(columns=['producto', 'sucursal'])
                print(df)
            except Exception as e:
                print(f"Error en la fact table ventas: {e}")
        
        print("#########################################################################")
        print('Resumen')
        print(f"··· Ventas: {df['venta'].sum():,.0f}".replace(",", "."))
        print(f"··· Cantidad: {df['cantidad'].sum():,.0f}".replace(",", "."))
        print(f"··· Nro de filas {len(df):,.0f}".replace(",", "."))
        print("#########################################################################\n")

        return df 
    
    def exportacion(self):
            print('Export process has been starting')

            print('Export articulos')
            # Verificamos la existencia de nuevos articulos y exportamos unicamente los nuevos articulos
            self.validacion_sql(df=self.articulos, df_colum_validate='cod_producto',tabla_sql = None, columna_sql='cod_producto')
            
            # Verificamos la existencia de nuevas zonas geograficas y exportamos unicamente las nuevas zonas
            print('Export sucursales')
            self.validacion_sql(df=self.sucursales, df_colum_validate='cod_sucursal', tabla_sql = None, columna_sql='cod_sucursal')            
            
            print('Export datos')
            df.to_sql(name=self.ft_ventas, con=self.engine, if_exists="append", index=False)
            
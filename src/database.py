from sqlalchemy import create_engine
import os

# Docker usa el nombre del servicio 'db' definido en docker-compose.yml
DB_URL = "postgresql://user_etl:password_etl@db:5432/db_sellout"

def get_engine():
    return create_engine(DB_URL)

def save_to_db(df, table_name):
    engine = get_engine()
    # 'append' para que no borre los datos anteriores al subir uno nuevo
    df.to_sql(table_name, engine, if_exists='append', index=False)
    return True

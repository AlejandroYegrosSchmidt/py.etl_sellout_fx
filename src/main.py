import pipeline_sellout


if __name__ == "__main__":
    ruta = rf"C:\Users\aleja\Desktop\Drive\Clientes\Fortylex\fortylex_sellout_etl\py.etl_sellout_fx\data\ventas_tata_febrero_26.xlsx"
    pipeline = pipeline_sellout.processing_pipeline(ruta=ruta)
    df_final = pipeline.create_final_dataframe()
    dim_articulos = pipeline.dim_articulos()
    dim_sucursal = pipeline.dim_sucursal()
    ft_ventas = pipeline.ft_ventas()
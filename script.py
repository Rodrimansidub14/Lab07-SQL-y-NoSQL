import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine
from bson import ObjectId
from datetime import datetime

def conectar_postgres():
    try:
        return psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port=os.getenv("DB_PORT")
        )
    except psycopg2.OperationalError as e:
        print(f"Error conectando a PostgreSQL: {e}")
        return None

def conectar_mongo():
    try:
        cliente = MongoClient(os.getenv("MONGO_URI"))
        print("MongoDB conectado correctamente")
        return cliente[os.getenv("MONGO_DB_NAME")]
    except Exception as e:
        print(f"Error conectando a MongoDB: {e}")
        return None

def extraer_tabla_pgsql(conexion, tabla):
    try:
        return pd.read_sql(f"SELECT * FROM {tabla}", conexion)
    except Exception as e:
        print(f"Error al extraer {tabla}: {e}")
        return pd.DataFrame()

def unir_collections_mongo(db):
    regiones = [
        'costos_turisticos_africa',
        'costos_turisticos_america',
        'costos_turisticos_asia',
        'costos_turisticos_europa'
    ]
    datos = []
    for col in regiones:
        datos.extend(list(db[col].find()))
    return pd.DataFrame(datos)

def guardar_en_dw(dataframe, tabla):
    if '_id' in dataframe.columns:
        dataframe['_id'] = dataframe['_id'].astype(str)

    try:
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('DW_USER')}:{os.getenv('DW_PASS')}@localhost:{os.getenv('DW_PORT')}/{os.getenv('DW_NAME')}"
        )
        dataframe.to_sql(
            name=tabla,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"DataFrame guardado exitosamente en el DW: {tabla}")
        return True
    except Exception as e:
        print(f"Error al guardar en el DW: {e}")
        return False

# Carga de variables de entorno
load_dotenv()

# Extracción desde PostgreSQL
pg_conn = conectar_postgres()
if pg_conn:
    df_poblacion = extraer_tabla_pgsql(pg_conn, "pais_poblacion")
    df_envejecimiento = extraer_tabla_pgsql(pg_conn, "pais_envejecimiento")

    df_poblacion['pais_con_mismos_nombres'] = df_poblacion['pais'].str.strip().str.lower() #Esto es de la tabla poblacion
    df_envejecimiento['nombre_pais_con_mismos_nombres'] = df_envejecimiento['nombre_pais'].str.strip().str.lower() #Esto de la tabla envejecimiento

    df_sql_integrado = pd.merge( #Esto es como la limpieza de datos para que las uniones sean correctas en el merge
        df_poblacion,
        df_envejecimiento[['nombre_pais_con_mismos_nombres', 'tasa_de_envejecimiento']], 
        left_on='pais_con_mismos_nombres',
        right_on='nombre_pais_con_mismos_nombres',
        how='left'
    ).drop(columns=['pais_con_mismos_nombres', 'nombre_pais_con_mismos_nombres'], errors='ignore')

    pg_conn.close()

# Extracción y transformación desde Mongo
mongo_db = conectar_mongo()
if mongo_db is not None:
    df_turismo = unir_collections_mongo(mongo_db)

    if 'costos_diarios_estimados_en_dólares' in df_turismo.columns:
        costos_detalle = pd.json_normalize(df_turismo['costos_diarios_estimados_en_dólares'])
        costos_detalle.columns = [c.replace('.', '_') for c in costos_detalle.columns]

        df_turismo_normalizado = pd.concat([
            df_turismo.drop('costos_diarios_estimados_en_dólares', axis=1),
            costos_detalle
        ], axis=1)

        df_bigmac = pd.DataFrame(mongo_db['paises_mundo_big_mac'].find()).drop('_id', axis=1, errors='ignore')

        df_turismo_normalizado['pais_con_mismos_nombres'] = df_turismo_normalizado['país'].str.lower().str.strip()
        df_bigmac['pais_con_mismos_nombres'] = df_bigmac['país'].str.lower().str.strip()

        df_mongo_final = pd.merge( #Esto es como la limpieza de datos para que las uniones sean correctas en el merge
            df_turismo_normalizado,
            df_bigmac[['pais_con_mismos_nombres', 'precio_big_mac_usd']],
            on='pais_con_mismos_nombres',
            how='left'
        ).drop(columns=['pais_con_mismos_nombres'], errors='ignore')

# Integración final entre Mongo y PostgreSQL
if not df_sql_integrado.empty and not df_mongo_final.empty:
    df_sql_integrado['pais_con_mismos_nombres'] = df_sql_integrado['pais'].str.lower().str.strip()
    df_mongo_final['pais_con_mismos_nombres'] = df_mongo_final['país'].str.lower().str.strip()

    df_final = pd.merge(
        df_mongo_final,
        df_sql_integrado[['pais_con_mismos_nombres', 'tasa_de_envejecimiento']],
        on='pais_con_mismos_nombres',
        how='left'
    ).drop(columns=['pais_con_mismos_nombres'], errors='ignore')

    df_final['fecha_script'] = datetime.today().strftime('%Y-%m-%d')
    df_final.to_csv('resumen_turistico_dw.csv', index=False, encoding='utf-8-sig')

    if guardar_en_dw(df_final, 'resumen_turistico'):
        print("\nIntegración y carga completada con éxito")
    else:
        print("\nError al guardar en el Data Warehouse")
else:
    print("\nAlguno de los DataFrames está vacío. Verificá la fuente de datos.")
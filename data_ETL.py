import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from pymongo import MongoClient
from prefect import task, flow

# --- TASKS ---
# For the ingest_postgres_tables function, use SQLAlchemy instead of psycopg2 directly
@task
def ingest_postgres_tables():
    """
    Conecta a PostgreSQL y extrae dos tablas:
      - 'pais_poblacion'
      - 'pais_envejecimiento'
    Luego, normaliza la columna clave y realiza un merge (inner join) usando la columna 'pais' como llave.
    """
    # Use SQLAlchemy engine instead of psycopg2 connection
    engine = create_engine("postgresql+psycopg2://postgres:12345@localhost/lab7bd2")
    
    query1 = "SELECT * FROM pais_poblacion;"
    query2 = "SELECT * FROM pais_envejecimiento;"
    
    df_poblacion = pd.read_sql_query(query1, engine)
    df_envejecimiento = pd.read_sql_query(query2, engine)
    
    # Imprime las columnas para verificación (opcional)
    print("Columnas en pais_poblacion:", df_poblacion.columns.tolist())
    print("Columnas en pais_envejecimiento:", df_envejecimiento.columns.tolist())
    
    # Normalización: convertir a minúsculas y quitar espacios en blanco
    df_poblacion["pais"] = df_poblacion["pais"].str.strip().str.lower()
    if "nombre_pais" in df_envejecimiento.columns:
        df_envejecimiento["nombre_pais"] = df_envejecimiento["nombre_pais"].str.strip().str.lower()
        # Renombrar la columna 'nombre_pais' a 'pais' para la unión
        df_envejecimiento = df_envejecimiento.rename(columns={"nombre_pais": "pais"})
    
    # Realiza el merge usando la columna 'pais'
    df_postgres = pd.merge(df_poblacion, df_envejecimiento, on="pais", how="inner")
    print("Cantidad de registros después del merge:", len(df_postgres))
    return df_postgres


@task
def ingest_mongodb():
    """
    Conecta a MongoDB usando un URI, extrae todas las colecciones y las almacena
    en un diccionario donde cada clave es el nombre de la colección.
    """
    client = MongoClient("mongodb+srv://diegolinares225:admin@cluster0.zbjy5yh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client['test']  # Ajusta el nombre de la base de datos si es necesario
    collection_names = db.list_collection_names()
    
    dfs = {}
    for col_name in collection_names:
        data = list(db[col_name].find())
        df = pd.DataFrame(data)
        dfs[col_name] = df
        
    client.close()
    return dfs

@task
def clean_data(df):
    """
    Aplica una limpieza básica eliminando filas con valores nulos.
    """
    df_clean = df.dropna()
    return df_clean

@task
def clean_mongo_dfs(mongo_dfs):
    """
    Aplica limpieza a cada DataFrame del diccionario de MongoDB.
    """
    cleaned = {}
    for col, df in mongo_dfs.items():
        cleaned[col] = df.dropna()
    return cleaned


@task
def integrate_all_data(df_postgres, mongo_dfs):
    """
    Integra los datos combinando el DataFrame proveniente de PostgreSQL con
    cada uno de los DataFrames de MongoDB mediante un outer join en la columna 'pais'.
    
    Si en alguna colección la columna que identifica el país se llama 'nombre_pais' o 'país',
    se renombra a 'pais' para la integración.
    """
    integrated_df = df_postgres.copy()
    
    for col_name, df_mongo in mongo_dfs.items():
        # Si existe "nombre_pais", renómbralo a "pais"
        if "nombre_pais" in df_mongo.columns:
            df_mongo = df_mongo.rename(columns={"nombre_pais": "pais"})
        # Si existe "país" (con acento), renómbralo a "pais"
        elif "país" in df_mongo.columns:
            df_mongo = df_mongo.rename(columns={"país": "pais"})
        
        # Verifica que la columna 'pais' esté presente en la colección de MongoDB
        if "pais" not in df_mongo.columns:
            print(f"La colección '{col_name}' no tiene la columna 'pais'. Se omite su integración.")
            continue
        
        # Normaliza la columna para asegurar coincidencia (minúsculas y sin espacios extra)
        df_mongo["pais"] = df_mongo["pais"].astype(str).str.strip().str.lower()
        integrated_df["pais"] = integrated_df["pais"].astype(str).str.strip().str.lower()
        
        # Realiza el merge (outer join) usando 'pais' como llave
        integrated_df = pd.merge(integrated_df, df_mongo, on="pais", how="outer", suffixes=("", f"_{col_name}"))
        
    return integrated_df


@task
@task
def load_data(df):
    """
    Convierte los valores de tipo ObjectId a cadena, aplana los diccionarios anidados
    y carga el DataFrame integrado en el data warehouse.
    Se usa una base de datos llamada 'lab7DW' en la misma instancia de PostgreSQL.
    """
    from bson import ObjectId  # Importar ObjectId para identificar el tipo
    import json

    # Función para convertir ObjectId a string y diccionarios a JSON
    def convert_value(val):
        if isinstance(val, ObjectId):
            return str(val)
        elif isinstance(val, dict):
            return json.dumps(val)  # Convertir diccionarios a strings JSON
        return val

    # Aplicar la conversión a todo el DataFrame
    df_converted = df.applymap(convert_value)

    # Opcional: imprimir algunos registros para verificar la conversión
    print("Ejemplo de registros a insertar (después de la conversión):")
    print(df_converted.head())
    print("Número total de registros a insertar:", len(df_converted))

    try:
        # Intentar cargar en PostgreSQL
        engine = create_engine("postgresql+psycopg2://postgres:12345@localhost/lab7DW")
        df_converted.to_sql('integrated_data2', engine, if_exists='replace', index=False)
        return "Carga completada en PostgreSQL"
    except Exception as e:
        # Si falla, guardar en CSV como respaldo
        print(f"Error al cargar en PostgreSQL: {str(e)}")
        print("Guardando datos en CSV como alternativa...")
        csv_path = "c:\\Users\\rodri\\Documents\\BDD2\\Lab7\\integrated_data.csv"
        df_converted.to_csv(csv_path, index=False)
        return f"Datos guardados en {csv_path}"

# --- Definir el Flow ETL ---
@flow(name="ETL_Integracion_Total_2_Tablas")
def etl_flow():
    # Ingesta de datos de las dos tablas de PostgreSQL
    df_postgres = ingest_postgres_tables()
    
    # Ingesta de todas las colecciones de MongoDB
    mongo_dfs = ingest_mongodb()
    
    # Limpieza de datos
    df_postgres_clean = clean_data(df_postgres)
    mongo_dfs_clean = clean_mongo_dfs(mongo_dfs)
    
    # Integración de los datos (PostgreSQL + todas las colecciones de MongoDB)
    integrated_df = integrate_all_data(df_postgres_clean, mongo_dfs_clean)
    
    # Carga del DataFrame integrado en el data warehouse
    resultado_carga = load_data(integrated_df)
    return resultado_carga

# --- Ejecución del Flow ---
if __name__ == "__main__":
    etl_flow()

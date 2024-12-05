import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()
#obtiene la variable de entorno de la conexion a la base de datos
conexion = os.getenv("conexion")

#conecta a la base de datos
def crear_database(database_name):
    conexion = pyodbc.connect(conexion)
    #crea un cursor para ejecutar las consultas
    cursor = conexion.cursor()
    #verifica si la base de datos existe
    cursor.execute(f"SELECT * FROM sys.databases WHERE name = '{database_name}'")
    #si no existe, crea la base de datos
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {database_name}")
        print(f"Base de datos {database_name} creada")
    else:
        print(f"Base de datos {database_name} ya existe")  
    conexion.commit()
    conexion.close()
crear_database("MovieDatabase")

# import pyodbc
# import os
# from dotenv import load_dotenv
# from CrearBaseDatos import conectar_sql

# load_dotenv()
# SQL_SERVER = os.getenv("SQL_SERVER")
# USUARIO = os.getenv("USUARIO")
# PASSWORD = os.getenv("PASSWORD")

# def crear_tabla(tabla_name):
#     conexion = conectar_sql()
#     cursor = conexion.cursor()
#     cursor.execute(f"SELECT * FROM sys.tables WHERE name = '{tabla_name}'")
#     if not cursor.fetchone():
#         cursor.execute(f"""CREATE TABLE {tabla_name} (id_pelicula INT PRIMARY KEY,
#         titulo VARCHAR(255),
#         fecha_lanzamiento DATE,
#         idioma_original VARCHAR(100), 
#         promedio_votos FLOAT, 
#         recuento_votos INT, 
#         popularidad FLOAT, 
#         descripcion_general VARCHAR(255), 
#         id_genero VARCHAR(255))""")
#         print(f"Tabla {tabla_name} creada")
#     else:
#         print(f"Tabla {tabla_name} ya existe")
#     conexion.commit()
#     conexion.close()
# crear_tabla("movies")



import pyodbc
import requests
import pandas as pd
import os
from dotenv import load_dotenv
#carga las variables de entorno con los datos de la API
load_dotenv()
TOKEN_API = os.getenv("TOKEN_API")
CLAVE_API = os.getenv("CLAVE_API")
conexion = os.getenv("conexion")


def extract_movieData():
    datosExtraidos = []
    for page in range(1, 5):
        url = f"https://api.themoviedb.org/3/movie/popular?language=en-US&page={page}"
        headers = {
        #Token de acceso
        'Authorization': f'Bearer {TOKEN_API}',
        #Tipo de contenido
        'Content-Type': 'application/json',
        #Clave de acceso
        'CLAVE_API': CLAVE_API
    }
    #Realiza la solicitud a la API
        response = requests.get(url, headers=headers)
        #Verifica si la solicitud fue exitosa
        if response.status_code == 200:
            data = response.json()
            datosExtraidos.extend(data.get("results",[]))
            print(datosExtraidos)
        else:
            print(f'Error No se extrajeron los datos: {response.status_code} - {response.text}')
            return None
    return datosExtraidos[:100]

def transform_movieData(datosExtraidos):
    DatosTransformados =[]
    for movie in datosExtraidos:
        DatosTransformados.append({
            "id_pelicula": movie.get("id",None),
            "titulo": movie.get("title",None),
            "fecha_lanzamiento":movie.get("release_date",None),
            "idioma_original":movie.get("original_language",None),
            "promedio_votos":movie.get("vote_average",None),
            "recuento_votos":movie.get("vote_count",None),
            "popularidad":movie.get("popularity",None),
            "descripcion_general":movie.get("overview",None),
            "id_genero":movie.get("genre_ids",None)
        })
    print(pd.DataFrame(DatosTransformados))
    return DatosTransformados
datosExtraidos = extract_movieData()
transform_movieData(datosExtraidos)

# def load_SQLServer(data, cargarDatos = True):
#     if cargarDatos:
#         conn = pyodbc.connect(conexion)
#         if conn is None:
#             print("Error al conectar a SQL Server")
#             return
#         cursor = conn.cursor()
#         for movie in data:
#                 cursor.execute("""INSERT INTO movies (
#                                     id_pelicula, titulo, fecha_lanzamiento, idioma_original, promedio_votos, recuento_votos, popularidad, descripcion_general, id_genero) 
#                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
#                                     movie["id_pelicula"],
#                                     movie["titulo"], 
#                                     movie["fecha_lanzamiento"], 
#                                     movie["idioma_original"], 
#                                     movie["promedio_votos"], 
#                                     movie["recuento_votos"], 
#                                     movie["popularidad"], 
#                                     movie["descripcion_general"], 
#                                     movie["id_genero"])
#         conn.commit()
#         conn.close()

# data = extract_movieData()
# transform_movieData(data)
# load_SQLServer(data)

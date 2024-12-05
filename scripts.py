import pyodbc
import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

#carga las variables de entorno con los datos de la API
load_dotenv()
TOKEN_API = os.getenv("TOKEN_API")
CLAVE_API = os.getenv("CLAVE_API")
conexion = os.getenv("conexion")


def extract_movieData():
    datosExtraidos = []
    total_results = 0
    page = 1
    #Mientras la cantidad de datos extraidos sea menor
    while len(datosExtraidos) < 150:
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
            total_results += data.get("total_results",0)
            datosExtraidos.extend(data.get("results",[]))
            page += 1
            if len(data.get("results",[])) == 0:
                break
        else:
            print(f'Error No se extrajeron los datos: {response.status_code} - {response.text}')
            return None
    print(datosExtraidos)
    return datosExtraidos[:150]

#Transforma los datos extraidos
def transform_movieData(data):
    DatosTransformados =[]
    for movie in data:
        
        DatosTransformados.append({
            "id_pelicula": movie.get("id"),
            "titulo": movie.get("title"),
            "fecha_lanzamiento":datetime.strptime(movie.get("release_date"), "%Y-%m-%d"),#Transforma la fecha a datetime
            "idioma_original":movie.get("original_language"),
            "promedio_votos":movie.get("vote_average"),
            "recuento_votos":movie.get("vote_count"),
            "popularidad":movie.get("popularity"),
            "descripcion_general":movie.get("overview"),
            "id_genero": ','.join(map(str, movie.get("genre_ids", [])))  # Convierte la lista a una cadena
        })
    print(pd.DataFrame(DatosTransformados))
    return DatosTransformados

#Carga los datos transformados a la base de datos
def load_SQLServer(data,tabla_name):
        try:
            conn = pyodbc.connect(conexion)
            cursor = conn.cursor()
            print("Conexion exitosa")
        except Exception as e:
            print(f"Error al conectar a SQL Server: {e}")
            return
        duplicados = []
        noInsertados = []
#Inserta los datos en la base de datos
        for movie in data:
            id_pelicula = movie["id_pelicula"]
            titulo = movie["titulo"]
            fecha_lanzamiento = movie["fecha_lanzamiento"]
            idioma_original = movie["idioma_original"]
            promedio_votos = movie["promedio_votos"]
            recuento_votos = movie["recuento_votos"]
            popularidad = movie["popularidad"]
            descripcion_general = movie["descripcion_general"]
            id_genero = movie["id_genero"]

#Imprime los datos que se van a insertar
            # print(f"Inserción de película:{type(id_pelicula)}

#Verifica si la pelicula ya existe en la base de datos
            cursor.execute(f"SELECT * FROM {tabla_name} WHERE id_pelicula = ?", id_pelicula)
            existe = cursor.fetchone()
            #Si no existe, se inserta
            if not existe:
                try:
                    cursor.execute(f"""INSERT INTO {tabla_name} (
                                id_pelicula,
                                titulo, 
                                fecha_lanzamiento, 
                                idioma_original, 
                                promedio_votos, 
                                recuento_votos, 
                                popularidad, 
                                descripcion_general, 
                                id_genero) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                id_pelicula,
                                titulo,
                                fecha_lanzamiento,
                                idioma_original,
                                promedio_votos,
                                recuento_votos,
                                popularidad,
                                descripcion_general,
                                id_genero)
                except Exception as e:
                    print(f"Error al insertar la pelicula: {e}")
                    noInsertados.append(movie)
            else:
                duplicados.append(id_pelicula)

        conn.commit()
        conn.close()

        if duplicados:
            print(f"Peliculas duplicadas: {len(duplicados)}")
        else:
            noInsertados.append(f"peliculas no insertadas: {len(noInsertados)}")
            
#Extrae los datos de la API
data = extract_movieData()
#Transforma los datos extraidos
if data:
    datosTransformados = transform_movieData(data)
    #Carga los datos transformados a la base de datos
    load_SQLServer(datosTransformados,"movies")
else:
    print("No se pudo extraer los datos de la API")


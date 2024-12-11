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

#Extrae los datos de la API
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
            total_results += data.get("total_results",0)   #Suma el total de resultados
            datosExtraidos.extend(data.get("results",[]))  #Agrega los resultados a la lista
            page += 1
            if len(data.get("results",[])) == 0:   #Si no hay resultados, se rompe el bucle
                break
            if len(datosExtraidos) >= total_results:
                break
        else:
            print(f'Error No se extrajeron los datos: {response.status_code} - {response.text}') 
            return None
    print(len(datosExtraidos))
    return datosExtraidos[:150]          #Devuelve los primeros 150 datos	

#Transforma los datos extraidos
def transform_movieData(data):
    DatosTransformados =[]               #crea una lista para almacenar los datos transformados
    for movie in data:
        
        DatosTransformados.append({         #Agrega los datos transformados a la lista
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
        try:                                    #se conecta a la base de datos
            conn = pyodbc.connect(conexion)
            cursor = conn.cursor()
            print("Conexion exitosa")
        except Exception as e:                   #si hay un error, se imprime y se retorna
            print(f"Error al conectar a SQL Server: {e}")
            return
        insertados = []
        actualizados = []
        noInsertados = []
#Inserta los datos en la base de datos
        for movie in data:
            id_pelicula = movie["id_pelicula"]
            titulo = movie.get("titulo")
            fecha_lanzamiento = movie.get("fecha_lanzamiento")
            idioma_original = movie.get("idioma_original")
            promedio_votos = movie.get("promedio_votos")
            recuento_votos = movie.get("recuento_votos")
            popularidad = movie.get("popularidad")
            descripcion_general = movie.get("descripcion_general")
            id_genero = movie.get("id_genero")

#Imprime los datos que se van a insertar
            # print(f"Inserción de película:{type(id_pelicula)}

#verifica si la pelicula ya existe en la base de datos
            cursor.execute(f"SELECT * FROM {tabla_name} WHERE id_pelicula = ?", id_pelicula)
            existe = cursor.fetchone()
            #Si existe, se actualiza
            if existe:                              
                try:
                    cursor.execute(f"""UPDATE {tabla_name} SET
                                titulo = ?, 
                                fecha_lanzamiento = ?, 
                                idioma_original = ?, 
                                promedio_votos = ?, 
                                recuento_votos = ?, 
                                popularidad = ?, 
                                descripcion_general = ?, 
                                id_genero = ?
                                WHERE id_pelicula = ?""",
                                titulo,
                                fecha_lanzamiento,
                                idioma_original,
                                promedio_votos,
                                recuento_votos,
                                popularidad,
                                descripcion_general,
                                id_genero,
                                id_pelicula)
                    actualizados.append(id_pelicula)
                except Exception as e:                  #si hay un error, se agrega a la lista de no insertados
                    print(f"Error al insertar la pelicula: {e}")
                    noInsertados.append(movie)

    #Si no existe, se inserta
            else:
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
                    insertados.append(id_pelicula)
                except Exception as e:
                    print(f"Error al insertar la pelicula: {e}")
                    noInsertados.append(movie)

        conn.commit()
        conn.close()

        print(f"Peliculas actualizadas {len(actualizados)}")
        print(f"Peliculas insertadas {len(insertados)}")


#Guarda los datos en un archivo csv
def guardar_csv(data,nombre_archivo):
    df = pd.DataFrame(data)
    df.to_csv(nombre_archivo)        
    print(f"Datos guardados en {nombre_archivo}")
            
#Extrae los datos de la API
data = extract_movieData()
#Transforma los datos extraidos
if data:
    datosTransformados = transform_movieData(data)
    #Guarda los datos en un archivo csv
    guardar_csv(datosTransformados,"datoscargados.csv")
    #Carga los datos transformados a la base de datos
    load_SQLServer(datosTransformados,"movies")
else:
    print("No se pudo extraer los datos de la API")


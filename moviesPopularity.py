from datetime import datetime
import pandas as pd
from scripts import transform_movieData, load_SQLServer,extract_movieData,data

#Transforma la popularidad de las peliculas
def transform_movieData(data):
    DatosTransformados =[]
    for movie in data:                #Recorre cada pelicula en la lista de peliculas
        max_popularidad = 100
        popularidad = (movie.get("popularity") / max_popularidad) * 10  
        if popularidad > 80.0:          #Si es mayor a 80 es ALTA
            catPopularidad = "ALTA"
        elif popularidad >= 40.0:       #Si es mayor o igual a 40 es MEDIA
            catPopularidad = "MEDIA"
        else:
            catPopularidad = "BAJA"     #Si es menor a 40 es BAJA

#Agrega los datos transformados a la lista de datos transformados
        DatosTransformados.append({
            "id_pelicula": movie.get("id"),
            "titulo": movie.get("title"),
            "fecha_lanzamiento":datetime.strptime(movie.get("release_date"), "%Y-%m-%d"),#Transforma la fecha a datetime
            "idioma_original":movie.get("original_language"),
            "promedio_votos":float(movie.get("vote_average")),
            "recuento_votos":movie.get("vote_count"),
            "popularidad":categoria_popularidad,
            "descripcion_general":movie.get("overview"),
            "id_genero": ','.join(map(str, movie.get("genre_ids", [])))  # Convierte la lista a una cadena
        })
    print(pd.DataFrame(DatosTransformados))
    return DatosTransformados
extract_movieData()                                                  #Extrae los datos de la API
datosTransformados = transform_movieData(data)                       #Transforma los datos
load_SQLServer(datosTransformados,"movies_popularity")               #Carga los datos en la base de datos
df = pd.DataFrame(datosTransformados)                                #Crea un dataframe con los datos transformados
df.to_csv("movies_popularity.csv", index=False)                      #Guarda los datos en un archivo csv


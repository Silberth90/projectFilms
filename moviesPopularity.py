from datetime import datetime
import pandas as pd
from scripts import transform_movieData, load_SQLServer,extract_movieData,data

#Transforma la popularidad de las peliculas
def transform_movieData(data):
    DatosTransformados =[]
    for movie in data:
        max_popularidad = 100
        normalizacion_popularidad = (movie.get("popularity") / max_popularidad) * 10
        if normalizacion_popularidad > 80.0:
            categoria_popularidad = "ALTA"
        elif normalizacion_popularidad >= 40.0:
            categoria_popularidad = "MEDIA"
        else:
            categoria_popularidad = "BAJA"

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
extract_movieData()
datosTransformados = transform_movieData(data)
load_SQLServer(datosTransformados,"movies_popularity")

# **Proyecto de Pipeline de Datos: API a SQL Server**

Este proyecto muestra cómo extraer datos de películas desde la **API de The Movie Database (TMDB)**, transformarlos para clasificar las películas según su popularidad y almacenarlos en una base de datos **SQL Server**.

---

## **Tabla de Contenidos**

- [Descripción General](#descripción-general)
- [Requisitos Previos](#requisitos-previos)
- [Instrucciones de Configuración](#instrucciones-de-configuración)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Cómo Funciona](#cómo-funciona)
- [Mejoras Futuras](#mejoras-futuras)

---

## **Descripción General**

se usaron las siguientes librerias:

- pandas
- requests
- pyodbc
- io
- json
- datetime
- tambien se uso variables de entorno para la api key para que no se exponga la api key en el codigo fuente
  ni otros datos sensibles
- para la conexion a la base de datos se uso pyodbc

### **Propósito**

- **Extraer** se extraen los datos de la API de TMDB.
- **Transformar**los datos de popularidad en categorías: Alta, Media y Baja.
- **Cargar** los datos en SQL Server para su análisis posterior.

### **Características Principales**

1. se van extrayendo las columnas relevantes para luego;
2. almacenar los datos crudos en una tabla llamada `movies` en SQL Server;
3. para finalizar,clasificar las películas según su popularidad y guardar los resultados en una tabla llamada `movies_popularity`.

---

## **Los Requisitos Para Ejecutar El Proyecto**

- Python 3.8 o superior.
- Conexión activa a Internet.
- Credenciales de acceso a la API de TMDB (obtenidas al crear una cuenta en TMDB).
- SQL Server instalado y configurado.
- Biblioteca de Python para conectarse a SQL Server (`pyodbc` o `pymssql`).

---

## **DATOS A EXTRAER **

movie_id
title
release_date
original_language
vote_average
vote_count
popularity
overview
genre_ids

## **TABLA DE POPULARIDAD**

la tabla de popularidad se hizo con las siguientes columnas y se realizo con python:

movie_id
title
popularity_category valores que deberia tener (BAJA, MEDIA, ALTA)

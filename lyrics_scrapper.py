"""Este módulo se encarga de buscar y descargar las letras de las canciones para entrenar el generador.

Este módulo accede a la API de https://genius.com/ para buscar y descargar las líricas seleccionadas para entrenar el
modelo generador.

  Typical usage example:

  get_metadatos_canciones(artists_path="./data/Artistas_IDs.csv")
  get_liricas()

"""
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv, read_pickle
import numpy as np
from os.path import isfile, exists, isdir
import os
from time import sleep
import re

_URL_API = "https://api.genius.com/" # GENIUS API URL
_URL_ARTIST = "artists/"
_ENDPOINT_ARTIST_SONGS = "/songs"
_TOKEN_GENIUS_API = "CATTyVT506krN0vbogZTybp72Ciqg7fFw8Ua9DnXeIsarXB8TX2hGY-EKtZjFcO_"
_NOMBRE_COL_IDS = "ID_CANCION"
_NOMBRE_COL_LYR_URL = "URL_LYRICAS"
_NOMBRE_COL_TIT = "TITULO_CANCION"
_NOMBRE_COL_ART = "ARTISTA"
_NOMBRE_COL_LETRA = "LETRA"
_DF_CANCIONES = "canciones.pkl"
_DIR_DATA = "./data/"
_DIR_LETRAS = "letras_raw/"
_URI_DF_CANCIONES = _DIR_DATA + _DF_CANCIONES
_URI_LETRAS = _DIR_DATA + _DIR_LETRAS
_DF_LETRAS = "letras.pkl"
_URI_DF_LETRAS = _DIR_DATA + _DF_LETRAS

"""Descarga metadatos de canciones de https://genius.com/.

    Usando el id de cada artista. Consigue las URLs de líricas de todas las canciones que guarda en un df 
    junto al nombre del artista, de la canción y el id de ésta. El df es guardado como "canciones.pkl en una carpeta 
    llamada "data".

    Args:
        artist_path: La dirección URI del archivo CSV que contiene las parejas de <artista - ID de artista (En genius)> 

    Returns:
        None.

    Raises:
        RequestException: Un error al recuperar los datos de la API de Genius.
    """
def get_metadatos_canciones(artists_path="./data/Artistas_IDs.csv"):
	art_df = read_csv(artists_path, sep=";")
	auth_headers = {"Authorization": "Bearer " + _TOKEN_GENIUS_API}

	if isfile(_URI_DF_CANCIONES): # Si ya existe la información, se evita crearlo de nuevo
		print("Ya se descargó la metainformación anteriormente. Saltando este paso.")
		return

	ids_canciones = [] # Lista de ids de las canciones
	urls_lyr_can = [] # Lista de URLs de las líricas de las canciones
	titulos = [] # Lista de los titulos de las canciones
	artistas = [] # Lista de los artistas de las canciones
	for _, fila in art_df.iterrows():
		artista = fila["ARTISTA"]
		id = fila["ID"]
		num_canciones = 0
		num_pag = 1
		seguir_buscando = True
		get_songs_string = _URL_API + _URL_ARTIST + str(id) + _ENDPOINT_ARTIST_SONGS

		while num_pag is not None:
			params = {"page" : num_pag}

			try :
				response = requests.get(get_songs_string, headers=auth_headers, params=params)
				if response.status_code == 200:
					canciones = response.json()["response"]["songs"] # Coge la lista de canciones
					num_pag = response.json()["response"]["next_page"] # Coge el id de la siguiente página
				else:
					raise requests.exceptions.RequestException()

			except requests.exceptions.RequestException:
				raise requests.exceptions.RequestException("Ha ocurrido un error al conectar con la API de https//:genius.com")


			for cancion in canciones:
				ids_canciones.append(cancion["id"])
				urls_lyr_can.append(cancion["url"])

				titulo = cancion["full_title"]
				artistas.append(artista)
				titulos.append(titulo)
				num_canciones += 1
		print(f"-------------------------------------------------------\nDescargada la metainformación de las canciones de \"{artista}\" ({num_canciones}).")

	can_df = DataFrame(data=np.array([ids_canciones,urls_lyr_can, titulos, artistas]).T, columns=[_NOMBRE_COL_IDS, _NOMBRE_COL_LYR_URL, _NOMBRE_COL_TIT, _NOMBRE_COL_ART])
	can_df = can_df.groupby([_NOMBRE_COL_IDS], as_index=False).first() # Conseguimos las canciones únicas (por si hay repetidas)

	can_df.to_pickle(_URI_DF_CANCIONES)


"""Descarga letras de canciones de https://genius.com/.

    Usa el DataFrame creado anteriormente con la función 'get_metadatos_canciones'. Por cada artista crea un DataFrame
    que contenga la letra de la canción junto a su ID. Estos Dataframes se guardan en una carpeta 'letras' dentro de 
    la carpeta de datos.

    Args:
        None 

    Returns:
        None.

    Raises:
        RequestException: Un error al recuperar las líricas.
    """
def get_liricas():

	if not exists(_URI_LETRAS):
		print("Creando carpeta con canciones")
		os.mkdir(_URI_LETRAS)

	letras_df = read_pickle(_URI_DF_CANCIONES)
	letras_df.sort_values(by=_NOMBRE_COL_ART)

	artistas = list(letras_df[_NOMBRE_COL_ART].unique())
	artistas.sort()

	for artista in artistas:

		print(f"Se va a proceder a descargar las letras de {artista}")
		letras_error = 0


		_URI_LETRAS_ARTISTA = _URI_LETRAS + artista + ".pkl"

		if exists(_URI_LETRAS_ARTISTA): # Si ya existe el archivo de ese artista, se pasa a la siguiente iteración
			print(f"Ya fueron descargadas las líricas de las canciones de \"{artista}\". Saltando...")
			continue

		canart_df = letras_df[letras_df[_NOMBRE_COL_ART] == artista]
		total_canciones = canart_df.shape[0]

		letras = [] # Lista que contendrá las letras de un artista
		ids_can = [] # Lista que contendrá los ids de las canciones

		n_can = 0
		print(f"{n_can}/{total_canciones} de {artista} descargadas...", end="")
		for _, row in canart_df.iterrows():

			try:
				page = requests.get(row[_NOMBRE_COL_LYR_URL])
			except requests.exceptions.RequestException:
				raise requests.exceptions.RequestException(f"Ha ocurrido un error al descargar la lírica de la canción \"{row[_NOMBRE_COL_TIT]}\"...")

			html = BeautifulSoup(page.text, "html.parser")  # Extract the page's HTML as a string

			# Scrape the song lyrics from the HTML
			lyrics = html.find("div", class_="lyrics")

			# Scrape the song lyrics from the HTML
			lyrics = html.find("div", class_="lyrics")

			if lyrics is None:
				lyrics = html.find_all("div", class_=re.compile("^Lyrics__Container"))
				if lyrics is None:
					letras_error+=1
					# print(f"No se ha podido descargar la cancion {row[_NOMBRE_COL_TIT]} de {row[_NOMBRE_COL_ART]} [{row[_NOMBRE_COL_LYR_URL]}]...")
					continue
				texto = "\n".join([div.get_text() for div in lyrics])
			else:
				texto = lyrics.get_text()

			letras.append(texto)
			ids_can.append(row[_NOMBRE_COL_IDS])
			n_can += 1
			print(f"\r{n_can}/{total_canciones} de {artista} descargadas...", end="")

		print("COMPLETADO")


		# Se guarda el df con todas las letras descargadas
		DataFrame(data=np.array([ids_can, letras]).T, columns=[_NOMBRE_COL_IDS, _NOMBRE_COL_LETRA]).to_pickle(_URI_LETRAS_ARTISTA)

		print(f"-------------------------------------------------------")




if __name__ == '__main__':
	get_metadatos_canciones()
	get_liricas()
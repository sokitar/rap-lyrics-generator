"""Este módulo se encarga de buscar y descargar las letras de las canciones para entrenar el generador.

Este módulo accede a la API de https://genius.com/ para buscar y descargar las líricas seleccionadas para entrenar el
modelo generador.

  Typical usage example:

  scrape_lyrics(path)
"""
import urllib

import urllib3
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv, read_pickle
import numpy as np
from os.path import isfile

_URL_API = "https://api.genius.com/" # GENIUS API URL
_URL_ARTIST = "artists/"
_ENDPOINT_ARTIST_SONGS = "/songs"
_TOKEN_GENIUS_API = "CATTyVT506krN0vbogZTybp72Ciqg7fFw8Ua9DnXeIsarXB8TX2hGY-EKtZjFcO_"
_NOMBRE_COL_IDS = "ID_CANCION"
_NOMBRE_COL_LYR_URL = "URL_LYRICAS"
_NOMBRE_COL_TIT = "TITULO_CANCION"
_NOMBRE_COL_LETRA = "LETRA"
_DF_CANCIONES = "canciones.pkl"
_URI_DF_CANCIONES = "./data/" + _DF_CANCIONES
_DF_LETRAS = "letras.pkl"
_URI_DF_LETRAS = "./data/"+ _DF_LETRAS

"""Descarga metadatos de canciones de https://genius.com/.

    Retrieves rows pertaining to the given keys from the Table instance

    Args:
        table_handle: An open smalltable.Table instance.
        keys: A sequence of strings representing the key of each table
          row to fetch.  String keys will be UTF-8 encoded.
        require_all_keys: Optional; If require_all_keys is True only
          rows with values set for all keys will be returned.

    Returns:
        A dict mapping keys to the corresponding table row data
        fetched. Each row is represented as a tuple of strings. For
        example:

        {b'Serak': ('Rigel VII', 'Preparer'),
         b'Zim': ('Irk', 'Invader'),
         b'Lrrr': ('Omicron Persei 8', 'Emperor')}

        Returned keys are always bytes.  If a key from the keys argument is
        missing from the dictionary, then that row was not found in the
        table (and require_all_keys must have been False).

    Raises:
        IOError: An error occurred accessing the smalltable.
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
				titulos.append(titulo)
				num_canciones += 1
		print(f"-------------------------------------------------------\nDescargada la metainformación de las canciones de \"{artista}\" ({num_canciones}).")

	can_df = DataFrame(data=np.array([ids_canciones,urls_lyr_can, titulos]).T, columns=[_NOMBRE_COL_IDS, _NOMBRE_COL_LYR_URL, _NOMBRE_COL_TIT])
	can_df = can_df.groupby([_NOMBRE_COL_IDS], as_index=False).first() # Conseguimos las canciones únicas (por si hay repetidas)

	can_df.to_pickle(_URI_DF_CANCIONES)

def get_liricas():

	letras_df = read_pickle(_URI_DF_CANCIONES)

	letras_df[_NOMBRE_COL_LETRA] = None

	letras_error = 0
	for _, row in letras_df.iterrows():
		try:

			page = requests.get(row[_NOMBRE_COL_LYR_URL])
		except requests.exceptions.RequestException:
			raise requests.exceptions.RequestException(f"Ha ocurrido un error al descargar la lírica de la canción \"{row[_NOMBRE_COL_TIT]}\"")

		html = BeautifulSoup(page.text, "html.parser")  # Extract the page's HTML as a string
		# Scrape the song lyrics from the HTML


		lyrics = html.find("div", class_="lyrics")

		if lyrics is not None:
			lyrics = lyrics.get_text()
		else:
			letras_error+=1

	total_canciones = letras_df.shape[0]
	letras_desc = total_canciones - letras_error

	print(f"-------------------------------------------------------\nSe han podido descargas las líricas de {letras_desc}/{total_canciones} canciones...")


	letras_df.to_pickle(_URI_DF_LETRAS)

if __name__ == '__main__':
	get_metadatos_canciones()
	get_liricas()
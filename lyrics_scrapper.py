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
from pandas import DataFrame, read_csv

_URL_API = "https://api.genius.com/" # GENIUS API URL
_URL_ARTIST = "artists/"
_ENDPOINT_ARTIST_SONGS = "/songs"
_TOKEN_GENIUS_API = "CATTyVT506krN0vbogZTybp72Ciqg7fFw8Ua9DnXeIsarXB8TX2hGY-EKtZjFcO_"

"""Descarga líricas de https://genius.com/.

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
def scrape_lyrics(artists_path="./data/Artistas_IDs.csv"):
	art_df = read_csv(artists_path, sep=";")
	auth_headers = {"Authorization": "Bearer " + _TOKEN_GENIUS_API}

	for _, fila in art_df.iterrows():
		artista = fila["ARTISTA"]
		id = fila["ID"]
		print(f"-------------------------------------------------------\nSe va a buscar las canciones del artista {artista} ({id}):")
		num_canciones = 1
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
				titulo = cancion["full_title"]
				print(f"{num_canciones}.- {titulo}")
				num_canciones += 1


	pass

if __name__ == '__main__':
	scrape_lyrics()
# This is a BigData Project script 2023
# Authors:          Magdalena Pakula & Aleksandra Banasiak
# Sources of data:  https://developers.soundcloud.com/docs/api/guide
#                   https://developer.spotify.com/documentation/web-api/reference/get-track
#                   https://rapidapi.com/songstats-app-songstats-app-default/api/songstats/

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# importing Spotify library
import spotipy #Spotify web API wrapper for python
from spotipy.oauth2 import SpotifyOAuth #auth
from spotipy.oauth2 import SpotifyClientCredentials #auth
import pandas as pd #dataframe
import time #pause execution of loops

client_id = '5a5cb989d0e147419065969f51e58710'
client_secret = 'b17f47d55ef94b3a934645f43fc1cc55'

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


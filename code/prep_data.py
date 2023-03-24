import pandas as pd
import requests
import numpy as np
import time
import tqdm

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

cid = 'b0d7a8b88bf049fab7ddd19c083b76d9'
secret = '42d7f03fdaba4318ab3994b2921bdae3'
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

spotify2021 = pd.read_csv("../spotify-datasets/spotify-2021.csv", index_col=0)
spotifydataset = pd.read_csv("../spotify-datasets/spotify-tracks-dataset.csv", index_col=0)
spotifydb = pd.read_csv('../spotify-datasets/spotify-tracks-db.csv', index_col=0)

# ACCESS_TOKEN = 'BQBmhAGUqnsDvk63uFA_zXPe6Jx6aTmrDun_n44BsioumbMiPcyerMhtx5AxTayXtwSa5UC5Sz-VBpLDTV0l2lZfTw6Cei_m1xhGZlmNmavwAX7tQzHSgWnoDp58SXY7o7j4LpXPObQwiSkodNhBtC1hQwsl2Vx1RoWjdqclxkoxCbOkWTxvLfQL1Da81KSlUmvXNFngR8v8YoMkKb68jlg4M0abVjPH7Ri8Wh0j4PoNyUxcFrw8FrYBIg6TofowYNaPyUSNGfQqw0YDcsF3nqt72MWnX2xUsLDYpmv1IVIwBm6TutsmsjMKkdbjGxpul59WseeSUQ'
# headers = {
#     'Authorization': 'Bearer {token}'.format(token=ACCESS_TOKEN)
# }

def get_information(track_id):

    try:
        print("getting track")
        data = sp.track(track_id)
    except:
        print("Excepting ... missing track", track_id)
        return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan

    try:
        total_available_markets = len(data['available_markets']) #count number of markets this track is available in
    except:
        print("excepting for total available markets")
        total_available_markets = np.nan
    try:
        release_date = data['album']['release_date']
    except:
        print("excepting for release date")
        release_date = np.nan
    try:
        artist_id = []
        artist_name = ""
        for i, art in enumerate(data['artists']):
            artist_id.append(art['id'])
            artist_name += art['name']+";"

            if i == 0:
                topartist_id = art['id']
                topartist_name = art['name']
    except:
        print("excepting for artis id and name")
        artist_id = np.nan
        artist_name = np.nan
    

    #query for the genre of the top artist id
    try:
        data = sp.artist(topartist_id)
        try:
            genres = data['genres'][0]
        except:
            print("excepting for genre")
            genres = np.nan
    except:
        print("Excepting, missing artist", track_id, topartist_id)
        genres = np.nan

    #get a genre list
    genrelist = ""
    try:
        for artids in artist_id:
            data = sp.artist(artids)
            try:
                genrelist += data['genres'][0]+";"
            except:
                print("excepting for genre for this arts", track_id, artids)
                genrelist = np.nan

    except:
        genrelist=np.nan


    

    #query for the audio features
    try:
        data = sp.audio_features(track_id)

        try:
            key = data[0]['key']
        except:
            print("excepting for key")
            key = np.nan
        try:
            mode = data[0]['mode']
        except:
            print("excepting for mode")
            mode = np.nan
        try:
            time_signature = data[0]['time_signature']
        except:
            print("Excepting for time signature")
            time_signature = np.nan
    except:
        print("Audio features couldn't be extracted for", track_id)
        key, mode, time_signature = np.nan, np.nan, np.nan



    return release_date, total_available_markets, artist_id, artist_name, genres, key, mode, time_signature, topartist_id, topartist_name, genrelist


print("Merging dfs ...")

print(list(spotify2021.columns.intersection(spotifydataset.columns)))
combined_df = pd.merge(spotify2021, spotifydataset, how='outer', on=list(spotify2021.columns.intersection(spotifydataset.columns)))

#Drop certain columns from spotifydb dataframe due to datatype mismatch. repopulate these later
spotifydb = spotifydb.drop(columns=['key', 'mode', 'time_signature'])
spotifydb = spotifydb.astype({
    'duration_ms':'float',
})

# print("combined dtypes", combined_df[list(combined_df.columns.intersection(spotifydb.columns))].dtypes)
# print("spotfy db", spotifydb[list(combined_df.columns.intersection(spotifydb.columns))].dtypes)
# exit(0)



combined_df = pd.merge(combined_df, spotifydb, how='outer', on=list(combined_df.columns.intersection(spotifydb.columns)))
common_columns = list(spotify2021.columns.intersection(spotifydataset.columns).intersection(spotifydb.columns))


mismatchedartist = 0
update_artist = 0

print(combined_df.head())
print(combined_df.columns)
# print(combined_df.iloc[0]['artist_name_x'], combined_df.iloc[0]['artist_name_y'], combined_df.iloc[0]['artist_name'])
# print(combined_df.iloc[0]['popularity_x'], combined_df.iloc[0]['popularity_y'], combined_df.iloc[0]['popularity'])
# exit(0)

print("Lenghts of all dfs")
print("spotify2021", len(spotify2021))
print("spotifydataset", len(spotifydataset))
print("spotifydb", len(spotifydb))

print("\ntotal combined df len", len(combined_df))

#filter required columns
# print("df1", spotify2021.columns)
# print("df2", spotifydataset.columns)
# print("df3", spotifydb.columns)
# print("common columns", common_columns)
final_columns = common_columns + ['key', 'mode', 'time_signature', 'track_genre', 'release_date', 'total_available_markets']
combined_df = combined_df[final_columns]

print("Combined df total columns", len(combined_df.columns))
print()
print("Checking for NaN values in each column")
print(combined_df.isna().sum())
# exit(0)

#iterate and fill missing values for total available markets, release date and genre
for idx, row in tqdm.tqdm(combined_df.iterrows()):

    if (idx+1)%200 == 0:
        print("Sleeping for 30")
        time.sleep(30)

    # print(idx, end='\t')
    release_date, total_available_markets, artist_id, artist_name, genres, key, mode, time_signature, topartist_id, topartist_name, genrelist = get_information(combined_df.loc[idx, 'track_id'])

    if topartist_name.lower() != combined_df.loc[idx, 'artist_name'].lower():
        print("track mismatch for", combined_df.loc[idx, 'track_id'])
        print(artist_name, combined_df.loc[idx,'artist_name'])
        mismatchedartist += 1
        


    combined_df.loc[idx,'artist_name'] = artist_name
    combined_df.loc[idx,'topartist_name'] = topartist_name
    combined_df.loc[idx,'total_available_markets'] = total_available_markets
    combined_df.loc[idx,'release_date'] = release_date
    combined_df.loc[idx,'track_genrelist'] = genrelist
    combined_df.loc[idx,'track_genre'] = genres
    combined_df.loc[idx, 'key'] = key
    combined_df.loc[idx, 'mode'] = mode
    combined_df.loc[idx, 'time_signature'] = time_signature
    # combined_df.loc[idx,'total_available_markets'] = total_available_markets

    



print("Final dfs length after all processing")
print(len(combined_df))

print("Combined df total columns", len(combined_df.columns))
print()
print("Checking for NaN values in each column after allllllllll processing")
print(combined_df.isna().sum())

print("Update artsits", update_artist)
print("Mismatched artsits", mismatchedartist)

combined_df.to_csv('../spotify-datasets/combined_data.csv')
    



    
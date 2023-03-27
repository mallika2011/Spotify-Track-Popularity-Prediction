import pandas as pd
import requests
import numpy as np
import time
import tqdm

cid = ['bf313ecf796a4bfbaaaeb4c26bc8190e','acc8d1ac25f04823986ba4ab423ca9c4','b0d7a8b88bf049fab7ddd19c083b76d9','5f95e8ca95f2449b97be4391cd60c08c', 
       'e094eddbfb6d4bb4895e6968d2c96084', '74ab37b86a3743c29c7ed53c4c0fb7d8']
secret = ['23ab2b06a5e64e9280ad3be6d3c3ca68','9b8631c746194215baabd0d25e7dcdbf','42d7f03fdaba4318ab3994b2921bdae3','55f20211544e4e92bdfa30904301f7e5', 
          '0f6e7ffec8cf48d985582bada6115156', '18286ffffc2846e9a170d14ab8f5509c']
CURR_IDX = 0


spotify2021 = pd.read_csv("../spotify-datasets/spotify-2021.csv", index_col=0)
spotifydataset = pd.read_csv("../spotify-datasets/spotify-tracks-dataset.csv", index_col=0)
spotifydb = pd.read_csv('../spotify-datasets/spotify-tracks-db.csv', index_col=0)


def get_token():
    global ACTIVE_TOKEN

    AUTH_URL = "https://accounts.spotify.com/api/token"

    token_req = requests.post(AUTH_URL, {
        "grant_type": "client_credentials",
        "client_id": cid[CURR_IDX],
        "client_secret": secret[CURR_IDX]
        })

    access_token = token_req.json()["access_token"]
    # print(token_req.json())
    ACTIVE_TOKEN = access_token


track_info = {}
artist_ids_info = {}
def get_track_info(track_ids_list, current_token):
    tracks_url = "https://api.spotify.com/v1/tracks"
    params = {
        'ids':','.join(track_ids_list)
    }
    headers={
    "Authorization": "Bearer {}".format(current_token),
    'Content-Type':'application/json',
    'Accept': "application/json"
    }

    resp = requests.get(tracks_url, params=params,headers=headers)

    # print(len(resp.json()['tracks']))
    # print(resp.json())
    all_tracks = resp.json()['tracks']

    for song in all_tracks:
        if song['id'] in track_info:
            print("Ooooooooooooooooooooooops repeat, fishy")
            continue
            
        try:
            total_available_markets = len(set(song['available_markets']))
        except:
            total_available_markets = np.nan
        try:
            release_date = song['album']['release_date']
        except:
            release_date = np.nan
        
        artist_names = ""
        topartist_id = np.nan
        topartist_name = np.nan
        for i, art in enumerate(song['artists']):
            if art['id'] not in artist_ids_info:
                artist_ids_info[art['id']] = {'artist_name':art['name'], 'artist_genre':np.nan, 'artist_genre_list':np.nan}
            artist_names += art['name']+";"
            if i == 0:
                topartist_id = art['id']
                topartist_name = art['name']

        track_info[song['id']] = {}
        track_info[song['id']]['total_available_markets'] = total_available_markets
        track_info[song['id']]['release_date'] = release_date
        track_info[song['id']]['topartist_id'] = topartist_id
        track_info[song['id']]['topartist_name'] = topartist_name

        if artist_names == "":
            track_info[song['id']]['artist_names'] = np.nan
        else:
            track_info[song['id']]['artist_names'] = artist_names



def get_audio_features_info(track_ids_list, current_token):
    tracks_url = "https://api.spotify.com/v1/audio-features"
    params = {
        'ids':','.join(track_ids_list)
    }
    headers={
    "Authorization": "Bearer {}".format(current_token),
    'Content-Type':'application/json',
    'Accept': "application/json"
    }

    resp = requests.get(tracks_url, params=params,headers=headers)

    # print(len(resp.json()['tracks']))
    # print(resp.json())
    all_audio_features = resp.json()['audio_features']

    for song in all_audio_features:
        if song['id'] not in track_info:
            print("aaaaaaaaaaaaaaaaaaaa missing, fishy")
            continue
        
        try:
            key = song['key']
        except:
            key = np.nan
        try:
            time_signature = song['time_signature']
        except:
            time_signature = np.nan
        try:
            mode = song['mode']
        except:
            mode = np.nan

        track_info[song['id']]['key'] = key
        track_info[song['id']]['mode'] = mode
        track_info[song['id']]['time_signature'] = time_signature


def get_genre_info(artist_ids_list, current_token):
    tracks_url = "https://api.spotify.com/v1/artists"
    params = {
        'ids':','.join(artist_ids_list)
    }
    headers={
    "Authorization": "Bearer {}".format(current_token),
    'Content-Type':'application/json',
    'Accept': "application/json"
    }

    resp = requests.get(tracks_url, params=params,headers=headers)

    # print(len(resp.json()['tracks']))
    # print(resp.json())
    all_artists = resp.json()['artists']

    for artist in all_artists:
        if artist['id'] not in artist_ids_info:
            print("Eeeeeeeeeeeeeeeeeeeeks misssssssing, fishy")
            continue
        
        artist_genre_list = ""
        for i, genre in enumerate(artist['genres']):
            if i == 0:
                artist_ids_info[artist['id']]['artist_genre'] = genre
            artist_genre_list += genre+";"
        
        if artist_genre_list == "":
            artist_ids_info[artist['id']]['artist_genre_list'] = np.nan
        else:
            artist_ids_info[artist['id']]['artist_genre_list'] = artist_genre_list




####################################################
#               MAIN FUNCTIONS AND CODE
####################################################

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

#drop duplicates based on track ID
print("Total number of unique ids", len(pd.unique(combined_df['track_id'])))

combined_df = combined_df.drop_duplicates(subset='track_id', keep="first")
print("\ntotal combined df len after drop duplicates", len(combined_df))

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

#prepare track IDs set
trackidsset = list(set(combined_df['track_id'].tolist()))
print("Full track ids len", len(trackidsset))
# trackidsset = trackidsset[:4] #DEBUG

#GET RID OF THE DFS TO SAVE MEMORY
del combined_df
del spotify2021
del spotifydataset
del spotifydb


#START BY GETTING A TOKEN
get_token()


print("*****************************")
print("Getting Track data")
print("*****************************")

track_chunksize = 50
for idx in range(0,len(trackidsset), track_chunksize):
    target_chunk = trackidsset[idx:idx+track_chunksize]
    print("processing", idx, idx+track_chunksize)
    
    success = False
    while(not success):
        try:
            get_track_info(track_ids_list=target_chunk, current_token=ACTIVE_TOKEN)
            success = True
        except Exception as e:
            print(e)
            print("Sleeping for 300...")
            time.sleep(300)
            CURR_IDX += 1
            get_token()

print("*****************************")
print("Getting Audio features data")
print("*****************************")

audio_feat_chunksize = 100
for idx in range(0,len(trackidsset), audio_feat_chunksize):
    target_chunk = trackidsset[idx:idx+audio_feat_chunksize]
    print("processing", idx, idx+audio_feat_chunksize)

    success = False
    while(not success):
        try:
            get_audio_features_info(track_ids_list=target_chunk, current_token=ACTIVE_TOKEN)
            success = True
        except Exception as e:
            print(e)
            print("Sleeping for 300...")
            time.sleep(300)
            CURR_IDX +=1
            get_token()


print("*****************************")
print("Getting Artist features data")
print("*****************************")

artist_feat_chunksize = 50
artistids_set = list(artist_ids_info.keys())

print("artttt settt")
print(artistids_set)

for idx in range(0,len(artistids_set), artist_feat_chunksize):
    target_chunk = artistids_set[idx:idx+artist_feat_chunksize]
    print("processing", idx, idx+artist_feat_chunksize)

    success = False
    while(not success):
        try:
            get_genre_info(artist_ids_list=target_chunk, current_token=ACTIVE_TOKEN)
            success = True
        except Exception as e:
            print(e)
            print("Sleeping for 300...")
            time.sleep(300)
            CURR_IDX += 1
            get_token()

print("track infooo", len(track_info))
print(track_info)
print()
#save this as a df
df = pd.DataFrame(track_info)
df = df.transpose()
df = df.rename_axis('track_id').reset_index()
print(df.head())
df.to_csv("../spotify-datasets/track_info_scraped.csv")

print("genre infooo", len(artist_ids_info))
print(artist_ids_info)
print()
#save this as a df
df = pd.DataFrame(artist_ids_info)
df = df.transpose()
df = df.rename_axis('topartist_id').reset_index()
print(df.head())
df.to_csv("../spotify-datasets/genre_artist_ids_info_scraped.csv")
import pandas as pd
import requests
import numpy as np
import time
import tqdm




spotify2021 = pd.read_csv("../spotify-datasets/spotify-2021.csv", index_col=0)
spotifydataset = pd.read_csv("../spotify-datasets/spotify-tracks-dataset.csv", index_col=0)
spotifydb = pd.read_csv('../spotify-datasets/spotify-tracks-db.csv', index_col=0)


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

combined_df = pd.merge(combined_df, spotifydb, how='outer', on=list(combined_df.columns.intersection(spotifydb.columns)))
common_columns = list(spotify2021.columns.intersection(spotifydataset.columns).intersection(spotifydb.columns))

mismatchedartist = 0
update_artist = 0

print(combined_df.head())
print(combined_df.columns)

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
print("df1", spotify2021.columns)
print("df2", spotifydataset.columns)
print("df3", spotifydb.columns)
print("common columns", common_columns)
combined_df = combined_df[common_columns]
combined_df.drop('artist_name', axis=1, inplace=True)

print("Combined df total columns", len(combined_df.columns))
print()
print("Checking for NaN values in each column")
print(combined_df.isna().sum())

print()
print("Merging with Spotify API scraped dataframes")

tracks_info = pd.read_csv('../spotify-datasets/track_info_scraped.csv', index_col=0)
genre_artist_info = pd.read_csv('../spotify-datasets/genre_artist_ids_info_scraped.csv', index_col=0)

print(tracks_info.columns)
print(genre_artist_info.columns)

genre_artist_info = genre_artist_info.rename({'artist_name': 'topartist_name'}, axis=1)
combined_df = combined_df.rename({'artist_name': 'topartist_name', 'track_genre':'artist_genre'}, axis=1)


api_df = pd.merge(tracks_info, genre_artist_info, how='outer', on=['topartist_id', 'topartist_name'])
print("len api df", len(api_df))

final_df = pd.merge(combined_df, api_df, how='left', on='track_id')
print(final_df.head())

print()
print("***********************")
print("Lennn final df", len(final_df))
print("nulls")
print(final_df.isna().sum())

apidftracks = list(api_df['track_id'])
combined_dftracks = list(combined_df['track_id'])

print("intersection tracks ", len(set(apidftracks).intersection(set(combined_dftracks))))

final_df.to_csv("../spotify-datasets/final_spotify_data.csv")




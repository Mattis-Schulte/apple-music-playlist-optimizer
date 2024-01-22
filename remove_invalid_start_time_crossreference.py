import pandas as pd
import sqlite3


# Read the sqlite3 database
conn = sqlite3.connect('identified_songs.sqlite3')
matched_songs = pd.read_sql('SELECT * FROM crossreference', conn)

print(f'Number of matches: {matched_songs["Track Identifier"].notna().sum()}, Number of unmatched songs: {matched_songs["Track Identifier"].isna().sum()}')

# Convert 'Event Start Timestamp' to datetime
matched_songs['Event Start Timestamp'] = pd.to_datetime(matched_songs['Event Start Timestamp'], format="ISO8601")

# Filter the dataframe to only include songs from 2015 to 2024
matched_songs = matched_songs[matched_songs['Event Start Timestamp'] >= pd.to_datetime('2015-01-01')]
matched_songs = matched_songs[matched_songs['Event Start Timestamp'] < pd.to_datetime('2024-01-01')]

print(f'Number of matches: {matched_songs["Track Identifier"].notna().sum()}, Number of unmatched songs: {matched_songs["Track Identifier"].isna().sum()}')

# Return the 'matched_songs' DataFrame sorted by 'Event Start Timestamp'
matched_songs.sort_values(by='Event Start Timestamp').to_sql('crossreference', conn, if_exists='replace', index=False)
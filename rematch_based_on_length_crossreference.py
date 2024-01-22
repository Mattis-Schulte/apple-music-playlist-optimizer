import pandas as pd
import sqlite3


def rematch_based_on_length(row: pd.Series, df: pd.DataFrame) -> pd.Series:
    """
    Rematch songs based on the song name and the media duration in milliseconds.
    """
    # If the Track Identifier is NaN
    if pd.isna(row['Track Identifier']):
        # Find matches based on Song Name and Media Duration In Milliseconds
        matches = df[(df['Song Name'].str.contains(row['Song Name'], regex=False, case=False)) & 
             (abs(df['Media Duration In Milliseconds'] - row['Media Duration In Milliseconds']) <= 5) & 
             (~pd.isna(df['Track Identifier']))]
        # If there are any matches
        if not matches.empty:
            # Update the row with the details of the first match
            row['Track Identifier'] = matches.iloc[0]['Track Identifier']
            row['Song Name'] = matches.iloc[0]['Song Name']
            print(f'Rematched {row["Song Name"]} ({row["Event Start Timestamp"]}) based on length')

    return row


if __name__ == '__main__':
    matched_songs = pd.read_sql('SELECT * FROM crossreference', sqlite3.connect('identified_songs.sqlite3'))
    print(f'Number of matches: {matched_songs["Track Identifier"].notna().sum()}, Number of unmatched songs: {matched_songs["Track Identifier"].isna().sum()}')
    matched_songs = matched_songs.apply(rematch_based_on_length, axis=1, df=matched_songs)
    print(f'Number of matches: {matched_songs["Track Identifier"].notna().sum()}, Number of unmatched songs: {matched_songs["Track Identifier"].isna().sum()}')
    matched_songs['Event Start Timestamp'] = pd.to_datetime(matched_songs['Event Start Timestamp'], format='ISO8601').dt.tz_localize(None)
    # Save the 'matched_songs' DataFrame to a sqlite3 database sorted by 'Event Start Timestamp'
    matched_songs.sort_values(by='Event Start Timestamp').to_sql('crossreference', sqlite3.connect('identified_songs.sqlite3'), if_exists='replace', index=False)
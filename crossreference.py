import os
import pandas as pd
import pickle
import sqlite3


def read_data(filename: str) -> pd.DataFrame:
    """
    Read the data from the csv file and return it as a DataFrame.
    """
    # Strip file extension
    pickle_filename = os.path.splitext(filename)[0] + '.pkl'
    # Check if pickle file exists
    if os.path.exists(pickle_filename):
        with open(pickle_filename, 'rb') as f:
            return pickle.load(f)

    try:
        df = pd.read_csv(filename, on_bad_lines='error', encoding='utf-8', engine='python')
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    except (TypeError, ValueError):
        df = pd.read_csv(filename, sep=';', on_bad_lines='warn', encoding='utf-8', engine='python')
        
    # Save DataFrame to pickle file
    with open(pickle_filename, 'wb') as f:
        pickle.dump(df, f)    
        
    return df

def crossreference(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Crossreference the two DataFrames by matching the song name and the event timestamp.
    """
    # Convert datetime in df1 and make sure it is timezone-naive
    df1['Event Start Timestamp'] = pd.to_datetime(df1['Event Start Timestamp'], format='ISO8601').dt.tz_localize(None)

    # Convert date and hours in df2 to datetime
    df2['Date Played'] = pd.to_datetime(df2['Date Played'], format='%Y%m%d')
    df2 = df2.assign(Hours=df2['Hours'].str.split(',')).explode('Hours')
    df2['Hours'] = df2['Hours'].astype(int)

    # Create a datetime column and make sure it is timezone-naive
    df2['Event Timestamp'] = pd.to_datetime(df2['Date Played']) + pd.to_timedelta(df2['Hours'], unit='h')

    # Drop rows with null values in specified columns
    df1.dropna(subset=['Event Start Timestamp'], inplace=True)
    df2.dropna(subset=['Event Timestamp', 'Track Description'], inplace=True)

    # Select necessary columns
    df1 = df1.loc[:, ['Event Start Timestamp', 'Song Name', 'Play Duration Milliseconds', 'Media Duration In Milliseconds']]
    df2 = df2.loc[:, ['Event Timestamp', 'Track Description', 'Track Identifier']]

    # Drop all rows that contain any zero values in df1
    df1.replace(0, pd.NA, inplace=True)
    df1.dropna(inplace=True)

    def find_closest_match(row: pd.Series, df: pd.DataFrame) -> pd.Series:
        if pd.isna(row['Track Identifier']):
            # Create a mask to filter df based on the condition
            mask = (
                df['Event Timestamp'].between(row['Event Start Timestamp'] - pd.Timedelta(hours=2), row['Event Start Timestamp'] + pd.Timedelta(hours=2)) &
                df['Track Description'].str.contains(row['Song Name'], case=False, regex=False, na=False)
            )
            # Apply the mask to df
            matches = df[mask]

            # If there are any matches
            if not matches.empty:
                # Find the closest match based on the 'Event Timestamp'
                closest_match = matches[abs(matches['Event Timestamp'] - row['Event Start Timestamp']) == abs(matches['Event Timestamp'] - row['Event Start Timestamp']).min()].iloc[0]
                # Update the row with the details of the closest match
                row['Song Name'] = closest_match['Track Description']
                row['Track Identifier'] = closest_match['Track Identifier']
                print(f'Matched {row["Song Name"]} ({row["Event Start Timestamp"]}) based on time')
        
        return row

    # Apply the find_closest_match function to the df1 DataFrame
    matched_songs = df1.apply(find_closest_match, axis=1, df=df2)

    def rematch_based_on_length(row: pd.Series, df: pd.DataFrame) -> pd.Series:
        """
        Rematch songs based on the song name and the media duration in milliseconds.
        """
        # If the 'Track Identifier' is NaN
        if pd.isna(row['Track Identifier']):
            # Find matches based on 'Song Name' and 'Media Duration In Milliseconds'
            matches = df[(df['Song Name'].str.contains(row['Song Name'], regex=False, case=False)) & (df['Media Duration In Milliseconds'] == row['Media Duration In Milliseconds']) & (~pd.isna(df['Track Identifier']))]
            # If there are any matches
            if not matches.empty:
                # Update the row with the details of the first match
                row['Track Identifier'] = matches.iloc[0]['Track Identifier']
                row['Song Name'] = matches.iloc[0]['Song Name']
                print(f'Rematched {row["Song Name"]} ({row["Event Start Timestamp"]}) based on length')
        
        return row

    # Apply the rematch_based_on_length function to the 'matched_songs' DataFrame
    matched_songs = matched_songs.apply(rematch_based_on_length, axis=1, df=matched_songs)
    print(f'Number of matches: {matched_songs["Track Identifier"].notna().sum()}, Number of unmatched songs: {matched_songs["Track Identifier"].isna().sum()}')
    # Return the 'matched_songs' DataFrame sorted by 'Event Start Timestamp'
    return matched_songs.sort_values(by='Event Start Timestamp')

if __name__ == '__main__':
    play_activity_path = read_data(r'<your_play_activity_path>')
    play_history_daily_path = read_data(r'<your_play_history_daily_path>')
    # Save the 'crossreference' DataFrame to a sqlite3 database
    crossreference(play_activity_path, play_history_daily_path).to_sql('crossreference', sqlite3.connect('identified_songs.sqlite3'), if_exists='replace', index=False)

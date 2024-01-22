import os
import pandas as pd
import pickle


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


def calculate_playtime(df: pd.DataFrame):
    """
    Calculate the total playtime in minutes for each year and all years combined.
    """
    df = df[['Event Start Timestamp', 'Play Duration Milliseconds']].dropna()
    df = df[(df['Play Duration Milliseconds'].between(0, 5*60*60*1000)) & (pd.to_datetime(df['Event Start Timestamp'], format='ISO8601').dt.year >= 2015)]
    print((df.groupby(pd.to_datetime(df['Event Start Timestamp'], format='ISO8601').dt.year)['Play Duration Milliseconds'].sum() / 60000).round(2))
    print(f'Total Play Duration in Minutes: {(df["Play Duration Milliseconds"].sum() / 1000 / 60).round(2)}')


if __name__ == '__main__':
    calculate_playtime(read_data(r'<your_play_acitivity_path>'))

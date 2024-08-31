from tqdm import tqdm
import numpy as np
import pandas as pd
import sqlite3
import tempfile
import os
import subprocess
import networkx as nx
from collections import defaultdict


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the data by removing streams that are too short, have no track identifier or have been played for less than 5 minutes in total.
    """
    df['Event Start Timestamp'] = pd.to_datetime(df['Event Start Timestamp'], format='ISO8601').dt.tz_localize(None)
    print(f'Number of songs: {len(df)}, unique: {len(df["Track Identifier"].unique())}, starting preprocessing...')

    # Add the streams that belong to the same song session together
    df['Time Difference Milliseconds'] = df['Event Start Timestamp'].diff().dt.total_seconds() * 1000
    df['Is Same Song Session'] = df['Track Identifier'].eq(df['Track Identifier'].shift()) & (df['Time Difference Milliseconds'] <= 180000)
    df['Play Duration Milliseconds'] = df.groupby((~df['Is Same Song Session']).cumsum())['Play Duration Milliseconds'].transform('sum')
    df = df[~df['Is Same Song Session']].drop(columns=['Time Difference Milliseconds', 'Is Same Song Session'])

    # Remove streams which are too short, have no track identifier, have been played for less than 5 minutes in total or only once in total
    df = df[
        (df['Play Duration Milliseconds'] > 25000) &
        (df['Track Identifier'].notna()) &
        (df.groupby('Song Name')['Play Duration Milliseconds'].transform('sum') > 300000) &
        (df.groupby('Song Name')['Song Name'].transform('count') > 1)
    ]

    # Create a temporary song name column for case-insensitive matching
    df['Temp Song Name'] = df['Song Name'].str.lower()

    print(f'Number of songs: {len(df)}, unique: {len(df["Track Identifier"].unique())}, finished preprocessing.')
    return df


def graph_data(df: pd.DataFrame) -> (list, np.ndarray):
    """
    Create a distance matrix based on the distance between songs in the data, considering the number of transitions between songs as the weight of the edge.
    """
    print(f'Creating distance matrix...')
    songs = df['Temp Song Name'].unique()
    song_indices = {song: index for index, song in enumerate(songs)}
    num_songs = len(songs)

    G = nx.Graph()
    transition_counts = defaultdict(lambda: defaultdict(int))

    # Create pairs for transitions
    song_pairs = list(zip(df['Temp Song Name'], df['Temp Song Name'].shift(-1)))
    for song1, song2 in song_pairs:
        if pd.isna(song1) or pd.isna(song2):
            continue
        transition_counts[song1][song2] += 1
    
    # Find the maximum transition count for normalization
    max_transition_count = max(max(transition_counts[song1].values()) for song1 in transition_counts)
    
    # Add edges to the graph with normalized weights
    for song1 in transition_counts:
        for song2 in transition_counts[song1]:
            if song1 != song2:
                count = transition_counts[song1][song2]
                normalized_distance = (max_transition_count - count + 1)
                G.add_edge(song1, song2, weight=normalized_distance)

    # Initialize a distance matrix with high distance
    HIGH_DISTANCE = float('inf')
    distance_matrix = np.full((num_songs, num_songs), HIGH_DISTANCE)
    
    # Compute shortest paths using Dijkstra's algorithm for weighted graphs
    for i, song in enumerate(tqdm(songs, desc="Computing distance matrix")):
        lengths = nx.single_source_dijkstra_path_length(G, song, weight='weight')
        song_index = song_indices[song]
        
        for target_song, distance in lengths.items():
            target_index = song_indices[target_song]
            distance_matrix[song_index][target_index] = distance
    
    # Set self-distances to zero (diagonal of the matrix)
    np.fill_diagonal(distance_matrix, 0)

    print(f'Created distance matrix with {num_songs} songs.')
    return songs, distance_matrix


def write_tsplib_file(songs: list, distance_matrix: np.ndarray) -> str:
    """
    Write the distance matrix to a TSPLIB file for the LKH solver.
    """
    num_songs = len(songs)
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.tsp') as f:
        f.write("NAME: song_tsp\n")
        f.write("TYPE: TSP\n")
        f.write(f"DIMENSION: {num_songs}\n")
        f.write("EDGE_WEIGHT_TYPE: EXPLICIT\n")
        f.write("EDGE_WEIGHT_FORMAT: FULL_MATRIX\n")
        f.write("EDGE_WEIGHT_SECTION\n")
        f.writelines(" ".join(map(str, row)) + "\n" for row in distance_matrix)
        f.write("EOF\n")
        return f.name


def write_lkh_par_file(tsp_filename: str) -> str:
    """
    Write the parameter file for the LKH solver.
    """
    par_content = f"""
PROBLEM_FILE = {tsp_filename}
MOVE_TYPE = 5
PATCHING_C = 3
PATCHING_A = 2
RUNS = 1
TRACE_LEVEL = 2
TOUR_FILE = {tsp_filename}.tour
    """
    par_filename = tsp_filename + '.par'
    with open(par_filename, 'w') as f:
        f.write(par_content)
    return par_filename


def find_path(songs: list, distance_matrix: np.ndarray) -> list:
    """
    Find the optimal path using the LKH solver.
    """
    tsp_file_path = write_tsplib_file(songs, distance_matrix)
    par_file_path = write_lkh_par_file(tsp_file_path)
    print(f'Created TSPLIB file at {tsp_file_path} and LKH parameter file at {par_file_path}.\nRunning LKH solver...')
    
    # Running LKH solver
    subprocess.run(['/usr/local/bin/LKH', par_file_path], check=True)

    # Parse the LKH solution
    tour_file = tsp_file_path + '.tour'
    solution = []
    with open(tour_file) as f:
        lines = f.readlines()
        start_index = lines.index('TOUR_SECTION\n') + 1
        for line in lines[start_index:]:
            if line.strip() == '-1':
                break
            solution.append(int(line.strip()) - 1)  # LKH uses 1-based indexing

    # Clean up temporary files
    os.remove(tsp_file_path)
    os.remove(par_file_path)
    os.remove(tour_file)

    path = [songs[idx] for idx in solution]
    print(f'Found optimal path with {len(path)} songs.')
    return path


def export_path(df: pd.DataFrame, path: list, export_path: str):
    """
    Export the calculated path to a sqlite3 database while also retaining the Track Identifier and Media Duration In Milliseconds columns.
    """
    print(f'Exporting optimal path to {export_path}...')
    path_df = pd.DataFrame(path, columns=['Temp Song Name'])
    merged_df = path_df.merge(
        df[['Temp Song Name', 'Song Name', 'Track Identifier', 'Media Duration In Milliseconds']], 
        on='Temp Song Name', 
        how='left'
    ).drop_duplicates(subset='Temp Song Name')
    merged_df = merged_df.drop(columns=['Temp Song Name'])
    with sqlite3.connect(export_path) as conn:
        merged_df.to_sql('exported_path', conn, if_exists='replace', index=False)


if __name__ == '__main__':
    df_preprocessed = preprocess_data(df=pd.read_sql('SELECT * FROM crossreference', sqlite3.connect('identified_songs.sqlite3')))
    songs, distance_matrix = graph_data(df_preprocessed)
    optimal_path = find_path(songs, distance_matrix)
    export_path(df_preprocessed, optimal_path, 'calculated_path_tkh.sqlite3')

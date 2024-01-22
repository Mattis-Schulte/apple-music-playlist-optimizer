import networkx as nx
import pandas as pd
import random
import sqlite3


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

    # Remove streams which are too short, have no track identifier or have been played for less than 5 minutes in total
    df = df[
        (df['Play Duration Milliseconds'] > 25000) &
        (df['Track Identifier'].notna()) &
        (df.groupby('Song Name')['Play Duration Milliseconds'].transform('sum') > 300000)
    ]

    print(f'Number of songs: {len(df)}, unique: {len(df["Track Identifier"].unique())}, finished preprocessing.')
    return df


def graph_data(df: pd.DataFrame) -> nx.DiGraph:
    """
    Create a directed graph from the data, where each node represents a song and each edge represents a transition from one song to another.
    """
    G = nx.DiGraph()
    songs = df['Song Name'].tolist()
    
    # Loop through the songs, creating an edge between each song and the next one
    # If the edge already exists, increase its weight by 1
    for i, j in zip(songs, songs[1:]):
        G.add_edge(i, j, weight=G.get_edge_data(i, j, {'weight': 0})['weight'] + 1)
    
    return G


def find_best_path(G: nx.DiGraph, num_attempts: int = 50) -> list:
    """
    Find the best path in the graph using a greedy algorithm and jump to the next unvisited node with the highest out-degree if necessary.
    """
    def find_path_from_node(G: nx.DiGraph, start_node: str) -> tuple:
        path = [start_node]
        visited = set(path)
        current_node = start_node
        total_weight = 0

        while len(visited) < len(G.nodes()):
            # Find the heaviest unvisited neighbor
            neighbors = [(neighbor, G[current_node][neighbor]['weight'])
                         for neighbor in G.neighbors(current_node)
                         if neighbor not in visited]
            if neighbors:
                next_node, weight = max(neighbors, key=lambda x: x[1])
                total_weight += weight
            else:
                # If there are no unvisited neighbors, jump to the next unvisited
                # node with the highest out-degree that's not visited yet
                remaining_nodes = [node for node in sorted_nodes if node not in visited]
                if not remaining_nodes:
                    break  # No more nodes to visit
                next_node = remaining_nodes[0]
                # Weight for jumps is considered zero or average weight
                total_weight += avg_weight

            # Visit the next node
            path.append(next_node)
            visited.add(next_node)
            current_node = next_node

        return path, total_weight

    # Pre-calculate some graph properties
    sorted_nodes = sorted(G.nodes(), key=lambda n: G.out_degree(n, weight='weight'), reverse=True)
    avg_weight = sum(data['weight'] for _, _, data in G.edges(data=True)) / G.number_of_edges()

    # Run the find_path_from_node function num_attempts times
    best_path = []
    best_weight = -1
    for _ in range(num_attempts):
        # Start from a random node for each attempt
        start_node = random.choice(list(G.nodes()))
        path, total_weight = find_path_from_node(G, start_node)
        print(f'Found path with length {len(path)} and weight {total_weight}...')
        if total_weight > best_weight or (total_weight == best_weight and len(path) > len(best_path)):
            best_path = path
            best_weight = total_weight
    
    print(f'Chose best path with length {len(best_path)} and weight {best_weight}.')
    return best_path


def export_path(df: pd.DataFrame, path: list, export_path: str):
    """
    Export the calculated path to a sqlite3 database while also retaining the Track Identifier and Media Duration In Milliseconds columns.
    """
    path_df = pd.DataFrame(path, columns=['Song Name'])
    merged_df = path_df.merge(df[['Song Name', 'Track Identifier', 'Media Duration In Milliseconds']], on='Song Name', how='left').drop_duplicates(subset='Song Name')
    with sqlite3.connect(export_path) as conn:
        merged_df.to_sql('exported_path', conn, if_exists='replace', index=False)
    print(f'Data successfully exported to {export_path}.')


if __name__ == '__main__':
    df = preprocess_data(pd.read_sql('SELECT * FROM crossreference', sqlite3.connect('identified_songs.sqlite3')))
    G = graph_data(df)
    path = find_best_path(G)
    export_path(df, path, 'calculated_path.sqlite3')

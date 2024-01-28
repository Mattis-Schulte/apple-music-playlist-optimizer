from collections import Counter
import csv
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

    # Remove streams which are too short, have no track identifier, have been played for less than 5 minutes in total or only once in total
    df = df[
        (df['Play Duration Milliseconds'] > 25000) &
        (df['Track Identifier'].notna()) &
        (df.groupby('Song Name')['Play Duration Milliseconds'].transform('sum') > 300000) &
        (df.groupby('Song Name')['Song Name'].transform('count') > 1)
    ]

    print(f'Number of songs: {len(df)}, unique: {len(df["Track Identifier"].unique())}, finished preprocessing.')
    return df


def graph_data(df: pd.DataFrame) -> nx.Graph:
    """
    Create a directed graph from the data, where each node represents a song and each edge represents a transition from one song to another.
    """
    G = nx.Graph()
    songs = df['Song Name'].tolist()
    
    # Loop through the songs, creating an edge between each song and the next one
    # If the edge already exists, increase its weight by 1 plus some random fuzz
    for i, j in zip(songs, songs[1:]):
        G.add_edge(i, j, weight=G.get_edge_data(i, j, {'weight': 0})['weight'] + random.uniform(0.95, 1.05))
    
    print(f'Created graph with {len(G.nodes())} nodes and {len(G.edges())} edges.')
    return G


def find_path(G: nx.Graph, start_node: str) -> list:
    path = [start_node]
    visited = set(path)

    # Find the most common weight in the graph
    weights = Counter(data['weight'] for u, v, data in G.edges(data=True))
    mode_weight = weights.most_common(1)[0][0]

    # Precompute the shortest paths between each node
    print('Precomputing shortest paths...') 
    shortest_paths = dict(nx.all_pairs_shortest_path_length(G))

    current_node = start_node
    total_weight, total_jumps = 0, 0

    print('Finding optimal path...')
    while len(visited) < len(G.nodes()):
        # Find the heaviest unvisited neighbor
        neighbors = [(neighbor, G[current_node][neighbor]['weight'])
                        for neighbor in G.neighbors(current_node)
                        if neighbor not in visited]
        if neighbors:
            next_node, weight = max(neighbors, key=lambda x: x[1])
            total_weight += weight
        else:
            # Find the the closest unvisited node
            neighbors = [(node, shortest_paths[current_node][node])
                            for node in G.nodes()
                            if node not in visited]
            next_node, path_length = min(neighbors, key=lambda x: (x[1], -G.degree(x[0], weight='weight')))
            # Add the average weight of the graph in case of a jump
            total_weight += mode_weight
            total_jumps += 1

        # Visit the next node
        path.append(next_node)
        visited.add(next_node)
        current_node = next_node

    print(f'Found path with {len(path)} songs, {total_jumps} jumps and a total weight of {total_weight}.')
    return path


def export_graph(G: nx.Graph, export_path: str):
    """
    Export the graph to a csv file. This csv file can be imported into Cosmograph for visualization.
    See: https://cosmograph.app/
    """
    print(f'Exporting graph to {export_path}...')
    with open(export_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['source', 'target', 'value'])
        for source, target, data in G.edges(data=True):
            writer.writerow([source, target, data["weight"]])
    

def export_path(df: pd.DataFrame, path: list, export_path: str):
    """
    Export the calculated path to a sqlite3 database while also retaining the Track Identifier and Media Duration In Milliseconds columns.
    """
    print(f'Exporting optimal path to {export_path}...')
    path_df = pd.DataFrame(path, columns=['Song Name'])
    merged_df = path_df.merge(df[['Song Name', 'Track Identifier', 'Media Duration In Milliseconds']], on='Song Name', how='left').drop_duplicates(subset='Song Name')
    with sqlite3.connect(export_path) as conn:
        merged_df.to_sql('exported_path', conn, if_exists='replace', index=False)


if __name__ == '__main__':
    df = preprocess_data(df=pd.read_sql('SELECT * FROM crossreference', sqlite3.connect('identified_songs.sqlite3')))
    G = graph_data(df=df)
    # export_graph(G=G, export_path='graph.csv') # Uncomment this line to export the graph as a csv file for visualization purposes in Cosmograph
    path = find_path(G=G, start_node=random.choice(list(G.nodes())))
    export_path(df=df, path=path, export_path='calculated_path.sqlite3')

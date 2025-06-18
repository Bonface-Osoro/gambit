import configparser
import os
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
from glob import glob
from tqdm import tqdm
from scipy.spatial import cKDTree
from shapely.geometry import Point, LineString
from concurrent.futures import ProcessPoolExecutor, as_completed
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')

CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), 'script_config.ini'))
BASE_PATH = CONFIG['file_locations']['base_path']


DATA_PROCESSED = os.path.join(BASE_PATH, '..', 'results', 'processed')
DATA_RESULTS = os.path.join(BASE_PATH, '..', 'results', 'final')


def get_nearest_graph_node(coord, node_list):
    
    node_array = np.array(node_list)
    tree = cKDTree(node_array)
    _, idx = tree.query(coord)
    
    return tuple(node_array[idx])


def run_pcst_from_shapefiles(road_shapefile, population_shapefile, 
                             output_folder, file_id):
    
    try:
    
        roads = gpd.read_file(road_shapefile)
        population_nodes = gpd.read_file(population_shapefile)

        roads = roads.to_crs(epsg=3857)
        population_nodes = population_nodes.to_crs(epsg=3857)

        G = nx.Graph()
        for _, row in roads.iterrows():
            if not isinstance(row.geometry, LineString):
                continue
            coords = list(row.geometry.coords)
            for i in range(len(coords) - 1):
                u = coords[i]
                v = coords[i+1]
                dist = Point(u).distance(Point(v))
                G.add_edge(u, v, weight=dist, geometry=LineString([u, v]))

        graph_nodes = list(G.nodes)

        prize_nodes = {}
        node_coords = {}
        for _, row in population_nodes.iterrows():
            point = row.geometry
            original_coord = (point.x, point.y)
            nearest_coord = get_nearest_graph_node(original_coord, graph_nodes)
            population = row['population']
            prize_nodes[original_coord] = population 
            node_coords[original_coord] = row

            if original_coord != nearest_coord:
                dist = Point(original_coord).distance(Point(nearest_coord))
                G.add_edge(original_coord, nearest_coord, weight=dist, geometry=LineString([original_coord, nearest_coord]))
                graph_nodes.append(original_coord)

        if not prize_nodes:
            raise ValueError("No population nodes matched to the road network.")

        root = max(prize_nodes, key=prize_nodes.get)

        T = nx.Graph()
        T.add_node(root)
        included = {root}

        while True:
            best_score = float('-inf')
            best_path = None
            best_target = None
            candidates = 0

            for target in prize_nodes:
                if target in included:
                    continue
                if not G.has_node(target):
                    continue
                try:
                    path = nx.shortest_path(G, source=root, target=target, weight='weight')
                    cost = sum(G[u][v]['weight'] for u, v in zip(path[:-1], path[1:]))
                    score = prize_nodes[target] * 1000000 - cost
                    candidates += 1
                    if score > best_score:
                        best_score = score
                        best_path = path
                        best_target = target
                except nx.NetworkXNoPath:
                    continue

            if candidates == 0 or best_score <= 0 or best_path is None:
                break

            for u, v in zip(best_path[:-1], best_path[1:]):
                edge_data = G.get_edge_data(u, v, default={})
                if 'geometry' in edge_data:
                    T.add_edge(u, v, **edge_data)
                included.add(u)
                included.add(v)

        selected_edges = []
        for u, v, data in T.edges(data=True):
            geom = data.get('geometry')
            if geom:
                selected_edges.append({'geometry': geom, 'weight': data.get('weight', 0)})

        if not selected_edges:
            raise ValueError("No road segments with geometry were selected.")

        selected_roads = gpd.GeoDataFrame(selected_edges, geometry='geometry', crs=roads.crs)

        selected_nodes = []
        for coord in included:
            if coord in node_coords:
                selected_nodes.append(node_coords[coord])
        selected_population_nodes = gpd.GeoDataFrame(selected_nodes, crs=population_nodes.crs)

        road_key = os.path.basename(road_shapefile).lower()

        basename = os.path.splitext(os.path.basename(road_shapefile))[0]  # e.g. "SLE.1.1.11_1"
        region_code = basename.split('_')[0]  # "SLE.1.1.11"
        dot_count = region_code.count('.')  # Count dots

        if dot_count == 3:
            folder_suffix = 'sub_regions'
        elif dot_count == 2:
            folder_suffix = 'regions'
        else:
            folder_suffix = 'other'

        # ✅ Create output folders
        edge_folder = os.path.join(output_folder, "edges", folder_suffix)
        node_folder = os.path.join(output_folder, "nodes", folder_suffix)
        os.makedirs(edge_folder, exist_ok=True)
        os.makedirs(node_folder, exist_ok=True)

        selected_roads.to_file(os.path.join(edge_folder, f'{file_id}_solution.shp'))
        selected_population_nodes.to_file(os.path.join(node_folder, f'{file_id}_solution_nodes.shp'))


    except Exception as e:

        return f"❌ {file_id}: {e}"


    return selected_roads, selected_population_nodes


def batch_pcst_parallel(roads_folder, population_folder, output_folder, max_workers=None):
    road_files = {os.path.basename(f): f for f in glob(os.path.join(roads_folder, '*.shp'))}
    pop_files = {os.path.basename(f): f for f in glob(os.path.join(population_folder, '*.shp'))}

    matching_files = sorted(set(road_files.keys()) & set(pop_files.keys()))
    if not matching_files:
        print("⚠️ No matching shapefiles found.")
        return

    # Extract ISO3 from any matching file (assuming consistent naming)
    iso3 = os.path.splitext(matching_files[0])[0][:3] if matching_files else "ISO"

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                run_pcst_from_shapefiles,
                road_files[file_name],
                pop_files[file_name],
                output_folder,
                os.path.splitext(file_name)[0]
            ): file_name
            for file_name in matching_files
        }

        # Show ISO3 in progress bar
        for future in tqdm(as_completed(future_to_file), total = 
            len(future_to_file), desc = 
            f"Finding the PCST least cost path between population {iso3} nodes"):
            try:
                future.result()
            except Exception as e:
                pass
import configparser
import fiona
import json
import os
import rasterio
import shapely
import geopandas as gpd
import osmnx as ox
import pandas as pd
from rasterio.mask import mask
from rasterstats import zonal_stats
from shapely.geometry import MultiPolygon, mapping, shape, MultiLineString
from tqdm import tqdm

CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), 'script_config.ini'))
BASE_PATH = CONFIG['file_locations']['base_path']
DATA_PROCESSED = os.path.join(BASE_PATH, '..', 'results', 'processed')
DATA_RAW = os.path.join(BASE_PATH, 'raw')

#### setup all the required folders ####
if not os.path.exists(DATA_RAW):
    
    os.makedirs(DATA_RAW)

boundary_folder = os.path.join(DATA_RAW, 'boundaries')

if not os.path.exists(boundary_folder):
    
    os.makedirs(boundary_folder)

pop_folder = os.path.join(DATA_RAW, 'WorldPop')

if not os.path.exists(pop_folder):
    
    os.makedirs(pop_folder)


def remove_small_shapes(x):
    """
    Remove small multipolygon shapes.

    Parameters
    ---------
    x : polygon
        Feature to simplify.

    Returns
    -------
    MultiPolygon : MultiPolygon
        Shapely MultiPolygon geometry without tiny shapes.

    """
    if x.geometry.type == 'Polygon':

        return x.geometry

    elif x.geometry.type == 'MultiPolygon':

        area1 = 0.01
        area2 = 50

        if x.geometry.area < area1:
            return x.geometry

        if x['GID_0'] in ['CHL','IDN']:

            threshold = 0.01

        elif x['GID_0'] in ['RUS','GRL','CAN','USA']:

            threshold = 0.01

        elif x.geometry.area > area2:

            threshold = 0.1

        else:

            threshold = 0.001

        new_geom = []
        for y in list(x['geometry'].geoms):

            if y.area > threshold:

                new_geom.append(y)

        return MultiPolygon(new_geom)


class ProcessCountry:

    """
    This class process the country folders and
    the national outline shapefile.
    """


    def __init__(self, csv_country, country_iso3):
        """
        A class constructor

        Arguments
        ---------
        csv_country : string
            Name of the country metadata file.
        country_iso3 : string
            Country iso3 to be processed.
        """
        self.csv_country = csv_country
        self.country_iso3 = country_iso3


    def get_countries(self):
        """
        Get all countries.

        Returns
        -------
        countries : dataframe
            Dataframe containing all the country metadata.

        """
        countries = pd.read_csv(self.csv_country, encoding = 'utf-8-sig')

        countries = countries[countries.Exclude == 0]
        
        countries = countries.sample(frac = 1)

        return countries
    

    def process_country_shapes(self):
        """
        This function creates regional folders for each country 
        and then process a national outline shapefile.

        """          
        path = os.path.join('results', 'processed', self.country_iso3)

        if os.path.exists(os.path.join(path, 'national_outline.shp')):

            print('Completed national outline processing')
            
        print('Processing country shapes for {}'.format(self.country_iso3))

        if not os.path.exists(path):

            os.makedirs(path)

        shape_path = os.path.join(path, 'national_outline.shp')

        path = os.path.join('data', 'raw', 'boundaries', 'gadm36_0.shp')

        countries = gpd.read_file(path)

        single_country = countries[countries.GID_0 == self.country_iso3].reset_index()

        single_country = single_country.copy()
        single_country['geometry'] = single_country.geometry.simplify(
            tolerance = 0.01, preserve_topology = True)
        
        single_country['geometry'] = single_country.apply(
            remove_small_shapes, axis = 1)
        
        glob_info_path = os.path.join(self.csv_country)
        load_glob_info = pd.read_csv(glob_info_path, encoding = 'utf-8-sig', 
                                     keep_default_na = False)
        
        single_country = single_country.merge(load_glob_info, left_on = 'GID_0', 
            right_on = 'iso3')
        
        single_country.to_file(shape_path)

        return print('National outline shapefile processing completed for {}'.format(self.country_iso3))


class ProcessRegions:

    """
    This class process the country folders and
    the national outline shapefile.
    """


    def __init__(self, country_iso3, gid_level):
        """
        A class constructor

        Arguments
        ---------
        country_iso3 : string
            Country iso3 to be processed..
        gid_level : integer
            Gid level to process.
        """
        self.gid_level = gid_level
        self.country_iso3 = country_iso3


    def process_regions(self):
        """
        Function for processing the lowest desired subnational
        regions for the chosen country.
        """
        regions = []

        for regional_level in range(1, int(self.gid_level) + 1): 

            filename = 'regions_{}_{}.shp'.format(regional_level, self.country_iso3)
            folder = os.path.join('results', 'processed', self.country_iso3, 'regions')
            path_processed = os.path.join(folder, filename)

            if os.path.exists(path_processed):

                continue

            print('Processing GID_{} region shapes for {}'.format(regional_level, self.country_iso3))

            if not os.path.exists(folder):

                os.mkdir(folder)

            filename = 'gadm36_{}.shp'.format(regional_level)
            path_regions = os.path.join('data', 'raw', 'boundaries', filename)
            regions = gpd.read_file(path_regions)

            regions = regions[regions.GID_0 == self.country_iso3]

            regions = regions.copy()
            regions['geometry'] = regions.geometry.simplify(
                tolerance=0.005, preserve_topology=True)

            regions['geometry'] = regions.apply(remove_small_shapes, axis = 1)

            try:

                regions.to_file(path_processed, driver = 'ESRI Shapefile')

            except:

                print('Unable to write {}'.format(filename))

                pass

        return None
    

    def process_sub_region_boundaries(self):

        region_path = os.path.join('results', 'processed', 
                    self.country_iso3, 'regions', 'regions_{}_{}.shp'.format(
                        3, self.country_iso3)) 
        region_path_2 = os.path.join('results', 'processed', self.country_iso3, 
                    'regions', 'regions_{}_{}.shp'.format(2, self.country_iso3))
        
        if os.path.exists(region_path):

            countries = gpd.read_file(region_path)
            gid = 'GID_3'

        else:

            countries = gpd.read_file(region_path_2)
            gid = 'GID_2'

        for index, row in tqdm(countries.iterrows(), 
                desc = 'Processing sub-region boundaries for {}'.format(
                    self.country_iso3)):

            sub_region_shapefile = gpd.GeoDataFrame([row], crs = countries.crs)

            filename = '{}.shp'.format(row[gid])    

            folder_out = os.path.join('results', 'processed', self.country_iso3, 
                                      'boundaries')

            if not os.path.exists(folder_out):

                os.makedirs(folder_out)

            path_out = os.path.join(folder_out, filename)

            sub_region_shapefile.to_file(path_out, driver = 'ESRI Shapefile')

        return None


class ProcessPopulation:
    """
    This class process the country folders and
    the national outline shapefile.
    """


    def __init__(self, csv_country, country_iso3, gid_region, pop_tiff):
        """
        A class constructor

        Arguments
        ---------
        csv_country : string
            Name of the country metadata file.
        country_iso3 : string
            Country iso3 to be processed.
        gid_region: string
            GID boundary spatial level to process
        pop_tiff: string
            Filename of the population raster layer

        """
        self.csv_country = csv_country
        self.country_iso3 = country_iso3
        self.pop_tiff = pop_tiff
        self.gid_region = gid_region


    def process_national_population(self):

        """
        This function creates a national population .tiff
        using national boundary files created in 
        process_national_boundary function
        """

        iso3 = self.country_iso3

        filename = self.pop_tiff
        path_pop = os.path.join(filename)
        hazard = rasterio.open(path_pop, 'r+')
        hazard.nodata = 255                       
        hazard.crs.from_epsg(4326) 

        filename = 'national_outline.shp'
        folder = os.path.join('results', 'processed', self.country_iso3)
        
        #then load in our country as a geodataframe
        path_in = os.path.join(folder, filename)
        country_pop = gpd.read_file(path_in, crs = 'epsg:4326')

        #create a new gpd dataframe from our single country geometry
        geo = gpd.GeoDataFrame(gpd.GeoSeries(country_pop.geometry))

        #this line sets geometry for resulting geodataframe
        geo = geo.rename(columns={0:'geometry'}).set_geometry('geometry')

        #convert to json
        coords = [json.loads(geo.to_json())['features'][0]['geometry']]        

        #carry out the clip using our mask
        out_img, out_transform = mask(hazard, coords, crop = True)

        #update our metadata
        out_meta = hazard.meta.copy()
        out_meta.update({'driver': 'GTiff', 'height': out_img.shape[1],
                        'width': out_img.shape[2], 'transform': out_transform,
                        'crs': 'epsg:4326'})
        
        #now we write out at the regional level
        filename_out = 'ppp_2020_1km_Aggregated.tif' 
        folder_out = os.path.join('results', 'processed', iso3, 'population', 'national')

        if not os.path.exists(folder_out):

            os.makedirs(folder_out)

        path_out = os.path.join(folder_out, filename_out)

        with rasterio.open(path_out, 'w', ** out_meta) as dest:

            dest.write(out_img)

        return print('Population processing completed for {}'.format(iso3))
    

    def process_population_tif(self):
        """
        Process population layer.
        
        Parameters
        ----------
        data_name: string
            Filename of the population raster layer
        gid_level: string
            GID boundary spatial level to process
            
        Returns
        -------
        output: dictionary.
            Dictionary containing the country population and grid level
        """
        gid_region = self.gid_region
        iso = self.country_iso3

        filename = 'regions_{}_{}.shp'.format(gid_region, iso)
        path_regions = os.path.join('results', 'processed', iso, 'regions', filename)
        rastername = 'ppp_2020_1km_Aggregated.tif'
        path_raster = os.path.join('results', 'processed', iso, 'population', 'national', rastername)

        boundaries = gpd.read_file(path_regions, crs = 'epsg:4326')

        output = []
        print('Working on {}'.format(iso))
        for idx, boundary in boundaries.iterrows():
    
            with rasterio.open(path_raster) as src:
                
                affine = src.transform
                array = src.read(1)
                array[array <= 0] = 0
                
                population = [i['sum'] for i in zonal_stats(
                    boundary['geometry'], array, nodata = 255,
                    stats = ['sum'], affine = affine)][0]

                #Calculate the central coordinates of each of the polygons
                boundary['centroid'] = boundary['geometry'].centroid
                boundary['longitude'] = boundary['centroid'].x
                boundary['latitude'] = boundary['centroid'].y

                try:
                    output.append({
                        'iso3':boundary['GID_0'],
                        'GID_2': boundary['GID_2'],
                        'GID_3': boundary['GID_3'],
                        'population': population,
                        'latitude': boundary['latitude'],
                        'longitude': boundary['longitude'],
                        'geometry': boundary['geometry'],
                        'area': boundary['geometry'].area * 12309
                    })
                    
                except:

                    output.append({
                        'iso3':boundary['GID_0'],
                        'GID_2': boundary['GID_2'],
                        'GID_2': boundary['GID_2'],
                        'population': population,
                        'latitude': boundary['latitude'],
                        'longitude': boundary['longitude'],
                        'geometry': boundary['geometry'],
                        'area': boundary['geometry'].area * 12309
                    })

        df = pd.DataFrame(output)
        df.dropna(subset = ['population'], inplace = True)
        df['population'] = df['population'].astype(int)
        df[['latitude', 'longitude']] = df[['latitude', 'longitude']].round(4)

        fileout = '{}_population_results.csv'.format(iso)
        folder_out = os.path.join('results', 'final', iso, 'population')
        if not os.path.exists(folder_out):

            os.makedirs(folder_out)

        path_out = os.path.join(folder_out, fileout)
        df.to_csv(path_out, index = False)

        return output


    def process_sub_regional_pop_tiff(self):
        """
        This function creates a regional composite population .tiff 
        using regional boundary files created in 
        process_regional_boundary function and national
        population files created in process_national_population
        function.
        """
        countries = pd.read_csv(self.csv_country, encoding = 'utf-8-sig')
        print('Working on {}'.format(self.country_iso3))
        
        for idx, country in countries.iterrows():

            if not country['iso3'] == self.country_iso3: 

                continue   
            
            #define our country-specific parameters, including gid information
            iso3 = country['iso3']
            
            #set the filename depending our preferred regional level
            region_path = os.path.join('results', 'processed', 
                          self.country_iso3, 'regions', 
                          'regions_{}_{}.shp'.format(3, 
                          self.country_iso3)) 
            
            region_path_2 = os.path.join('results', 'processed', 
                            self.country_iso3, 'regions', 
                            'regions_{}_{}.shp'.format(2, 
                            self.country_iso3))
            
            if os.path.exists(region_path):

                regions = gpd.read_file(region_path)
                gid = 'GID_3'

            else:

                regions = gpd.read_file(region_path_2)
                gid = 'GID_2'
            
            for idx, region in regions.iterrows():

                #get our gid id for this region 
                #(which depends on the country-specific gid level)
                gid_id = region[gid]

                filename = 'ppp_2020_1km_Aggregated.tif'
                folder = os.path.join('results', 'processed', iso3, 'population', 'national')
                path_pop = os.path.join(folder, filename)
                hazard = rasterio.open(path_pop, 'r+')
                hazard.nodata = 255                      
                hazard.crs.from_epsg(4326)                

                geo = gpd.GeoDataFrame({'geometry': [region.geometry]}, crs=regions.crs)

                geo = geo.rename(columns = {0:'geometry'}).set_geometry('geometry')

                geo = geo.to_crs(hazard.crs)
                coords = [json.loads(geo.to_json())['features'][0]['geometry']] 

                out_img, out_transform = mask(hazard, coords, crop = True)

                out_meta = hazard.meta.copy()

                out_meta.update({'driver': 'GTiff', 'height': out_img.shape[1],
                                'width': out_img.shape[2], 'transform': out_transform,
                                'crs': 'epsg:4326'})

                filename_out = '{}.tif'.format(gid_id) 
                folder_out = os.path.join('results', 'processed', 
                                          self.country_iso3, 'population', 'tiffs')

                if not os.path.exists(folder_out):

                    os.makedirs(folder_out)

                path_out = os.path.join(folder_out, filename_out)

                with rasterio.open(path_out, 'w', **out_meta) as dest:

                    dest.write(out_img)
            
            print('Processing complete for {}'.format(iso3))


    def pop_process_shapefiles(self):

        """
        This function process each of the population 
        raster layers to vector shapefiles
        """
        folder = os.path.join('results', 'processed', self.country_iso3, 'population', 'tiffs')

        for tifs in tqdm(os.listdir(folder), 
                         desc = 'Processing sub-regional population shapefiles for {}...'.format(
                        self.country_iso3)):
            try:

                if tifs.endswith('.tif'):

                    tifs = os.path.splitext(tifs)[0]

                    folder = os.path.join('results', 'processed', 
                             self.country_iso3, 'population', 'tiffs')
                    filename = tifs + '.tif'
                    gid_name = os.path.basename(filename)
                    gid_name = gid_name.rsplit(".", 1)[0]
                    
                    path_in = os.path.join(folder, filename)

                    folder = os.path.join('results', 'processed', 
                             self.country_iso3, 'population', 'shapefiles')
                    
                    if not os.path.exists(folder):

                        os.mkdir(folder)
                        
                    filename = tifs + '.shp'
                    path_out = os.path.join(folder, filename)

                    with rasterio.open(path_in) as src:

                        affine = src.transform
                        array = src.read(1)

                        output = []

                        for vec in rasterio.features.shapes(array):

                            if vec[1] > 0 and not vec[1] == 255:

                                coordinates = [i for i in vec[0]['coordinates'][0]]

                                coords = []

                                for i in coordinates:

                                    x = i[0]
                                    y = i[1]

                                    x2, y2 = src.transform * (x, y)

                                    coords.append((x2, y2))

                                output.append({
                                    'type': vec[0]['type'],
                                    'geometry': {
                                        'type': 'Polygon',
                                        'coordinates': [coords],
                                    },
                                    'properties': {
                                        'GID_2': gid_name,
                                        'value': vec[1],
                                    }
                                })

                    output = gpd.GeoDataFrame.from_features(output, crs = 'epsg:4326')
                    output.to_file(path_out, driver = 'ESRI Shapefile')

            except:

                pass

        return None


class FiberProcess:

    """
    This class generates lines 
    connecting central coordinates 
    of sub-regions.
    """

    def __init__(self, country_iso3, country_iso2, csv_country):
        """
        A class constructor

        Arguments
        ---------
        country_iso3 : string
            Country iso3 to be processed.
        country_iso2 : string
            Country iso2 to be processed 
            (specific for fiber data).
        csv_country : string
            Name of the country metadata file.
        """
        self.country_iso3 = country_iso3
        self.country_iso2 = country_iso2
        self.csv_country = csv_country


    def process_existing_fiber(self):
        """
        Load and process existing fiber data.

        """
        iso3 = self.country_iso3
        iso2 = self.country_iso2.lower()

        folder = os.path.join(DATA_PROCESSED, iso3, 'network_existing')

        if not os.path.exists(folder):

            os.makedirs(folder)

        filename = '{}_core_edges_existing.shp'.format(iso3)
        path_output = os.path.join(folder, filename)

        if os.path.exists(path_output):

            return print('Existing fiber already processed')

        else:

            path = os.path.join(DATA_RAW, 'existing_fiber', 
                                'SSA_existing_fiber.shp')

            shape = fiona.open(path)

            data = []

            for item in shape:

                if item['properties']['iso2'] == iso2:

                    if item['geometry']['type'] == 'LineString':

                        data.append({
                            'type': 'Feature',
                            'geometry': {
                                'type': 'LineString',
                                'coordinates': item['geometry']['coordinates'],
                            },
                            'properties': {
                                'operators': item['properties']['operator'],
                                'source': 'existing'
                            }
                        })

                    if item['geometry']['type'] == 'MultiLineString':
                            
                        geom = MultiLineString(item['geometry']['coordinates'])

                        for line in geom.geoms:

                            data.append({
                                'type': 'Feature',
                                'geometry': mapping(line),
                                'properties': {
                                    'operators': item['properties']['operator'],
                                    'source': 'existing'
                                }
                            })


            if len(data) == 0:

                return print('No existing infrastructure')

            data = gpd.GeoDataFrame.from_features(data, crs = 'epsg:4326')
            data.to_file(path_output)

        return print('Existing fiber processed')


    def find_nodes_on_existing_infrastructure(self):
        """
        Find those agglomerations which are within a buffered zone of
        existing fiber links.

        """

        countries = pd.read_csv(self.csv_country, encoding = 'utf-8-sig')

        for idx, country in countries.iterrows():

            if not country['iso3'] == self.country_iso3: 

                continue   

            iso3 = country['iso3']

            folder = os.path.join(DATA_PROCESSED, iso3, 'network_existing')
            filename = '{}_core_nodes_existing.shp'.format(iso3)
            path_output = os.path.join(folder, filename)

            if os.path.exists(path_output):

                return print('Already found nodes on existing infrastructure')
            
            else:

                if not os.path.dirname(path_output):

                    os.makedirs(os.path.dirname(path_output))

            path = os.path.join(folder, '{}_core_edges_existing.shp').format(iso3)

            if not os.path.exists(path):

                return print('No existing infrastructure')

            existing_infra = gpd.read_file(path, crs='epsg:4326')

            existing_infra = existing_infra.to_crs(epsg=3857)
            existing_infra['geometry'] = existing_infra['geometry'].buffer(5000)
            existing_infra = existing_infra.to_crs(epsg=4326)

            path = os.path.join(DATA_PROCESSED, iso3, 'agglomerations', 
                                'agglomerations.shp').format(iso3)
            agglomerations = gpd.read_file(path, crs='epsg:4326')

            bool_list = agglomerations.intersects(existing_infra.unary_union)

            agglomerations = pd.concat([agglomerations, bool_list], axis=1)

            agglomerations = agglomerations[agglomerations[0] == 
                                            True].drop(columns = 0)

            agglomerations['source'] = 'existing'

            agglomerations.to_file(path_output)


        return print('Found nodes on existing infrastructure')


def download_street_data(iso3):

    """
    This function download the street data for each country.

    Parameters
    ----------
    iso3 : string
        Country ISO3 code
    """
    path = os.path.join(DATA_RAW, 'countries.csv')
    countries = pd.read_csv(path, encoding = 'utf-8-sig')
    ssa_countries = countries

    for idx, ssa in ssa_countries.iterrows():

        if not ssa['iso3'] == iso3:

            continue
        
        print('Extracting street data for {}'.format(iso3))
        roads = ox.graph_from_place(format('{}'.format(ssa['country'])))

        print('Converting extracted {} street data to geodataframe'.format(iso3))
        road_gdf = ox.graph_to_gdfs(roads, nodes = False, edges = True)

        print('Converting extracted {} street geodataframe to csv'.format(iso3))
        fileout = '{}_national_street_data.csv'.format(iso3)
        folder_out = os.path.join(DATA_RAW, 'street_data', iso3)
        if not os.path.exists(folder_out):

            os.makedirs(folder_out)

        path_out = os.path.join(folder_out, fileout)
        road_gdf.to_csv(path_out, index = False)


    return None


def generate_street_shapefile(iso3):

    """
    This function convert the downloaded csv data into shapefile.

    Parameters
    ----------
    iso3 : string
        Country ISO3 code
    """
    path = os.path.join(DATA_RAW, 'countries.csv')
    countries = pd.read_csv(path, encoding = 'utf-8-sig')
    ssa_countries = countries

    for idx, ssa in ssa_countries.iterrows():

        if not ssa['iso3'] == iso3:

            continue
        
        csv_path = os.path.join(DATA_RAW, 'street_data', iso3, 
                                '{}_national_street_data.csv'.format(iso3))
        
        print('Reading CSV street data for {}'.format(iso3))
        df = pd.read_csv(csv_path)
        df = df[['highway', 'length', 'geometry']]
        df['iso3'] = ''

        print('Processing CSV street data for {}'.format(iso3))
        df['iso3'] = iso3
        df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.loads(x))
        gdf = gpd.GeoDataFrame(data = df, geometry = df['geometry'], crs = 4329)

        filename = '{}_street_data.shp'.format(iso3)
        folder_out = os.path.join(DATA_RAW, 'street_data', iso3)

        if not os.path.exists(folder_out):

            os.makedirs(folder_out)

        path_out = os.path.join(folder_out, filename)
        gdf.to_file(path_out)


    return None


def process_region_street(iso3):

    path = os.path.join(DATA_RAW, 'countries.csv')
    countries = pd.read_csv(path, encoding='utf-8-sig')
    if iso3 not in countries["iso3"].values:
        return None  # Exit early if iso3 not found

    region_path = os.path.join(DATA_PROCESSED, iso3, 'regions', f'regions_2_{iso3}.shp')
    regions = gpd.read_file(region_path)
    gid = 'GID_2'

    file_in = os.path.join(DATA_RAW, 'street_data', iso3, f'{iso3}_street_data.shp')
    gdf_street = gpd.read_file(file_in)
    gdf_street = gdf_street.set_crs("epsg:4326", allow_override=True)

    for _, region in regions.iterrows():
        gid_id = region[gid]
        gdf_region = regions[regions[gid] == gid_id]
        gdf_region = gdf_region.set_crs(gdf_street.crs)

        print(f'Intersecting {gid_id} street data points')
        try:
            gdf_result = gpd.overlay(gdf_street, gdf_region, how='intersection')
        except Exception as e:
            print(f"Overlay failed for {gid_id}: {e}")
            continue

        folder_out = os.path.join(DATA_PROCESSED, iso3, 'streets', 'regions')
        os.makedirs(folder_out, exist_ok=True)
        path_out = os.path.join(folder_out, f'{gid_id}.shp')
        gdf_result.to_file(path_out)

    return None


def process_access_street(iso3):
    """
    Function to process the street data at sub-regional level.

    Parameters
    ----------
    iso3 : string
        Country ISO3 code
    """

    # Load countries and verify ISO3 is valid
    path = os.path.join(DATA_RAW, 'countries.csv')
    countries = pd.read_csv(path, encoding='utf-8-sig')
    if iso3 not in countries["iso3"].values:
        print(f"{iso3} not found in countries.csv")
        return None

    # Determine region shapefile and GID column
    region_path_3 = os.path.join(DATA_PROCESSED, iso3, 'regions', 
                                 f'regions_3_{iso3}.shp')
    region_path_2 = os.path.join('results', 'processed', iso3, 'regions', 
                                 f'regions_2_{iso3}.shp')

    if os.path.exists(region_path_3):
        regions = gpd.read_file(region_path_3)
        gid = 'GID_3'
    elif os.path.exists(region_path_2):
        regions = gpd.read_file(region_path_2)
        gid = 'GID_2'
    else:
        print(f"No valid region shapefile found for {iso3}")
        return None

    # Read the full street data once
    file_in = os.path.join(DATA_RAW, 'street_data', iso3, 
                           f'{iso3}_street_data.shp')
    if not os.path.exists(file_in):
        print(f"Street data not found for {iso3}")
        return None

    gdf_street = gpd.read_file(file_in)
    gdf_street = gdf_street.set_crs("epsg:4326", allow_override=True)

    # Ensure regions are in the same CRS
    regions = regions.set_crs(gdf_street.crs, allow_override=True)

    # Process each region
    for _, region in regions.iterrows():
        gid_id = region[gid]
        gdf_region = regions[regions[gid] == gid_id]

        print(f'Intersecting {gid_id} street data points')

        try:
            gdf_result = gpd.overlay(gdf_street, gdf_region, how='intersection')
        except Exception as e:
            print(f"Overlay failed for {gid_id}: {e}")
            continue

        # Save result
        folder_out = os.path.join(DATA_PROCESSED, iso3, 'streets', 'sub_regions')
        os.makedirs(folder_out, exist_ok=True)
        path_out = os.path.join(folder_out, f'{gid_id}.shp')
        gdf_result.to_file(path_out)

    return None


def population_decile(decile_value):

    """
    This function determines the population decile

    Parameters
    ----------
    decile_value : Integer
        Decile categorization value

    Returns
    -------
    decile : string
        Population decile category where the region belongs
    """

    if decile_value == 1:

        decile = 'Decile 10'

    elif decile_value == 2:

        decile = 'Decile 9'

    elif decile_value == 3:

        decile = 'Decile 8'

    elif decile_value == 4:

        decile = 'Decile 7'

    elif decile_value == 5:

        decile = 'Decile 6'

    elif decile_value == 6:

        decile = 'Decile 5'

    elif decile_value == 7:

        decile = 'Decile 4'

    elif decile_value == 8:

        decile = 'Decile 3'

    elif decile_value == 9:

        decile = 'Decile 2'

    else:

        decile = 'Decile 1'

    return decile 
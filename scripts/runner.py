import configparser
import os
import sys
import warnings
import pandas as pd
from gambit.preprocessing import (ProcessCountry, ProcessRegions, 
    ProcessPopulation, FiberProcess, download_street_data, 
    generate_street_shapefile, process_region_street, process_access_street)
from gambit.netPlanning import (process_regional_settlement_tifs, 
    generate_regional_settlement_lut, process_access_settlement_tifs, 
    generate_access_settlement_lut, generate_agglomeration_lut, 
    find_largest_regional_settlement, get_settlement_routing_paths,
    create_regions_to_model, create_routing_buffer_zone, 
    create_regional_routing_buffer_zone)
from gambit.optimizer import batch_pcst_parallel

pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore')

CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(__file__), 'script_config.ini'))
BASE_PATH = CONFIG['file_locations']['base_path']

DATA_RAW = os.path.join(BASE_PATH, 'raw')
DATA_PROCESSED = os.path.join(BASE_PATH, '..', 'results', 'processed')
DATA_RESULTS = os.path.join(BASE_PATH, '..', 'results', 'final')

path = os.path.join(DATA_RAW, 'countries.csv')
pop_tif_loc = os.path.join(DATA_RAW, 'WorldPop', 'ppp_2020_1km_Aggregated.tif')
countries = pd.read_csv(path, encoding = 'utf-8-sig')

poverty_shp = os.path.join(DATA_RAW, 'poverty_data', 'GSAP2.shp')

if __name__ == '__main__':

    for idx, country in countries.iterrows():
            
        #if not country['regions'] == 'Sub-Saharan Africa' or country['iso3'] == 'SLE':
            
        if not country['iso3'] == 'SLE':
            
            continue 

        '''country = ProcessCountry(path, countries['iso3'].loc[idx])
        country.process_country_shapes()

        regions = ProcessRegions(countries['iso3'].loc[idx], countries['lowest'].loc[idx])
        regions.process_regions()
        regions.process_sub_region_boundaries()

        populations = ProcessPopulation(path, countries['iso3'].loc[idx], countries['lowest'].loc[idx], pop_tif_loc)
        populations.process_national_population()
        populations.process_population_tif()
        populations.process_sub_regional_pop_tiff()
        populations.pop_process_shapefiles()

        process_regional_settlement_tifs(country)
        generate_regional_settlement_lut(country)
        process_access_settlement_tifs(country)
        generate_access_settlement_lut(country)

        generate_agglomeration_lut(country)
        find_largest_regional_settlement(country)
        get_settlement_routing_paths(country)
        create_regions_to_model(country)
        create_routing_buffer_zone(country)
        create_regional_routing_buffer_zone(country)
        
        
        fiber = FiberProcess(countries['iso3'].loc[idx], 
                                   countries['iso2'].loc[idx], path)
        #fiber.process_existing_fiber()
        #fiber.find_nodes_on_existing_infrastructure()'''

        ''' This block of code should only be run once to download and process 
        road data. '''
        #download_street_data(countries['iso3'].loc[idx])
        #generate_street_shapefile(countries['iso3'].loc[idx])
        #process_region_street(countries['iso3'].loc[idx])
        #process_access_street(countries['iso3'].loc[idx])

        # Perform Spatial Optimization
        batch_pcst_parallel(roads_folder = os.path.join(DATA_PROCESSED, 
                countries['iso3'].loc[idx], 'streets', 'regions'),
        population_folder = os.path.join(DATA_PROCESSED, countries['iso3'].loc[idx], 
                'buffer_routing_zones', 'regional_nodes'),
        output_folder = os.path.join(DATA_RESULTS, countries['iso3'].loc[idx]),
        max_workers = None)

        batch_pcst_parallel(roads_folder = os.path.join(DATA_PROCESSED, 
                countries['iso3'].loc[idx], 'streets', 'sub_regions'),
        population_folder = os.path.join(DATA_PROCESSED, countries['iso3'].loc[idx], 
                'buffer_routing_zones', 'nodes'),
        output_folder = os.path.join(DATA_RESULTS, countries['iso3'].loc[idx]),
        max_workers = None)
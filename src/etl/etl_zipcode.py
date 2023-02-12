#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!pip install geopandas
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.style.use("ggplot")

import os

#print(os.listdir("../../data/raw"))
#assert False
from bs4 import BeautifulSoup  
import requests
import zipfile
import io


import geopandas as gpd
import shapely

# Load the Drive helper and mount
#from google.colab import drive
#mounted_path_folder = '/content/drive'
#drive.mount(mounted_path_folder, force_remount=True)
#curr_path_folder = "/content/drive/MyDrive/" # parent of current src folder

def get_shapefile_download_href(soup):
    temp_finder = soup.find_all('h3', text = "ZIP Code Tabulation Areas (ZCTAs)")[0]
    temp_finder = temp_finder.parent.find_next_sibling()
    shapefile_download_href = temp_finder.find_all("a",text="shapefile")[0]["href"]
    return shapefile_download_href

def create_zipcode_shapefile_folder(path_folder, zipcode_shapefile_zipped_folder_name):
    path_folder_zipcode_shapefile = os.path.join(path_folder, zipcode_shapefile_zipped_folder_name)
    os.makedirs(path_folder_zipcode_shapefile)
    return path_folder_zipcode_shapefile

def download_extract_shapefile_href_to_folder(shapefile_download_href, path_zipped_folder_zipcode_shapefile):
    r = requests.get(shapefile_download_href)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path_zipped_folder_zipcode_shapefile)

def get_zipcode_general_file_name(shapefile_download_href):
    zipcode_general_file_name = shapefile_download_href.split("/")[-1].split(".")[0]
    return zipcode_general_file_name

def get_zipcode_shapefile_name(zipcode_general_file_name):
    zipcode_shapefile_name = zipcode_general_file_name + ".shp"
    return zipcode_shapefile_name

def get_path_zipcode_shapefile(path_folder_zipcode_shapefile, zipcode_shapefile_name):
    path_zipcode_shapefile = os.path.join(path_folder_zipcode_shapefile, zipcode_shapefile_name)
    return path_zipcode_shapefile

def delete_zipcode_shapefile_folder(path_folder_zipcode_shapefile):
    for file_name in os.listdir(path_folder_zipcode_shapefile):
        # construct full file path
        file = os.path.join(path_folder_zipcode_shapefile, file_name)
        if os.path.isfile(file):
            os.remove(file)
    os.rmdir(path_folder_zipcode_shapefile)


def geospatial_data_preparation(path_zipcode_shapefile):
    '''
    Function that does all of the geospatial data preparation. 
    It takes in the zipcode shapefile, and gets the xy coordinats for each zipcode

    Args:
        path_zipcode_shapefile (str): the path to the shapefile

    Returns:
        GeoDataFrame: the dataframe with 3 columns - the zipcode, and the x and y coordinates.

    '''
    gdf_zipcode_column_name = "ZCTA5CE20"
    gdf_zipcode_3digits_column_name = "3DIGIT_ZIP"
    x_coord_column_name = "X_COORD"
    y_coord_column_name = "Y_COORD"
    pseudo_mercator = 3857
    # see https://en.wikipedia.org/wiki/Web_Mercator_projection

    gdf = gpd.read_file(path_zipcode_shapefile, include_fields=["FID",gdf_zipcode_column_name])
    # the original zipcode column is a length 5 string of numbers (front-zeropadded)
    # but the zipcodes in matt's orders dataset are only the first 3 digits of the zipcode
    # so we add a new column that crops out 5digit zipcode to 3digits so it's usable as a foreign key
    gdf[gdf_zipcode_3digits_column_name] = gdf[gdf_zipcode_column_name].apply(lambda x: x[:3])


    gdf = gdf[["geometry",gdf_zipcode_3digits_column_name]]

    # projected the geometry from spherical to Flat 2D Map
    gdf = gdf.to_crs(pseudo_mercator)

    # But by cropping the 3 digits, we'd have to "dissolve" (basically a spatial version of groupby)
    # merge up all the zipcode polygons by their 3digits zipcode group.
    gdf_groupby = gdf.dissolve(
        by=gdf_zipcode_3digits_column_name,
        as_index=False
    )

    #gdf_groupby = gdf_groupby.simplify(0.1)

    # print(gdf_groupby.crs) # debugger

    # create the centroids (x, y coords)
    gdf_groupby_centroids = gdf_groupby.centroid
    gdf_groupby[x_coord_column_name] = gdf_groupby_centroids.x
    gdf_groupby[y_coord_column_name] = gdf_groupby_centroids.y

    zipcoords = gdf_groupby[[gdf_zipcode_3digits_column_name,x_coord_column_name,y_coord_column_name]]

    return zipcoords



def main(path_folder = "../../data/raw"):

    url_shapefile_location = "https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.2020.html"

    folder_zipcode_shapefile = "zipcodes_temp_geodata\\"

    FILENAME_ZIPCOORDS = "zipcode_coordinates.csv"

    page = requests.get(url_shapefile_location)
    soup = BeautifulSoup(page.text, 'html.parser')
    shapefile_download_href = get_shapefile_download_href(soup)

    path_folder_zipcode_shapefile = create_zipcode_shapefile_folder(path_folder, folder_zipcode_shapefile)
    download_extract_shapefile_href_to_folder(shapefile_download_href, path_folder_zipcode_shapefile)
    zipcode_general_file_name = get_zipcode_general_file_name(shapefile_download_href)
    zipcode_shapefile_name = get_zipcode_shapefile_name(zipcode_general_file_name)
    path_zipcode_shapefile = get_path_zipcode_shapefile(path_folder_zipcode_shapefile, zipcode_shapefile_name)
    zipcoords = geospatial_data_preparation(path_zipcode_shapefile)
    PATH_FILE_ZIPCOORDS = os.path.join(path_folder,FILENAME_ZIPCOORDS)
    zipcoords.to_csv(PATH_FILE_ZIPCOORDS, index=False)
    delete_zipcode_shapefile_folder(path_folder_zipcode_shapefile)
    print("geo etl done")

#if __name__ == "__main__":
#    main()


# In[ ]:





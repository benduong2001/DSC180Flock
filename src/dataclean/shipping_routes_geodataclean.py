# -*- coding: utf-8 -*-
"""shipping_routes_geodataclean.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1S_CzQlF5UBCuST7aaR4wvhCgmtS30__d
"""

#!pip install geopandas

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#plt.style.use("ggplot")

import os
from bs4 import BeautifulSoup  
import requests
import zipfile
import io
#import tqdm

import geopandas as gpd
import shapely

google_colab = 0
if google_colab == 1:
    # Load the Drive helper and mount
    from google.colab import drive
    mounted_path_folder = '/content/drive'
    drive.mount(mounted_path_folder, force_remount=True)
    path_folder = "/content/drive/MyDrive/dsprojects/dsproject_ff_geo/current/" # parent of current src folder
else:
    path_folder = "..//..//"
import sys
sys.path.insert(0, path_folder+"/src/dataclean")
import util



# https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html
def get_county_census_df(args):

    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data, "raw")

    path_folder_data_county_census = os.path.join(path_folder_data_raw,"county_census_data")
    file_name_county_census = 'county_census_data.xlsx'
    path_file_data_county_census = os.path.join(path_folder_data_county_census,file_name_county_census)
    county_census_df_orig = pd.read_excel(path_file_data_county_census,skiprows=3)
    county_census_df = county_census_df_orig
    # dataclean county census df3
    county_census_df = county_census_df.iloc[1:3143,:2]

    county_column_name = list(county_census_df.columns)[0]

    county_census_df[county_column_name] = county_census_df[county_column_name].str[1:].str.replace(" Parish, "," County, ")

    county_census_df.columns = ["COUNTY_NAME","POPULATION"]
    return county_census_df


def get_county_gdf(args):
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data, "raw")

    path_folder_data_county_shapefile = os.path.join(path_folder_data_raw,"county_geodata")
    file_name_county_shapefile = 'cb_2020_us_county_500k.shp'
    path_file_county_shapefile = os.path.join(path_folder_data_county_shapefile,file_name_county_shapefile)

    county_gdf = gpd.read_file(path_file_county_shapefile)
    county_gdf = county_gdf.to_crs(3857)
    us_state_fips_mapper = {
        '01': 'Alabama',
        '02': 'Alaska',
        '04': 'Arizona',
        '05': 'Arkansas',
        '06': 'California',
        '08': 'Colorado',
        '09': 'Connecticut',
        '10': 'Delaware',
        '11': 'District of Columbia',
        '12': 'Florida',
        '13': 'Georgia',
        '15': 'Hawaii',
        '16': 'Idaho',
        '17': 'Illinois',
        '18': 'Indiana',
        '19': 'Iowa',
        '20': 'Kansas',
        '21': 'Kentucky',
        '22': 'Louisiana',
        '23': 'Maine',
        '24': 'Maryland',
        '25': 'Massachusetts',
        '26': 'Michigan',
        '27': 'Minnesota',
        '28': 'Mississippi',
        '29': 'Missouri',
        '30': 'Montana',
        '31': 'Nebraska',
        '32': 'Nevada',
        '33': 'New Hampshire',
        '34': 'New Jersey',
        '35': 'New Mexico',
        '36': 'New York',
        '37': 'North Carolina',
        '38': 'North Dakota',
        '39': 'Ohio',
        '40': 'Oklahoma',
        '41': 'Oregon',
        '42': 'Pennsylvania',
        '44': 'Rhode Island',
        '45': 'South Carolina',
        '46': 'South Dakota',
        '47': 'Tennessee',
        '48': 'Texas',
        '49': 'Utah',
        '50': 'Vermont',
        '51': 'Virginia',
        '53': 'Washington',
        '54': 'West Virginia',
        '55': 'Wisconsin',
        '56': 'Wyoming'
    }

    county_gdf = county_gdf[county_gdf['STATEFP'].isin(us_state_fips_mapper)]
    state_fips_column = county_gdf['STATEFP'].astype(float).astype(int).astype(str).str.zfill(2)
    state_name_column = state_fips_column.apply(lambda x: us_state_fips_mapper[x])
    county_name_column = county_gdf["NAME"] + " County, " + state_name_column
    county_gdf["COUNTY_NAME"] = county_name_column

    county_gdf = county_gdf[["COUNTY_NAME","ALAND","geometry"]]
    fig, ax = plt.subplots()
    county_gdf.plot(ax=ax)
    return county_gdf

def get_county_weather_df_monthly(args):
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data, "raw")
    
    path_folder_data_county_weather = os.path.join(path_folder_data_raw,"county_weather_data")
    county_weather_csv_file_names_list = (os.listdir(path_folder_data_county_weather))

    county_pcp_csv_file_names_list = [file_name for file_name in county_weather_csv_file_names_list if "pcp" in file_name]
    county_tavg_csv_file_names_list = [file_name for file_name in county_weather_csv_file_names_list if "tavg" in file_name]

    county_tavg_csv_file_names_list_sorted = sorted(county_tavg_csv_file_names_list)
    county_pcp_csv_file_names_list_sorted = sorted(county_pcp_csv_file_names_list)

    temp_tavg_df_concat_list = []
    temp_pcp_df_concat_list = []

    for month_i in list(range(12)):
        file_name_county_tavg = county_tavg_csv_file_names_list_sorted[month_i]
        path_file_county_tavg = os.path.join(path_folder_data_county_weather, file_name_county_tavg)
        tavg_df = pd.read_csv(path_file_county_tavg, skiprows=3).iloc[:,:3]
        assert list(tavg_df.columns) == ["Location ID","Location","Value"]
        tavg_df = tavg_df.rename(columns={"Value":"TEMPERATURE"})
        tavg_df["MONTH"] = month_i+1
        #print(tavg_df.shape)
        temp_tavg_df_concat_list.append(tavg_df)

        file_name_county_pcp = county_pcp_csv_file_names_list_sorted[month_i]
        path_file_county_pcp = os.path.join(path_folder_data_county_weather, file_name_county_pcp)
        pcp_df = pd.read_csv(path_file_county_pcp, skiprows=3).iloc[:,:3]
        assert list(pcp_df.columns) == ["Location ID","Location","Value"]
        pcp_df = pcp_df.rename(columns={"Value":"PRECIPITATION"})
        pcp_df["MONTH"] = month_i+1
        temp_pcp_df_concat_list.append(pcp_df)
        #print(pcp_df.shape)

    monthly_tavg_df = pd.concat(temp_tavg_df_concat_list, axis=0)
    monthly_pcp_df  = pd.concat(temp_pcp_df_concat_list, axis=0)

    county_weather_df_monthly = monthly_tavg_df.merge(monthly_pcp_df,on=["Location ID","Location","MONTH"])
    assert county_weather_df_monthly.shape[0] == 3137*12

    us_states_abbrev_mapper = {
        'AL': 'Alabama',
        'AK': 'Alaska',
        'AZ': 'Arizona',
        'AR': 'Arkansas',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'IA': 'Iowa',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'ME': 'Maine',
        'MD': 'Maryland',
        'MA': 'Massachusetts',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MS': 'Mississippi',
        'MO': 'Missouri',
        'MT': 'Montana',
        'NE': 'Nebraska',
        'NV': 'Nevada',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NY': 'New York',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VT': 'Vermont',
        'VA': 'Virginia',
        'WA': 'Washington',
        'WV': 'West Virginia',
        'WI': 'Wisconsin',
        'WY': 'Wyoming'
    }
    county_state_abbrev_column = county_weather_df_monthly["Location ID"].str[:2]
    county_state_name_column = county_state_abbrev_column.apply(lambda x: us_states_abbrev_mapper[x])
    county_name_column = county_weather_df_monthly["Location"] + ", " + county_state_name_column
    county_name_column = county_name_column.str.replace(" Parish, "," County, ")

    county_weather_df_monthly["COUNTY_NAME"] = county_name_column
    county_weather_df_monthly = (
        county_weather_df_monthly[["COUNTY_NAME","MONTH","PRECIPITATION","TEMPERATURE"]]
    )
    return county_weather_df_monthly

def get_operating_zip3_buffers_gdf(args):
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data, "raw")
    file_name_carrier_data = "carrier_data.csv"
    path_file_carrier_data = os.path.join(path_folder_data_raw, file_name_carrier_data)
    df = pd.read_csv(path_file_carrier_data)
    print("Loaded carrier_data df")
    print(df.shape)
    df["OPERATING_ZIP3"].fillna("null").value_counts()

    file_name_zipcode_coordinates = "zipcode_coordinates.csv"
    path_file_zipcode_coordinates = os.path.join(path_folder_data_raw, file_name_zipcode_coordinates)
    zipcode_coordinates = pd.read_csv(path_file_zipcode_coordinates)
    print("Loaded zipcodes df")
    print(zipcode_coordinates.shape)

    zipcode_coordinates["3DIGIT_ZIP"] = zipcode_coordinates["3DIGIT_ZIP"].astype(int).astype(str).str.zfill(3)

    print("Left-join between carrier data and zipcode coordinates")
    df = df.drop_duplicates(subset=["CARRIER_ID"])
    df = (
        df
        .merge(zipcode_coordinates, left_on=["OPERATING_ZIP3"], right_on=["3DIGIT_ZIP"],how="left")
        .rename(columns={"X_COORD":"X_COORD_OPER","Y_COORD":"Y_COORD_OPER"})
        .drop(columns=["3DIGIT_ZIP"])
    )


    xy_coords_to_point_func = np.vectorize(lambda x, y: shapely.Point(x,y))
    x_coord_column = df['X_COORD_OPER']
    y_coord_column = df['Y_COORD_OPER']
    point_column_name = "POINT_OPER"
    point_column = xy_coords_to_point_func(x_coord_column,y_coord_column)
    df[point_column_name] = point_column

    df.dropna(subset=["OPERATING_ZIP3"],inplace=True)
    oper_count_column = df.groupby(["OPERATING_ZIP3"])["CARRIER_ID"].transform("count").values
    zipcode_oper_column_name = "OPERATING_ZIP3"
    oper_count_column_name = "OPER_COUNT"
    df[oper_count_column_name] = oper_count_column
    df = df[[zipcode_oper_column_name,point_column_name,oper_count_column_name]]
    df.drop_duplicates(inplace=True)
    geometry_column_name = "geometry"
    df.rename(columns={point_column_name:geometry_column_name},inplace=True)
    gdf = gpd.GeoDataFrame(df,crs="EPSG:3857")
    gdf.set_geometry(geometry_column_name)
    gdf[geometry_column_name] = gdf[geometry_column_name].buffer(500_000)

    fig, ax = plt.subplots()
    #ax = util.bound_ax(ax)
    ax.set_title("Radiuses Drawn around Operating Zipcode Centroids of Carriers")
    gdf.plot(alpha=0.1,ax=ax)
    return gdf

class Temp_Path_Builder:
    '''
    A class made to hold info about the orders' shipping routes
    '''
    def __init__(self,orders_df):
        self.orders_df = orders_df
    def build_point_df(
        self, 
        x_coord_column_name, 
        y_coord_column_name, 
        point_column_name,
        point_df_name,
        df=None, 
        inplace=False
        ):
        '''

        '''
        if df is None: df = self.orders_df;
        xy_coords_to_point_func = np.vectorize(lambda x, y: shapely.Point(x,y))
        df = df.drop_duplicates(subset=[x_coord_column_name, y_coord_column_name])
        x_coord_column = df[x_coord_column_name]
        y_coord_column = df[y_coord_column_name]
        point_column = xy_coords_to_point_func(x_coord_column,y_coord_column)
        df[point_column_name] = point_column
        point_df = df[[x_coord_column_name,y_coord_column_name,point_column_name]]
        if inplace == True: setattr(self,point_df_name,point_df);
        return point_df
    def build_path_df(
        self, 
        point_orig_df,
        point_dest_df,
        x_coord_orig_column_name="X_COORD_ORIG",
        y_coord_orig_column_name="Y_COORD_ORIG",
        x_coord_dest_column_name="X_COORD_DEST",
        y_coord_dest_column_name="Y_COORD_DEST", 
        point_orig_column_name="POINT_ORIG",
        point_dest_column_name="POINT_DEST",
        path_column_name="SHIPPING_ROUTE",
        path_df_name="path_df",
        df=None, 
        inplace=False,
        ):
        if df is None: df = self.orders_df;
        if point_orig_df is None: point_orig_df = self.point_orig_df;
        if point_dest_df is None: point_dest_df = self.point_dest_df;

        df = df.drop_duplicates(subset=[x_coord_orig_column_name, y_coord_orig_column_name, 
                                        x_coord_dest_column_name, y_coord_dest_column_name])
        
        df = df.merge(point_orig_df,on=[x_coord_orig_column_name,y_coord_orig_column_name])
        df = df.merge(point_dest_df,on=[x_coord_dest_column_name,y_coord_dest_column_name])

        points_to_line_func = np.vectorize(lambda point_orig, point_dest: shapely.LineString([point_orig, point_dest]))

        path_point_dest_column = df[point_orig_column_name]
        path_point_orig_column = df[point_dest_column_name]
        path_line_column = points_to_line_func(path_point_dest_column,path_point_orig_column)
        df[path_column_name] = path_line_column
        #df[path_id_column_name] = df["ORIGIN_3DIGIT_ZIP"] + "_TO_" + df["DESTINATION_3DIGIT_ZIP"]
        df = df[[x_coord_orig_column_name,y_coord_orig_column_name,x_coord_dest_column_name,y_coord_dest_column_name,path_column_name]]
        path_df = df
        if inplace == True: setattr(self,path_df_name,path_df);
        return path_df
    def build_path_gdf(
        self,
        path_df=None,
        path_column_name="SHIPPING_ROUTE",
        path_gdf_name="path_gdf",
        inplace=False,
        ):
        if path_df is None: 
            path_df = self.path_df;
        geometry_column_name = "geometry"
        path_df.rename(columns={path_column_name:geometry_column_name},inplace=True)
        path_gdf = gpd.GeoDataFrame(path_df,crs="EPSG:3857")
        path_gdf.set_geometry(geometry_column_name)
        if inplace == True: setattr(self,path_gdf_name,path_gdf);
        return path_gdf
    def build_path_area_gdf(
        self,
        area_gdf,
        path_gdf=None,
        inplace=False,
        path_area_gdf_name="path_area_gdf",
        ):
        # area_gdf may or may not be already geoenriched with county-level data, if already joined to census
        # only after this function can population density be made as a column
        if path_gdf is None: path_gdf = self.path_gdf;
        path_area_gdf = area_gdf.sjoin(path_gdf)
        if inplace == True: setattr(self,path_area_gdf_name,path_area_gdf);
        return path_area_gdf

    def build_path_area_df(
        self,
        path_area_gdf=None,
        inplace=False,
        ):
        if path_area_gdf is None: 
            path_area_gdf = self.path_area_gdf;
        path_area_df = path_area_gdf.drop(columns=["geometry"])
        if inplace == True:
            self.path_area_df = path_area_df
        return path_area_df

def explode_orders_monthly(orders_df):
    x_coord_orig_column_name = "X_COORD_ORIG"
    y_coord_orig_column_name = "Y_COORD_ORIG"
    x_coord_dest_column_name = "X_COORD_DEST"
    y_coord_dest_column_name = "Y_COORD_DEST" 

    order_column = pd.to_datetime(orders_df["ORDER_DATETIME_PST"])
    deadline_column = pd.to_datetime(orders_df["PICKUP_DEADLINE_PST"])
    orders_df["ORDER_DATETIME_PST"] = order_column
    orders_df["PICKUP_DEADLINE_PST"] = deadline_column


    from datetime import datetime

    diff_month = lambda d1, d2: (d2.year - d1.year) * 12 + d2.month - d1.month

    month_difference_column = orders_df.apply(lambda df: diff_month(df["ORDER_DATETIME_PST"],df["PICKUP_DEADLINE_PST"]),axis=1)
    month_difference_column_name = "MONTH_DIFFERENCE"
    orders_df[month_difference_column_name] = month_difference_column
    orders_df = orders_df[orders_df[month_difference_column_name] >= 0]

    get_months_in_between = lambda d1, d2: list(range(d1.month, d2.month+1))
    months_in_between_column = orders_df.apply(lambda df: get_months_in_between(df["ORDER_DATETIME_PST"],df["PICKUP_DEADLINE_PST"]),axis=1)
    months_in_between_column_name = "MONTH"
    orders_df[months_in_between_column_name] = months_in_between_column
    print("removing rows with negative month differences")


    reference_numbers_column_name = "REFERENCE_NUMBER"
    orders_df = orders_df[[
        reference_numbers_column_name,
        months_in_between_column_name,
        x_coord_orig_column_name,
        y_coord_orig_column_name,
        x_coord_dest_column_name,
        y_coord_dest_column_name,

                            ]]
    orders_df_monthly = orders_df.explode(months_in_between_column_name)
    return orders_df_monthly

def temp_build_routes_df(args,orders_df):
    path_folder_data = args["path_folder_data"]
    path_folder_data_raw = os.path.join(path_folder_data, "raw")
    path_folder_data_temp = os.path.join(path_folder_data, "temp")

    #args = {"path_folder_data":os.path.join(path_folder,"data")}
    carrier_gdf = get_operating_zip3_buffers_gdf(args)

    county_weather_df_monthly = get_county_weather_df_monthly(args)
    area_gdf = get_county_gdf(args)
    census_df = get_county_census_df(args)

    county_weather_df_monthly = util.add_log_column_to_df(county_weather_df_monthly,"PRECIPITATION",drop_prelogged=True)
    county_weather_df_monthly = util.add_log_column_to_df(county_weather_df_monthly,"TEMPERATURE",drop_prelogged=True)

    area_gdf = util.add_log_column_to_df(area_gdf,"ALAND",drop_prelogged=True)

    census_df = util.add_log_column_to_df(census_df,"POPULATION",drop_prelogged=True)

    area_gdf = area_gdf.merge(census_df,on=["COUNTY_NAME"])
    zero_div_prevention = 0.0001
    area_gdf["POPULATION_DENSITY"] = area_gdf["LOG(POPULATION)"]/(area_gdf["LOG(ALAND)"]+zero_div_prevention)


    x_coord_orig_column_name = "X_COORD_ORIG"
    y_coord_orig_column_name = "Y_COORD_ORIG"
    x_coord_dest_column_name = "X_COORD_DEST"
    y_coord_dest_column_name = "Y_COORD_DEST" 
    point_orig_column_name = "POINT_ORIG" 
    point_dest_column_name = "POINT_DEST"

    temp_path_builder = Temp_Path_Builder(orders_df)
    point_orig_df = temp_path_builder.build_point_df(
    x_coord_column_name=x_coord_orig_column_name, 
    y_coord_column_name=y_coord_orig_column_name, 
    point_column_name=point_orig_column_name,
    point_df_name="point_orig_df",
    inplace=True
    )

    point_dest_df = temp_path_builder.build_point_df(
    x_coord_column_name=x_coord_dest_column_name, 
    y_coord_column_name=y_coord_dest_column_name, 
    point_column_name=point_dest_column_name,
    point_df_name="point_dest_df",
    inplace=True
    )

    path_df = temp_path_builder.build_path_df(
    point_orig_df=point_orig_df, 
    point_dest_df=point_dest_df, 
    inplace=True
    )

    path_gdf = temp_path_builder.build_path_gdf(inplace=True)

    ###
    print("Spatially Joining County Polygons against Shipping Route Polylines. This may take 5 to 8 minutes")
    path_area_gdf = temp_path_builder.build_path_area_gdf(area_gdf=area_gdf,inplace=False)
    path_area_df = temp_path_builder.build_path_area_df(path_area_gdf=path_area_gdf,inplace=True)
    del path_area_gdf
    print("Completed Joining County Polygons against Shipping Route Polylines")
    ###
    print("Spatially Joining Operating Zipcode Buffers against Shipping Route Polylines. This may take 5 to 8 minutes")

    path_oper_gdf = temp_path_builder.build_path_area_gdf(area_gdf=carrier_gdf,path_area_gdf_name="path_oper_gdf",inplace=False)
    print("Completed Joining Operating Zipcode Buffers against Shipping Route Polylines. This may take 5 to 8 minutes")


    path_oper_count = path_oper_gdf.groupby(
        ["X_COORD_ORIG","Y_COORD_ORIG","X_COORD_DEST","Y_COORD_DEST"],
        as_index=False
    ).agg({"OPER_COUNT": np.sum})
    print("Exploding Months per Order")
    orders_df_monthly = explode_orders_monthly(orders_df)

    orders_area_df = orders_df_monthly.merge(
        path_area_df,
        on=[x_coord_orig_column_name,y_coord_orig_column_name,
            x_coord_dest_column_name,y_coord_dest_column_name]
            )

    orders_weather_df = orders_area_df.merge(county_weather_df_monthly,on=["COUNTY_NAME","MONTH"])

    orders_df_final = orders_weather_df.groupby(
        ["REFERENCE_NUMBER"],as_index=False
    ).agg({
        "POPULATION_DENSITY": np.mean,
        "LOG(ALAND)": np.mean,
        "LOG(POPULATION)": np.mean,
        "LOG(TEMPERATURE)": np.mean,
        "LOG(PRECIPITATION)": np.mean,
        })


    orders_df_path_carriers_amount = (
        orders_df[["REFERENCE_NUMBER","X_COORD_ORIG","Y_COORD_ORIG","X_COORD_DEST","Y_COORD_DEST"]]
    ).merge(path_oper_count,
                    on=["X_COORD_ORIG","Y_COORD_ORIG","X_COORD_DEST","Y_COORD_DEST"]
    )[["REFERENCE_NUMBER","OPER_COUNT"]]
    orders_df_path_carriers_amount = util.add_log_column_to_df(orders_df_path_carriers_amount, column_name="OPER_COUNT",drop_prelogged=True)
    orders_df_final = orders_df_final.merge(orders_df_path_carriers_amount,on=["REFERENCE_NUMBER"])

    file_name_route_data = "paths_geoenriched.csv"
    path_file_route_data = os.path.join(path_folder_data_temp, file_name_route_data)
    orders_df_final.to_csv(path_file_route_data,index=False)
    return orders_df_final


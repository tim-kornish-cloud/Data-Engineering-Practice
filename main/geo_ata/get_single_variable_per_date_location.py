"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: Originally a python jupiter notebook - tp be used later

"""

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import shapefile
from shapely.geometry import mapping, Polygon
import time
import wget
import rioxarray as rio
import xarray as xr
import numpy as np
import datetime as dt
class Reader:
    def __init__(self):
        self.shapefile = None
        self.indexes = None
        self.df_features = None
        self.projections = None
        self.NORTHWEST_KNOWLEDGE = "http://www.northwestknowledge.net/metdata/data/"


    def read_shapefile(self, shapefile="./vector_input/mtbs_fod_pts_data/mtbs_FODpoints_DD.shp",
                       indexes=None,
                       db_features=None):

        self.shapefile = gpd.read_file(shapefile)
        if(indexes != None):
            self.indexes = gpd.read_file(indexes)
        if(db_features != None):
            self.db_features = pd.DataFrame(gpd.read_file(db_features))

    def output_shapefile(self, print_head=False, print_columns=False, print_extra_variables=False, close_window_after_sleep=True, plot_file=True):
        if(print_head):
            print(self.shapefile.head())
        if(print_columns):
            print(self.shapefile.columns)
        if(print_extra_variables):
            print(self.indexes.head())
            print(self.indexes.columns)
            print(self.db_features.head())
            print(self.db_features.columns)
        #print(self.projections)
        self.shapefile.plot()
        if(plot_file):
            plt.show()
            if (close_window_after_sleep):
                time.sleep(5)
                plt.close()

    def inspect_polygons(self):
        for row in self.shapefile['geometry']:
            print(row)
            """
            has list of polygons and multipolygons
            """

    def plot_polygon(self, polygons_to_plot=1):
        for index, row in self.shapefile.iterrows():
            #for col in self.shapefile.columns:
            #print(col + ": " + str(self.shapefile.iloc[index][col]) + "\n")
            print("about to plot")
            poly = gpd.GeoSeries(row.geometry)
            print(poly)
            poly.plot()
            print("plotted")
            plt.show()
            time.sleep(5)
            plt.close()

    def retrieve_state(self, state):
        poly = self.shapefile.loc[self.shapefile['NAME'].str.lower() == state.lower(), "geometry"]
        return poly

    def plot_state(self, polygon):
        polygon.plot()
        plt.show()

    def read_netcdf_from_web(self, variable, year, source=None):
        if source == None:
            source  = self.NORTHWEST_KNOWLEDGE
        url = source+str(variable)+"_"+str(year)+".nc"
        file = wget.download(url)
        return file

    def load_file_into_xarray(self, variable, year, use_downloaded_file=True):

        if(use_downloaded_file):
            filename = "input/" + variable.lower() + "_" + str(year) + ".nc"
            file_xarray = xr.open_dataset(filename)
        else:
            filename = self.read_netcdf_from_web(variable, year)
            file_xarray = xr.open_dataset(filename)
        return file_xarray
netfile = Reader()
xar = netfile.load_file_into_xarray("sph", 2020, False)
100% [......................................................................] 187171109 / 187171109
print(xar)
netfile.read_shapefile("./vector_input/US_States/cb_2018_us_state_500k/cb_2018_us_state_500k.shp", None, None)

netfile.output_shapefile(True, True, False, False, False)

montana_poly = netfile.retrieve_state("Montana")
netfile.plot_state(montana_poly)

file = netfile.read_netcdf_from_web("sph", 2020)

xr = rio.open_rasterio(file)
#xr_masked = xr.fillna(0)
xr_masked = xr.where(xr != xr.attrs['_FillValue'])
date = "2020/01/31" # "yyyy/mm/dd"
day = (dt.date(int(date[0:4]), int(date[5:7]), int(date[8:])) - dt.date(int(date[0:4]), 1, 1)).days
date = "2020/03/31" # "yyyy/mm/dd"
day2 = (dt.date(int(date[0:4]), int(date[5:7]), int(date[8:])) - dt.date(int(date[0:4]), 1, 1)).days
xr_masked[day2, :, :].plot() # 32st day of entire netcdf file for us

print(montana_poly.crs)
print(type(xr_masked))

clipped = xr_masked.rio.write_crs(montana_poly.crs).rio.clip(montana_poly.geometry.apply(mapping))

#clipped._FillValue = 0
print(clipped[150, :, :])
clipped = clipped.where(clipped != clipped.attrs['_FillValue'])
clipped[150, :, :].plot()

print(clipped[200, :, :]) # set up conversion of date to number ie january 31 = 31, 07-19 = 200
clipped[200, :, :].plot()

type(clipped)
# !pip install folium
# import folium
# from folium import plugins
print(clipped[0,:,:].head())
print(clipped[0,:,:].head().x)
print(clipped[0,:,:].head().y)
clipped[0,:,:].head().plot()

print(clipped[0,0,0])
print(clipped[0,0,0].x)
print(clipped[0,0,0].y)

print(clipped[0,1,0].x)
print(clipped[0,0,2].y)

test_y = 48.87
test_x = -115.91

def find_nearest(array, value, return_index = True):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin() - 1 # - 1 is to go to the preceding index, assuming pixel coordinate is upper left
    idx = 0 if idx < 0 else idx
    if return_index:
        return idx
    return array[idx]

# print(clipped[0,:,:].head().x)
# print(clipped[0,:,:].head().y)
found_x = find_nearest(clipped[0,:,:].head().x, test_x)
print(found_x)

found_y = find_nearest(clipped[0,:,:].head().y, test_y)
print(found_y)

found_y = find_nearest(clipped[0,:,:].head().y, test_y)
print(found_y)

pixel = clipped[0,found_y, found_x]

print(pixel.values == 399)

"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: Originally a python jupiter notebook

"""

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import shapefile
import time
class Reader:
    def __init__(self):
        self.shapefile = None
        self.indexes = None
        self.df_features = None
        self.projections = None

    def read_shapefile(self, shapefile="./vector_input/mtbs_fod_pts_data/mtbs_FODpoints_DD.shp",
                       indexes="./vector_input/mtbs_fod_pts_data/mtbs_FODpoints_DD.shx",
                       db_features="./vector_input/mtbs_fod_pts_data/mtbs_FODpoints_DD.dbf"):
        """
        All 3 inputs below result in the same dataframe. when geopandas reads a single file,
        it will also read the relevant adjacent files with same file name and different extensions.
        """
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
        """
        ['Event_ID', 'irwinID', 'Incid_Name', 'Incid_Type', 'Map_ID', 'Map_Prog',
       'Asmnt_Type', 'BurnBndAc', 'BurnBndLat', 'BurnBndLon', 'Ig_Date',
       'Pre_ID', 'Post_ID', 'Perim_ID', 'dNBR_offst', 'dNBR_stdDv', 'NoData_T',
       'IncGreen_T', 'Low_T', 'Mod_T', 'High_T', 'Comment', 'ORIG_FID',
       'geometry']
        """
        if(print_extra_variables):
            print(self.indexes.head())
            print(self.indexes.columns)
            print(self.db_features.head())
            print(self.db_features.columns)
        """
        ['Event_ID', 'irwinID', 'Incid_Name', 'Incid_Type', 'Map_ID', 'Map_Prog',
       'Asmnt_Type', 'BurnBndAc', 'BurnBndLat', 'BurnBndLon', 'Ig_Date',
       'Pre_ID', 'Post_ID', 'Perim_ID', 'dNBR_offst', 'dNBR_stdDv', 'NoData_T',
       'IncGreen_T', 'Low_T', 'Mod_T', 'High_T', 'Comment', 'ORIG_FID',
       'geometry']
        """
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
perim = Reader()
perim
<__main__.Reader at 0x1afbb4f2850>
perim.read_shapefile("./vector_input/mtbs_perimeter_data/mtbs_perims_DD.shp",
                         None, None)
perim.output_shapefile(False, False, False, False, False)
perim.plot_polygon()

import wget
import geopandas
import os
import xarray as xr
import datetime as dt
import rioxarray as rio

NORTHWEST_KNOWLEDGE = "http://www.northwestknowledge.net/metdata/data/"


class GeoDataRetriever:
    def __init__(self, source=None):
        self.NORTHWEST_KNOWLEDGE = "http://www.northwestknowledge.net/metdata/data/"
        self.source = self.NORTHWEST_KNOWLEDGE
        if source != None:
            self.source = source

    def _read_netcdf_from_web(self, variable, year, source=None):
        if source == None:
            source = self.source
        url = source+str(variable)+"_"+str(year)+".nc"
        file = wget.download(url)
        return file

    def download_file_into_xarray(self, variable, date, return_whole_year=False, date_range_start=None, date_range_end=None, replace_fill_with_nan=False):
        """Download NetCDF variable file into xarray.

        Parameters
        ----------
        variable : string
            The target location to save the raster to.
        date : string
            select exact day of year to return netCDF data
        return_whole_year : Boolean
            return whole netCDF file in single xarray, everyday of year included
        date_range_start : string
            instead of a single day, use this parameter to return a range of days, start date included.
        date_range_end=None : string
            instead of a single day, use this parameter to return a range of days, end date not included.
        replace_fill_with_nan : Boolean
            only return relevant data, cut out fillvalues from dataset

        Returns
        -------
            Xarray DataArray

        """

        year = int(date[0:4])
        file = self._read_netcdf_from_web(variable, year)
        file_xarray = rio.open_rasterio(file)
        if return_whole_year:
            return file_xarray
        if date_range_start == None and date_range_end == None and return_whole_year == False:
            day = (dt.date(int(date[0:4]), int(date[5:7]), int(
                date[8:])) - dt.date(int(date[0:4]), 1, 1)).days
            file_xarray_day = file_xarray[day, :, :]
            return file_xarray_day
        if date_range_start != None and date_range_end != None:
            #Assuming range has no errors in input()
            range_days_start = (dt.date(int(date_range_start[0:4]), int(date_range_start[5:7]), int(
                date_range_start[8:])) - dt.date(int(date[0:4]), 1, 1)).days
            range_days_end = (dt.date(int(date_range_end[0:4]), int(date_range_end[5:7]), int(
                date_range_end[8:])) - dt.date(int(date[0:4]), 1, 1)).days
            file_xarray = file_xarray[range_days_start:range_days_end, :, :]
        return file_xarray

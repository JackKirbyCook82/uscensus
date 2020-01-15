# -*- coding: utf-8 -*-
"""
Created on Sat Jan 9 2020
@name:   USCensus Table Display Application
@author: Jack Kirby Cook

"""

import visualization as vis
from utilities.geodataframes import geodataframe_fromshapefile

from uscensus.website import USCensus_APIGeography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['MapPlotter']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


class MapPlotter(object):
    def __repr__(self): return "{}(repository='{}', size={}, vintage='{}', colors='{}', roads={})".format(self.__class__.__name__, self.__repository, self.__size, self.__vintage, self.__colors, self.__roads)          
    def __init__(self, repository, size=(8,8), vintage=2018, colors='YlGn', roads=True, water=True):
        self.__repository = repository
        self.__size = size
        self.__colors = colors
        self.__vintage = str(vintage)
        self.__roads = roads
        
    def __geographyparser(self, geodataframe, *args, geography, **kwargs):
        GeographyClass = geography.__class__
        usgeographys = [USCensus_APIGeography(geokey, geovalue) for geokey, geovalue in geography.items()]           
        function = lambda values: str(GeographyClass({key:value for key, value in zip(geography.keys(), values)}))
        geodataframe.columns = map(str.lower, geodataframe.columns)          
        geodataframe['geography'] = geodataframe[[usgeography.shapegeography for usgeography in usgeographys]].apply(function, axis=1)

        geodataframe = geodataframe[['geography', 'geometry']]
        mask = geodataframe['geography'].apply(lambda x: GeographyClass.fromstr(x) in geography)
        geodataframe = geodataframe[mask]
        geodataframe = geodataframe.set_index('geography', drop=True)      
        return geodataframe 
    
    def __call__(self, table, *args, datakey=None, geography, **kwargs):
        if not datakey and len(table.datakeys) == 1: datakey = table.datakeys[0]
        else: assert datakey
        data = table.flatten().toseries(datakey, index='geography')
        span = table.spans[datakey]
          
        geo = self.geo_dataframe(geography, *args, **kwargs)
        basegeo = self.basegeo_dataframe(geography, *args, **kwargs)
        features = {'roads':self.feature_dataframe('primaryroad', geography, *args, **kwargs)}
    
        fig = vis.figures.createplot(self.__size, title=table.name)   
        ax = vis.figures.createax(fig, x=1, y=1, pos=1)
        vis.figures.setnames(ax, names={'x':'Latitude', 'y':'Longitude'})    
        vis.plots.map_plot(ax, data, *args, geo=geo, basegeo=basegeo, **features, colors=self.__colors, span=span, **kwargs)   
        vis.figures.addcolorbar(fig, span, *args, orientation='vertical', color=self.__colors, **kwargs)
        vis.figures.showplot(fig)

    def geo_dataframe(self, geography, *args, **kwargs):
        geo = geodataframe_fromshapefile(geography.getkey(-1), *args, geography=geography, year=self.__vintage, directory=self.__repository, **kwargs)
        return self.__geographyparser(geo, *args, geography=geography, **kwargs)
    
    def basegeo_dataframe(self, geography, *args, **kwargs):
        GeographyClass = geography.__class__
        basegeography = GeographyClass({key:value for key, value in geography.items() if value != geography.allChar})
        basegeo = geodataframe_fromshapefile(basegeography.getkey(-1), *args, geography=basegeography, year=self.__vintage, directory=self.__repository, **kwargs)
        return self.__geographyparser(basegeo, *args, geography=basegeography, **kwargs)
    
    def feature_dataframe(self, feature, geography, *args, **kwargs):
        try: return geodataframe_fromshapefile(feature, *args, geography=geography, year=self.__vintage, directory=self.__repository, **kwargs)['geometry']   
        except: return None
            
        
        



















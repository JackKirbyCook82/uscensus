# -*- coding: utf-8 -*-
"""
Created on Weds Sept 11 2019
@name:   USCensus WebAPI
@author: Jack Kirby cook

"""

import os.path
import io
import zipfile
import pandas as pd

from utilities.dataframes import dataframe_fromfile, dataframe_tofile, dataframe_fromjson, geodataframe_fromdir

from uscensus.urlapi import USCensus_Geography

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_WebAPI', 'USCensus_ShapeAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_DATEFORMATS = {'geoseries':'%Y', 'yearseries':'%Y', 'timeseries':'%Y-%m'}
_LABELDELIMITER = '!!'
_LABELALL = '*'
_FILENAME = '{tableID}_{survey}_{date}_{geoid}.csv'


def data_parser(item):
    if pd.isnull(item): return item
    try: return int(float(item)) if not bool(float(item) % 1) else float(item)
    except ValueError: return str(item)

def merge_geography(dataframe, *args, geography, **kwargs):
    GeographyClass = geography.__class__
    usgeographys = [USCensus_Geography(geokey, geovalue) for geokey, geovalue in geography.items()]    
    dataframe = dataframe.rename({usgeography.apigeography:usgeography.geography for usgeography in usgeographys}, axis='columns')
    
    geokeys = list(geography.keys())
    function = lambda geovalues: str(GeographyClass({key:value for key, value in zip(geokeys, geovalues)}))
    dataframe['geography'] = dataframe[geokeys].apply(function, axis=1)
    dataframe = dataframe.drop(geokeys, axis=1)
    return dataframe

def merge_date(dataframe, *args, date, **kwargs):
    dataframe['date'] = str(date)
    return dataframe

def merge_concepts(dataframe, *args, universe, index, header, concepts, **kwargs):
    if header: 
        assert len(concepts) < 1
        dataframe = dataframe.melt(id_vars=[column for column in dataframe.columns if column not in concepts], var_name=header, value_name=universe)
    else:      
        assert len(concepts) == 1
        dataframe = dataframe.rename({concepts[0]:universe})
    dataframe[universe] = dataframe[universe].apply(data_parser)
    return dataframe

def merge_scope(dataframe, *args, scope, **kwargs):    
    for key, value in scope.items(): 
        if key not in dataframe.columns: dataframe[key] = value
    return dataframe
    

class USCensus_WebAPI(object):
    def __repr__(self): return '{}(repository={}, saving={})'.format(self.__class__.__name__, self.__repository, self.__saving)    
    def __init__(self, repository, urlapi, webreader, saving=True):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__repository = repository
        self.__saving = saving
        
    @property
    def series(self): return self.__urlapi.series
    @property
    def survey(self): return self.__urlapi.survey
    
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository
    
    def filename(self, *args, tableID, survey, geography, date, estimate=5, **kwargs):
        return _FILENAME.format(tableID=tableID, survey=survey.format(estimate=estimate), geoid=geography.geoid, date=date)
    def file(self, *args, **kwargs): 
        return os.path.join(self.repository, self.filename(*args, **kwargs))
  
    def __query(self, *args, group, labels, **kwargs):              
        groupurl = self.urlapi.query(*args, query=['groups', group], filetype='json', **kwargs)
        groupjson = self.webreader(groupurl, *args, **kwargs)
        groupdata = {key:tuple(value['label'].split(_LABELDELIMITER)) for key, value in groupjson['variables'].items()}
        
        labels = [label.split(_LABELDELIMITER) for label in labels]  
        directlabels = [label for label in labels if labels[-1] != _LABELALL]
        alllabels = [label[:-1] for label in labels if labels[-1] == _LABELALL]
        labels = [*directlabels, *[concept for concept in groupdata.values() if concept[:-1] in alllabels]]
        
        tags_to_concepts = {tag:concept[-1] for tag, concept in groupdata.items() if concept in labels}
        return tags_to_concepts
    
    def __request(self, *args, tags, preds, **kwargs):
        tagsurl = self.urlapi(*args, tags=['NAME',*tags], preds=preds, **kwargs)
        tagsjson = self.webreader(tagsurl, *args, **kwargs)
        return dataframe_fromjson(tagsjson, header=0, forceframe=True)

    def __parser(self, dataframe, *args, tags_to_concepts, **kwargs):
        dataframe = dataframe.rename(tags_to_concepts , axis='columns')  
        dataframe['geoname'] = dataframe['NAME'].apply(lambda geoname: '|'.join(geoname.split(', ')))
        dataframe = dataframe.drop('NAME', axis=1)
        
        dataframe = merge_geography(dataframe, *args, **kwargs)
        dataframe = merge_concepts(dataframe, *args, concepts=tags_to_concepts.values(), **kwargs)
        dataframe = merge_date(dataframe, *args, **kwargs)
        dataframe = merge_scope(dataframe, *args, **kwargs)
        return dataframe

    def load(self, *args, **kwargs): 
        file = self.file(*args, **kwargs)
        return dataframe_fromfile(file, index=None, header=0, forceframe=True)    
    
    def save(self, webtable, *args, **kwargs): 
        file = self.file(*args, **kwargs)
        dataframe_tofile(file, webtable, index=False, header=True)        
        
    def download(self, *args, **kwargs):
        tags_to_concepts = self.__query(*args, **kwargs)
        table = self.__request(*args, tags=tags_to_concepts.keys(), **kwargs)
        table = self.__parser(table, *args, tags_to_concepts=tags_to_concepts, **kwargs)
        return table
    
    def generator(self, *args, geography, dates, estimate=5, **kwargs):
        for date in dates:
            date.setformat(_DATEFORMATS[self.series])
            try: dataframe = self.load(*args, geography=geography, date=date, estimate=estimate, **kwargs)
            except FileNotFoundError: 
                dataframe = self.download(*args, geography=geography, date=date, estimate=estimate, **kwargs)  
                if self.__saving: self.save(dataframe, *args, geography=geography, date=date, estimate=estimate, **kwargs)
            yield dataframe

    def __call__(self, tableID, *args, **kwargs):
        try: 
            dataframes = [dataframe for dataframe in self.generator(*args, **kwargs)]           
            dataframe = pd.concat(dataframes, axis=0)
            return dataframe
        except Exception as error:
           raise error


class USCensus_ShapeAPI(object):
    def __repr__(self): return '{}(repository={})'.format(self.__class__.__name__, self.__repository)  
    def __init__(self, repository, urlapi, webreader):
        self.__urlapi = urlapi
        self.__webreader = webreader
        self.__repository = repository       
        
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader
    @property
    def repository(self): return self.__repository        

    def downloaded(self, shape, *args, **kwargs): return os.path.exists(self.directory(shape, *args, **kwargs))
    
    def directory(self, shape, *args, geography, date, **kwargs): 
        usgeography = USCensus_Geography(shape)
        try: year = str(date.year)
        except: year = str(date)
        directoryname = usgeography.shapefile.format(year=year, state=geography[:1].geoid, county=geography[:2].geoid)
        return os.path.join(self.repository, directoryname)

    def __parser(self, shape, geodataframe, *args, geography, **kwargs):
        GeographyClass = geography.__class__
        usgeographys = [USCensus_Geography(geokey, geovalue) for geokey, geovalue in geography.items()]           
        function = lambda values: str(GeographyClass({key:value for key, value in zip(geography.keys(), values)}))
        geodataframe.columns = map(str.lower, geodataframe.columns)          
        geodataframe['geography'] = geodataframe[[usgeography.shapegeography for usgeography in usgeographys]].apply(function, axis=1)

        geodataframe = geodataframe[['geography', 'geometry']]
        mask = geodataframe['geography'].apply(lambda x: GeographyClass.fromstr(x) in geography)
        geodataframe = geodataframe[mask]
        geodataframe = geodataframe.set_index('geography', drop=True)      
        return geodataframe
    
    def __itemparser(self, shape, shapedataframe, *args, **kwargs):
        return shapedataframe['geometry']
    
    def download(self, shape, *args, **kwargs):
        url = self.urlapi(*args, shape=shape, **kwargs)
        shapezipfile = self.webreader(url, *args, **kwargs)
        shapecontent = zipfile.ZipFile(io.BytesIO(shapezipfile))
        shapecontent.extractall(path=self.directory(*args, shape=shape, **kwargs))
        
    def load(self, shape, *args, **kwargs):
        dataframe = geodataframe_fromdir(self.directory(shape, *args, **kwargs))   
        return dataframe

    def geotable(self, *args, geography, **kwargs):
        shape = geography.getkey(-1)
        if not self.downloaded(shape, *args, geography=geography, **kwargs): self.download(shape, *args, geography=geography, **kwargs)
        geodataframe = self.load(shape, *args, geography=geography, **kwargs)
        geodataframe = self.__parser(shape, geodataframe, *args, geography=geography, **kwargs)        
        return geodataframe
    
    def basetable(self, *args, geography, **kwargs):
        shape = geography.getkey(-2)
        if not self.downloaded(shape, *args, geography=geography, **kwargs): self.download(shape, *args, geography=geography, **kwargs)
        basedataframe = self.load(shape, *args, geography=geography[:-1], **kwargs)
        basedataframe = self.__parser(shape, basedataframe, *args, geography=geography[:-1], **kwargs)
        return basedataframe
        
    def itemtable(self, *args, shape, **kwargs):
        if not self.downloaded(shape, *args, **kwargs): self.download(shape, *args, **kwargs)   
        shapedataframe = self.load(shape, *args, **kwargs)
        shapedataframe = self.__itemparser(shape, shapedataframe, *args, **kwargs)
        return shapedataframe
    
    def __call__(self, *args, **kwargs):
        return self.geotable(*args, **kwargs), self.basetable(*args, **kwargs)
    
    def __getitem__(self, shape):        
        def wrapper(*args, **kwargs): return self.itemtable(*args, shape=shape, **kwargs)
        return wrapper


            


        
        
        
        
        
        
        
        
        
        
        
        
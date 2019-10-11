# -*- coding: utf-8 -*-
"""
Created on Weds Sept 11 2019
@name:   USCensus Query
@author: Jack Kirby cook

"""

import json
import pandas as pd

from utilities.dataframes import dataframe_fromfile, dataframe_parser

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Query']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""
    

_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_isnull = lambda value: pd.isnull(value) if not isinstance(value, (list, tuple, dict)) else False


class USCensus_Query(object):
    def __init__(self, file, parsers):
        data = dataframe_fromfile(file, index=None, header=0, forceframe=True)  
        data = dataframe_parser(data, parsers=parsers, defaultparser=None)
        data.set_index('tableID', drop=True, inplace=True)
        self.__file = file
        self.__data = data        
        self.reset()
        
    @property
    def status(self): 
        selections = {key:value for key, value in {'universe':self.__universe, 'index':self.__index, 'header':self.__header, **self.__scope}.items() if value}
        return ' '.join(['USCensus Query', json.dumps(selections, sort_keys=False, indent=3, separators=(',', ' : '))])  
    
    def __repr__(self): return '{}(file={})'.format(self.__class__.__name__, self.__file)    
    def asdict(self): return {tableID:{key:value for key, value in values.items() if not _isnull(value)} for tableID, values in self.__data.transpose().to_dict().items()}
       
    def __str__(self): 
        dataframe = self.dataframe()
        dataframe = dataframe[['universe', 'index', 'header', 'scope']]
        dataframe = dataframe['scope'].apply(lambda x: ', '.join(['='.join([key, x[key]]) for key in set(x.keys())]))
        return '\n'.join(['USCensus Query' , str(dataframe)]) 

    def dataframe(self):
        dataframe = self.__data
        if self.__universe: dataframe = dataframe[dataframe['universe'] == self.__universe]
        if self.__index: dataframe = dataframe[dataframe['index'] == self.__index]
        if self.__header: dataframe = dataframe[dataframe['header'] == self.__header]
        for key, value in self.__scope.items(): 
            if key in dataframe['scope'].keys() and value: dataframe = dataframe[dataframe['scope'][key] == value]
        dataframe = dataframe.dropna(axis=0, how='all').dropna(axis=1, how='all')
        try: dataframe = dataframe.to_frame()
        except: dataframe = dataframe
        return dataframe

    # SELECTION    
    def setuniverse(self, universe): self.__universe = universe
    def setindex(self, index): self.__index = index
    def setheader(self, header): self.__header = header
    def setscope(self, **scope): self.__scope.update(scope)

    def reset(self): 
        self.__universe = None
        self.__index = None
        self.__header = None
        self.__scope = {}
        
    # ENGINE
    @property
    def tableIDs(self): return self.dataframe().index.values

    def __getitem__(self, tableID): return self.asdict()[tableID]
    def __call__(self, *args, universe=None, index=None, header=None, **scope): 
        self.reset()
        self.setuniverse(universe)
        self.setindex(index)
        self.setheader(header)
        self.setscope(**scope)

        
        
        
        
        
        
        
        
        
        
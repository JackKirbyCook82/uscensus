# -*- coding: utf-8 -*-
"""
Created on Weds Sept 11 2019
@name:   USCensus Query
@author: Jack Kirby cook

"""

import json
import pandas as pd
from collections import OrderedDict as ODict

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
        tabledata = dataframe_fromfile(file, index=None, header=0, forceframe=True)  
        tabledata = dataframe_parser(tabledata, parsers=parsers, defaultparser=None)
        tabledata.set_index('tableID', drop=True, inplace=True)
        self.__file = file
        self.__tablesdata = tabledata        
        self.reset()
        
    @property
    def status(self): 
        selections = {key:value for key, value in {**self.__tableselections, **self.__scopeselections}.items() if value}
        return ' '.join([self.__class__.__name__, json.dumps(selections, sort_keys=False, indent=3, separators=(',', ' : '))])  
    
    def __repr__(self): return '{}(file={})'.format(self.__class__.__name__, self.__file)    
    def asdict(self): return {tableID:{key:value for key, value in values.items() if not _isnull(value)} for tableID, values in self.__tablesdata.transpose().to_dict().items()}
       
    def __str__(self): 
        dataframe = self.dataframe()
        dataframe = dataframe[[*self.tablekeys, *[scopekey for scopekey in self.scopekeys if scopekey in dataframe.columns]]]
        return '\n'.join([self.__class__.__name__ , str(dataframe)]) 

    def dataframe(self):
        dataframe = self.__tablesdata
        for key, values in self.__tableselections.items():
            if values: dataframe = dataframe.query('{key}==@values'.format(key=key))
        for key, values in self.__scopeselections.items(): 
            if values: dataframe = dataframe.query('{key}==@values'.format(key=key))
        dataframe = dataframe.dropna(axis=0, how='all').dropna(axis=1, how='all')
        try: dataframe = dataframe.to_frame()
        except: dataframe = dataframe
        return dataframe
        
    @property
    def tablekeys(self): return ['universe', 'index', 'header']
    @property
    def webkeys(self): return ['survey', 'group', 'preds', 'label']
    @property
    def filekeys(self): return ['filename']
    @property
    def scopekeys(self): return [column for column in self.__tablesdata.columns if column not in set([*self.tablekeys, *self.filekeys, *self.webkeys])]

    def tableParms(self, tableID): return {key:value for key, value in self.asdict()[tableID].items() if key in self.tablekeys}
    def scopeParms(self, tableID): return {key:value for key, value in self.asdict()[tableID].items() if key in self.scopekeys}
    def webParms(self, tableID): return {key:value for key, value in self.asdict()[tableID].items() if key in self.webkeys}
    def fileParms(self, tableID): return {key:value for key, value in self.asdict()[tableID].items() if key in self.filekeys}

    # SELECTION    
    def setuniverse(self, universe): self.__tableselections[self.tablekeys[0]] = universe
    def setindex(self, index): self.__tableselections[self.tablekeys[1]] = index
    def setheader(self, header): self.__tableselections[self.tablekeys[2]] = header
    def setscope(self, **scope): self.setitems(**scope)

    def setitems(self, **kwargs): 
        for key, value in kwargs.items(): 
            if key in self.__tableselections.keys(): self.__tableselections[key] = value 
            else: self.__scopeselections[key] = value

    def reset(self): 
        self.__tableselections = ODict([(key, None) for key in self.tablekeys])
        self.__scopeselections = ODict([(key, None) for key in self.scopekeys]) 
        
    # ENGINE
    def __len__(self): return len(self.tableIDs())
    def tableIDs(self): return self.dataframe().index.values

    def __iter__(self): 
        for tableID in self.tableIDs():
            yield tableID, {**self.tableParms(tableID), **self.webParms(tableID), **self.fileParms(tableID), 'scope':self.scopeParms(tableID)}
      
    def __getitem__(self, tableID):
        return {**self.tableParms(tableID), **self.webParms(tableID), **self.fileParms(tableID), 'scope':self.scopeParms(tableID)}
        
    def __call__(self, *args, universe=None, index=None, header=None, **scope): 
        self.reset()
        self.setuniverse(universe)
        self.setindex(index)
        self.setheader(header)
        self.setscope(**scope)
        
        print(self.status, '\n')
        print(str(self), '\n')
            
        
        
        
        
        
        
        
        
        
        
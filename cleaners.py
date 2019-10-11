# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 2019
@name:   USCensus Cleaners
@author: Jack Kirby Cook

"""

from abc import ABC, abstractmethod
import os.path
from parse import parse
import math

from utilities.dataframes import dataframe_fromfile

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Header_Parser']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


_DIR = os.path.dirname(os.path.realpath(__file__))
_HEADERS = dataframe_fromfile(os.path.join(_DIR, 'geography.csv'), index=None, header=0, forceframe=True) 

_numformat = lambda numvalue, digit: int(math.ceil(int(numvalue)/digit)) * digit


class USCensus_Parser(ABC):
    def __repr__(self): return '{}()'.format(self.__class__.__name__)
    def __call__(self, dataframe, *args, **kwargs): return self.execute(dataframe, *args, **kwargs)
    @abstractmethod
    def execute(self, dataframe, *args, **kwargs): pass


class USCensus_Header_Parser(USCensus_Parser):
    def parser(self, strvalue, labels, concepts, roundings):
        strvalue = strvalue.replace(" ", "").replace(",", "").lower()
        for label, concept, rounding in zip(labels, concepts, roundings):
            if strvalue == label: return concept
            else:
                unformated = parse(label, strvalue)
                if unformated is not None: return concept.format(**{key:_numformat(value, rounding) for key, value in unformated.named.items()})
        raise ValueError(strvalue)
    
    def execute(self, dataframe, *args, header, **kwargs): 
        headers = _HEADERS[_HEADERS['header'] == header]
        labels, concepts, roundings = headers['label'].values, headers['concept'].values, headers['rounding'].values
        dataframe[header] = dataframe[header].apply(lambda x: self.parser(x, labels, concepts, roundings))
        return dataframe
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
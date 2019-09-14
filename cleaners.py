# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 2019
@name:   USCensus Cleaners
@author: Jack Kirby Cook

"""

from parse import parse
from functools import update_wrapper

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Parser']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


INCOME_FORMATS = {'${}ormore': lambda x: [x, None], 
                  '${}to${}': lambda x, y: [x, y], 
                  'lessthan${}': lambda x: [None, x]}
VALUE_FORMATS = {'${}ormore': lambda x: [x, None], 
                 '${}to${}': lambda x, y: [x, y], 
                 'lessthan${}': lambda x: [None, x]}
RENT_FORMATS = {'${}ormore': lambda x: [x, None], 
                '${}to${}': lambda x, y: [x, y], 
                'lessthan${}': lambda x: [None, x], 
                'nocashrent': lambda: [0, 0]}
AGE_FORMATS = {'{}yearsandover': lambda x: [x, None], 
               '{}to{}years': lambda x, y: [x, y], 
               'under{}years': lambda x: [None, x]}


_aslist = lambda items: [item for item in items] if hasattr(items, '__iter__') and not isinstance(items, str) else [items]
_format = lambda numvalues, numformat: numformat.format(*_aslist(numvalues))
_unformat = lambda strvalue, numformat: list(parse(numformat, strvalue))
    

class USCensus_Parser(object):
    def __init__(self, variables): self.__variables = variables    
    def __getitem__(self, column): return self.registry(column)       

    def __call__(self, dataframe, *args, **kwargs):
        for column, parser in self.registry.items():
            dataframe[parser] = dataframe[parser].apply(parser, *args, **kwargs)
        return dataframe
        
    # REGISTER FUNCTIONS
    __registry = {}
    @classmethod
    def registry(cls, column): return cls.__registry[column]
    @classmethod
    def update(cls, **column_parser): return cls.__registry.update(column_parser)
    
    @classmethod
    def register(cls, column):
        def decorator(function):
            def wrapper(strvalue, *args, **kwargs):    
                assert isinstance(strvalue, str)
                strvalue = strvalue.replace(" ", "").lower()
                return function(strvalue, *args, **kwargs)
            cls.update(**{column:function})
            update_wrapper(wrapper, function)
            return wrapper
        return decorator


@USCensus_Parser.register('income')
def income_parser(strvalue, *args, **kwargs):    
    pass
    

@USCensus_Parser.register('value')
def value_parser(strvalue, *args, **kwargs):    
    pass    
    
    
@USCensus_Parser.register('rent')
def rent_parser(strvalue, *args, **kwargs):
    pass


@USCensus_Parser.register('age')
def age_parser(strvalue, *args, **kwargs):
    pass
    
    












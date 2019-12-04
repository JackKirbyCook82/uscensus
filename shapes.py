# -*- coding: utf-8 -*-
"""
Created on Tues Dec 3 2019
@name:   USCensus Shapes WebAPI Object
@author: Jack Kirby Cook

"""

from webdata.url import URLAPI
from webdata.webapi import WebAPI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['USCensus_Shapes_URLAPI', 'USCensus_Shapes_WebAPI']
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


class USCensus_Shapes_URLAPI(URLAPI):
    pass


class USCensus_Shapes_WebAPI(WebAPI):
    def __init__(self, repository, urlapi, webreader, saving=False):
        self.__urlapi = urlapi
        self.__webreader = webreader
        super().__init__('USCensusShapes', repository=repository, saving=saving)
        
    @property
    def urlapi(self): return self.__urlapi
    @property
    def webreader(self): return self.__webreader

    def filename(self, *args, **kwargs):
        pass

    def generator(self, *args, **kwargs):
        pass   


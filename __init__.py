# -*- coding: utf-8 -*-
"""
Created on Thu Dec 5 2019
@name:   USCensus Website Calculation Application
@author: Jack Kirby Cook

"""

import sys
import os.path

import tables as tbls
import visualization as vis
from tables.processors import Calculation, Renderer
from parsers import BoolParser, ListParser, DictParser
from variables import Geography, Date
from utilities.inputparsers import InputParser

from uscensus.microrvi import microrvi_calculation
from uscensus.macrorvi import macrorvi_calculation
from uscensus.display import MapPlotter

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['calculation']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(DIR, os.pardir))
SHAPE_DIR = os.path.join(ROOT_DIR, 'resources', 'shapefiles')
EXCEL_FILE = os.path.join(ROOT_DIR, 'USCensusTables.xlsx')

renderer = Renderer(style='double', extend=1)
mapplotter = MapPlotter(SHAPE_DIR, size=(16,8), vintage=2018, colors='YlGn', roads=True, water=True)
calculation = Calculation('uscensus', name='USCensus Calculation')
calculation += microrvi_calculation
calculation += macrorvi_calculation
calculation()


def display(table, tree, *inputArgs, **inputParms):  
    if table: print(str(tree))
    if table: print(str(table)) 
    if table: print(str(table.variables))  

def create_spreadsheet(table, *inputArgs, spreadsheet=False, **inputParms): 
    if spreadsheet: table.flatten().toexcel(EXCEL_FILE)

def create_mapplot(table, *inputArgs, mapplot=False, **inputParms): 
    if mapplot: mapplotter(table, *inputArgs, **inputParms)


def main(*inputArgs, tableID, **inputParms):     
    print(str(inputparser), '\n')  
    print(str(calculation), '\n')
    
    if tableID in calculation: 
        tree = renderer(calculation[tableID])
        table = calculation[tableID](*inputArgs, **inputParms)
        create_spreadsheet(table, *inputArgs, **inputParms)
        create_mapplot(table, *inputArgs, **inputParms)   
        display(table, tree, *inputArgs, **inputParms)
    else: print("{} Table Not Supported: '{}'".format(calculation.name, tableID))
    
   
if __name__ == '__main__':  
    tbls.set_options(linewidth=110, maxrows=40, maxcolumns=10, threshold=100, precision=3, fixednotation=True, framechar='=')
    vis.set_options(axisfontsize=8, legendfontsize=8, titlefontsize=12)
    tbls.show_options()
    vis.show_options()
    
    input_dictparser = DictParser(pattern=',|')
    input_listparser = ListParser(pattern=',')
    input_boolparser = BoolParser()
    geography_parser = lambda item: Geography(input_dictparser(item))
    dates_parser = lambda item: [Date.fromstr(value, dateformat='%Y') for value in input_listparser(item)] 
    variable_parsers = {'geography':geography_parser, 'dates':dates_parser, 'mapplot':input_boolparser, 'spreadsheet':input_boolparser}
    inputparser = InputParser(assignproxy='=', spaceproxy='_', parsers=variable_parsers)    
    
    print(repr(inputparser))
    print(repr(renderer))
    print(repr(mapplotter))
    print(repr(calculation), '\n')  
    
    sys.argv.extend(['tableID=avginc|geo',
                     'mapplot=True',
                     'spreadsheet=False',
                     'geography=state|06,county|029,tract|*,block|*', 
                     'dates=2015,2016,2017'])
    inputparser(*sys.argv[1:])
    main(*inputparser.inputArgs, **inputparser.inputParms)
    
    
    
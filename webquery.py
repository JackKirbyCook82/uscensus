# -*- coding: utf-8 -*-
"""
Created on Sat Oct 5 2019
@name:   USCensus Website Query Application
@author: Jack Kirby Cook

"""

import sys
import os.path

import tables as tbls
from utilities.inputparsers import InputParser
from parsers import DictParser, ListofTupleParser
from utilities.query import Query, EmptyQueryError

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ['query']
__copyright__ = "Copyright 2019, Jack Kirby Cook"
__license__ = ""


DIR = os.path.dirname(os.path.realpath(__file__))
QUERY_FILE = os.path.join(DIR, 'webtables.csv')

queryparsers = {'dataset': DictParser(pattern=';='), 'preds': DictParser(pattern=';='), 'variables': ListofTupleParser(pattern=';|'), 'labels':ListofTupleParser(pattern=';|'), 'scope':DictParser(pattern=';=')}
query = Query(QUERY_FILE, queryparsers, name='USCensus')


def main(*args, **kwargs): 
    stop = False
    while not stop:
        for key, value in inputparser.inputParms.items(): query[key] = value
        print(str(query), '\n')
        try: print(query.display(), '\n')
        except EmptyQueryError: print('{} Query Empty'.format(query.name))
        
        inputparser.reset()
        sysinput = input('{} Query Input ==> '.format(query.name))
        print()
        inputparser(*sysinput.split(' ')) 
        
        commands = [item.lower() for item in inputparser.inputArgs]
        if 'reset' in commands: query.reset()
        if 'exit' in commands: stop=True  
        if 'quit' in commands: stop=True                 
      
    
if __name__ == '__main__':  
    tbls.set_options(maxrows=40, maxcolumns=10)
    tbls.show_options()
    
    inputparser = InputParser(assignproxy='=', spaceproxy='_')
    
    print(repr(inputparser))
    print(repr(query), '\n')
        
    sys.argv.extend(['universe=', 'index=', 'header='])
    inputparser(*sys.argv[1:])
    main()
    
    
    
    
    



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
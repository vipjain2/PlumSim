from os import stat
import numpy as np
import time
from enum import Enum
import re
from re import match, search, findall

class Commands( object ):
    def __init__( self ) -> None:
        super().__init__()
        self.tokens = [ ( r"[^a-zA-Z]([Mm][Aa])(\d\d?\d?)", "movingAvg" ),
                        ( r"[^a-zA-Z]([Ee][Mm][Aa])(\d\d?\d?)", "expMovingAvg" ),
                        ( r"[^a-zA-Z]([Aa][Dd][Rr])(\d\d?\d?)?", "adr" ),
                        ( r"[^a-zA-Z](PrevClose)(\d\d?\d?)?", "prevClose" ),
                        ( r"[^a-zA-Z](PrevDayHigh|PrevHigh)(\d\d?\d?)?", "prevHigh" ),
                        ( r"[^a-zA-Z](PrevDayLow|PrevLow)(\d\d?\d?)?", "prevLow" ),
                        ( r"[^a-zA-Z](GapOpen)()", "gapOpen" ),
                        ( r"[^a-zA-Z](PrevOpenCloseRange)(\d\d?\d?)?", "prevOpenCloseRange" ),
                        ( r"[^a-zA-Z](PrevRange)(\d\d?\d?)?", "prevRange" ) ] 
    
    def compile( self, code, data ):
        indicators = []
        for token in self.tokens:
            regex, fname = token
            allMatches = set( findall( regex, code ) )
            for m in allMatches:
                indicators += [ ''.join( m ) ]
                func = getattr( self, fname )
                func( data, *m )
        print( f"compiled {indicators}" )

    @staticmethod
    def processLabel( label, n ):
        if not n:
            name = label
        else:
            name = f"{label}{n}"
            n = eval( n )
        return ( name, n )

    @staticmethod
    def movingAvg( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data[ 'Close' ].rolling( n ).mean()
    
    @staticmethod
    def expMovingAvg( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data[ 'Close' ].ewm( span=n ).mean()

    @staticmethod
    def adr( data, label, period ):
        if not period:
            period = 20
            name = f"{label}"
        else:
            name = f"{label}{period}"
            period = eval( period )
        cleanup = []
        if 'Range' not in data:
            data[ 'Range' ] = data[ 'High' ] / data[ 'Low' ]
            cleanup = [ 'Range' ]
        
        data[ name ] = round( ( data[ 'Range' ].rolling( period ).mean() - 1 ), 2 )
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def trend( data, label, n ):
        name, n = Commands.processLabel( label, n )
        ma = f"MA{n}"
        data[ name ] = data[ ma ].ewm( span=5 ).mean()

    @staticmethod
    def prevClose( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data.shift( axis=0 )[ "Close" ]
    
    @staticmethod
    def prevOpen( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data.shift( axis=0 )[ "Open" ]

    @staticmethod    
    def prevHigh( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data.shift( axis=0 )[ "High" ]

    @staticmethod    
    def prevLow( data, label, n ):
        name, n = Commands.processLabel( label, n )
        data[ name ] = data.shift( axis=0 )[ "Low" ]

    @staticmethod
    def gapOpen( data, label, *kargs ):
        cleanup = []
        if 'PrevClose' not in data:
            Commands.prevClose( data, "PrevClose", "" )
            cleanup += [ 'PrevClose' ]
        
        data[ 'GapOpen' ] = ( data[ 'Open' ] - data[ 'PrevClose' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def prevOpenCloseRange( data, label, *kargs ):
        cleanup = []
        if 'PrevClose' not in data:
            Commands.prevClose( data, "PrevClose", "" )
            cleanup += [ 'PrevClose' ]
        if 'PrevOpen' not in data:
            Commands.prevOpen( data, 'PrevOpen', "" )
            cleanup += [ 'PrevOpen' ]

        data[ 'PrevOpenCloseRange' ] = ( data[ 'PrevClose' ] - data[ 'PrevOpen' ] ) / data[ 'PrevClose' ]
        data.drop( cleanup, axis=1, inplace=True )

    @staticmethod
    def prevRange( data, label, *kargs ):
        cleanup = []
        if 'PrevHigh' not in data:
            Commands.prevHigh( data, "PrevHigh", "" )
            cleanup += [ 'PrevHigh' ]
        if 'PrevLow' not in data:
            Commands.prevLow( data, 'PrevLow', "" )
            cleanup += [ 'PrevLow' ]

        data[ 'PrevRange' ] = ( data[ 'PrevHigh' ] - data[ 'PrevLow' ] ) / data[ 'PrevLow' ]
        data.drop( cleanup, axis=1, inplace=True )
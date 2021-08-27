from os import stat
import numpy as np
import time
from enum import Enum
import re
from re import match, search, findall

class Commands( object ):
    def __init__( self ) -> None:
        super().__init__()
        self.tokens = [ ( r"[^a-zA-Z]([Mm][Aa])(\d\d?)", "movingAvg" ),
                        ( r"[^a-zA-Z]([Ee][Mm][Aa])(\d\d?)", "expMovingAvg" ),
                        ( r"[^a-zA-Z]([Aa][Dd][Rr])(\d\d?)?", "expMovingAvg" ),
                        ( r"[^a-zA-Z](PrevClose)(\d\d?)?", "prevClose" ),
                        ( r"[^a-zA-Z](PrevDayHigh|PrevHigh)(\d\d?)?", "prevHigh" ),
                        ( r"[^a-zA-Z](GapOpen)()", "gapOpen" ) ] 
    
    def compile( self, code, data ):
        for token in self.tokens:
            regex, fname = token
            allMatches = set( findall( regex, code ) )
            for m in allMatches:
                print( "calculating {}".format( ''.join( m ) ) )
                func = getattr( self, fname )
                func( data, *m )

    @staticmethod
    def movingAvg( data, label, period ):
        name = f"{label}{period}"
        period = eval( period )
        data[ name ] = data[ 'Close' ].rolling( period ).mean()
    
    @staticmethod
    def expMovingAvg( data, label, period ):
        name = f"{label}{period}"
        period = eval( period )
        data[ name ] = data[ 'Close' ].ewm( span=period ).mean()

    @staticmethod
    def adr( data, label, period=20 ):
        if not period:
            period = 20
            name = f"{label}"
        else:
            name = f"{label}{period}"
            period = eval( period )

        data[ 'Range' ] = data[ 'High' ] / data[ 'Low' ]
        data[ name ] = round( ( data[ 'Range' ].rolling( period ).mean() - 1 ) * 100, 2 )

    @staticmethod
    def trend( data, label, period ):
        name = f"{label}{period}"
        ma = f"MA{period}"
        data[ name ] = data[ ma ].ewm( span=5 ).mean()

    @staticmethod
    def prevClose( data, label, n ):
        data[ "PrevClose" ] = data.shift( axis=0 )[ "Close" ]
    
    @staticmethod    
    def prevHigh( data, label, n ):
        if not n:
            name = label
        else:
            name = f"{label}{n}"
            n = eval( n )
        data[ name ] = data.shift( axis=0 )[ "High" ]

    @staticmethod
    def gapOpen( data, label, *kargs ):
        if 'PrevClose' not in data:
            Commands.prevClose( data, "PrevClose", "" )
        data[ 'GapOpen' ] = ( data[ 'Open' ] - data[ 'PrevClose' ] ) / data[ 'PrevClose' ]
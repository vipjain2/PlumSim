
import pandas as pd
import numpy as np
from ticker_data import Data_loader

class Trader( object ):

    global STOP_LOSS
    global GAP_UP
    global START_DATE
    global END_DATE
    global DATA_DIR

    def __init__( self, ticker, buy_strategy, sell_strategy ):
        self._ticker = ticker
        self.buy_strategy = buy_strategy
        self.sell_strategy = sell_strategy

        self.df = Data_loader( ticker, DATA_DIR ).data( START_DATE, END_DATE )
        self.setup()
        self.trades = None
        self.calc_trades()
        self.cleanup()

    def setup( self ):
        self.df.rename( columns={   "date": "Date", 
                                    "close": "Close", 
                                    "open": "Open",
                                    "low": "Low", 
                                    "high": "High" }, inplace=True )

        self.df.sort_values( by=[ "Date" ], inplace=True )
        self.df.reset_index( drop=True, inplace=True )

        self.df[ "Date" ] = pd.to_datetime( self.df[ "Date" ] )
        self.df[ "PrevClose" ] = self.df.shift( axis=0 )[ "Close" ]
        self.df[ "PrevDayHigh" ] = self.df.shift( axis=0 )[ "High" ]
        self.df[ "GapOpenClose" ] = self.df.apply( lambda row : ( row[ "Open" ] - row[ "PrevClose" ] ) / row[ "PrevClose" ], axis=1 )

        # Add indicators
        self.calc_ma( [ 10, 20, 50, 100, 200 ] )
        self.df[ "Range" ] = self.df[ "High" ] / self.df[ "Low" ]
        self.df[ "ADR" ] = self.df[ "Range" ].rolling( 20 ).mean()

    def ticker( self ):
        return self._ticker

    def calc_ma( self, mas ):
        for i in mas:
            ma_name = "MA%d" % i
            self.df[ ma_name ] = self.df[ "Close" ].rolling( i ).mean()

    def calc_trades( self ):
        self.trades = self.df.query( self.buy_strategy ).copy()
        self.trades[ "BuyPrice" ] = self.trades.apply( lambda row : row[ "Open" ], axis=1 )

        def sell_price( buy_price, drawdown, closing_price ):
            if drawdown > STOP_LOSS/100:
                return ( 1 - STOP_LOSS/100 ) * buy_price
            else:
                return closing_price

        self.trades[ "Drawdown" ] = ( self.trades[ "BuyPrice" ] - self.trades[ "Low" ] ) / self.trades[ "BuyPrice" ]
        self.trades[ "SellPrice" ] = self.trades.apply( lambda row : sell_price( row[ "BuyPrice" ], row[ "Drawdown" ], row[ "Close" ] ), axis=1 )
        self.trades[ "Profit" ] = self.trades.apply( lambda row : ( row[ "SellPrice" ] - row[ "BuyPrice" ] ) / row[ "BuyPrice" ], axis=1 )

    def trade_range( self, start_date=None, end_date=None ):
        if not start_date:
            start_date = self.trades.iloc[ 0, "Date" ]
        if not end_date:
            end_date = self.trades.iloc[ -1, "Date" ]
        return self.trades.query( 'Date > @start_date and Date < @end_date', inplace=False )

    def trade( self, date ):
        date = pd.to_datetime( date )
        return self.trades.query( 'Date == @date' )

    def cleanup( self ):
        self.trades.drop( [ "PrevDayHigh", "PrevClose", "MA10", "MA20", "MA50", "MA100", "MA200", "Range", "ADR" ], axis=1, inplace=True )
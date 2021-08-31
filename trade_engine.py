from typing import NamedTuple, Type
from builtin_commands import Commands
from pathlib import Path
from collections import OrderedDict, namedtuple
import pandas as pd
import numpy as np
import time
import sys, code, traceback
from enum import Enum

from ticker_data import DataLoader, DataLoaderUtils

from utils_common import timer, timerData
from builtin_commands import Commands

DATA_DIR = "./data"

########################################################################
# TradeEngine code starts here
########################################################################
class TradeType( Enum ):
    BUY = 1,
    SELL = 2,
    SHORT = 3,
    COVER = 4

class TradeEngine( object ):
    def __init__( self, ticker, strategyInfo, params={} ):
        self._ticker = ticker
        self.strategyInfo = strategyInfo
        self.buyStrategy = strategyInfo[ "buy" ]
        self.sellStrategy = strategyInfo[ "sell" ]
        self.params = params
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.tradeInfo = {}
        self.trades = pd.DataFrame( columns=[ 'Date', 'Ticker', 'Type', 'Strategy', 'Price', 'Quantity' ] )
        self.positions = pd.DataFrame( columns=[ 'Date', 'Ticker', 'Type', 'Strategy', 'Price', 'Quantity' ] )

        self.loader = DataLoader( DATA_DIR )
        self.data = self.loader.data( ticker, period="daily" )
        self.intradayData = self.loader.data( ticker, period="intraday" )

        self.setup()

    def initTradeInfo( self ):
        self.tradeInfo = {}
        self.tradeInfo[ "totalQty" ] = 0
        self.tradeInfo[ "triggered" ] = []
        self.tradeInfo[ "liveStopLoss" ] = []

    def setup( self ):
        self.data.rename( columns={ 'close': 'Close',
                                    'open': 'Open',
                                    'low': 'Low', 
                                    'high': 'High' }, inplace=True )
        self.data.index.rename( 'Date', inplace=True )
        self.data.sort_index( inplace=True )

        # Init meta data
        self.initTradeInfo()

        # Compile the indicators
        commands = Commands()
        code = self.strategyInfo[ "code" ]
        commands.compile( code, self.data )

    def ticker( self ):
        return self._ticker
    
    @timer
    def executeCondition( self, condition, globalVars, localVars ):
        globals = globalVars
        locals = localVars
        condition = f"returnVal={condition}"
        try:
            code = compile( condition + "\n", "<stdin>", "single" )
            saved_stdin = sys.stdin
            saved_stdout = sys.stdout
            sys.stdin = self.stdin
            sys.stdout = self.stdout

            try:
                exec( code, globals, locals )
            finally:
                sys.stdin = saved_stdin
                sys.stdout = saved_stdout
        except:
            exec_info = sys.exc_info()[ :2 ]
            print( traceback.format_exception_only( *exec_info )[ -1 ].strip() )
        return locals[ "returnVal" ]

    @timer
    def findTrade( self, type, data, condition, tradeQty, priceCondition, stopLossCondition, stopQty, env ):
        """Takes a table of daily or intraday price data and finds the first row entry that meets the trading condition

        Inputs - 
            Type : Trade type as in sell, buy, short, cover
            Data : intraday or daily data
        """
        def _executeAndLogTrade( condition1, condition2, qty ):
            nonlocal found
            ret = self.executeCondition( condition1, globals, locals )
            if ret:
                found = True
                price = self.executeCondition( condition2, globals, locals )
                self.tradeInfo[ "triggered" ] += [ ( price, qty, tradeDate ) ]
                #print( ( price, qty, tradeDate ), condition1, condition2, sep=",  " )              #DEBUG
            return ret

        globals = env
        tradeDate = None
        found = False
        
        for d in data.itertuples():
            tradeDate = d.Index
            locals = d._asdict()
            
            # Some special processing for sell or cover trades. 
            # Drawdown and stop loss need to be calculcated only once we are in a trade.
            if type == TradeType.SELL or type == TradeType.COVER:                
                drawdown = self.executeCondition( "( Price - Low ) / Price", globals, locals )
                locals.update( { 'Drawdown' : drawdown } )                

                # Test the stoploss if one exists
                keep = []
                for ( price, qty, date ) in self.tradeInfo[ "liveStopLoss" ]:
                    # Do not process stop losses that were set today
                    if date == tradeDate:
                        continue
                    isTrade = _executeAndLogTrade( f"Low < {price}", f"{price} * ( 1 - DISPERSION )", qty )
                    if not isTrade:
                        keep += [ ( price, qty, date ) ]
                self.tradeInfo[ "liveStopLoss" ] = keep

            isTrade = _executeAndLogTrade( condition, priceCondition, tradeQty )
            if isTrade and stopLossCondition:
                stopLossPrice = self.executeCondition( f"{stopLossCondition}", globals, locals )
                self.tradeInfo[ "liveStopLoss" ] += [ ( stopLossPrice, stopQty, tradeDate ) ]

            if found:
                break
        return found

    @timer
    def getBuys( self, strategy ):
        if self.data.empty:
            return
        
        print( "{}: calculating buy trades.".format( self.ticker() ) )

        type = TradeType.BUY
        tradeId = 1
        env = self.params

        for d in self.data.itertuples( index=True ):
            date = d.Index
            for name in strategy:
                ( timeframe, qty, condition, priceCondition, stopLoss ) = strategy[ name ]

                data = self.processTimeframe( timeframe, date, startDate=date )

                # Now we walk through data from startTime till endTime and find out if we meet the trade condition
                stopLossQty = qty
                _ = self.findTrade( TradeType.BUY, data, condition, qty, priceCondition, stopLoss, stopLossQty, env )

                for ( price, qty, _ ) in self.tradeInfo[ "triggered" ]:
                    self.positions.loc[ tradeId ] = { 'Date': date, 'Type': type, 'Strategy' : name, 'Price': price, 'Quantity': qty, 'Ticker': self.ticker() }
                    tradeId += 1
                self.tradeInfo[ "triggered" ] = []
     
        self.positions[ 'Date' ] = pd.to_datetime( self.positions[ 'Date' ] )

    def processTimeframe( self, timeframe, date, startDate=None, endDate=None ):
        ( d1, t1, d2, t2 ) = timeframe

        # The following stetement block should be converted to structural pattern matching
        # once we upgrade to Python 3.10
        if ( d1, t1, d2 ) == ( "Day", "1", None ):
            if startDate is not None and date == startDate:
                data = self.data.loc[ date : date ]
            else:
                data = pd.DataFrame()

        elif ( d1, d2 ) == ( "Day", "Day" ):
            pass
        
        elif ( d1, d2 ) == ( "Day", "All" ):
            if endDate is None:
                data = self.data.loc[ date : ]  
            else:
                data = self.data.loc[ date : endDate ]
                data = data.iloc[ : -1 ]
        
        elif ( d1, d2 ) == ( "1Min", "1Min" ):
            data = self.intradayData.loc[ date ]
        
        else:
            print( "Syntax error in timeframe" )
            data = None
        return data

    @timer
    def getSales( self, strategy ):

        def _runStrategies( newQty, prevQty ):
            nonlocal tradeId
            found = False
            cursor = 0
            totalQty = 0
            remaining = newQty + prevQty

            for name in strategy:
                ( timeframe, strategyQty, condition, priceCondition, stopLossCondition ) = strategy[ name ]
            
                for ( tradeQty, buyDate ) in ( ( newQty, tradeDate ), ( prevQty, None ) ):  

                    # If the previous strategies have already sold all of the position, there is no point in continuing
                    if not remaining or ( buyDate == tradeDate and remaining <= prevQty ):
                        continue

                    data = self.processTimeframe( timeframe, tradeDate, buyDate, endDate )
                    
                    if data.empty:
                        continue
                
                    # The qty in strategy is a percentage. Convert it to specific number first i.e. 
                    # split the tradeQty according to the strategy specification
                    qty = tradeQty * strategyQty
                    stopLossQty = tradeQty * ( 1 - strategyQty )

                    found = self.findTrade( TradeType.SELL, data, condition, qty, priceCondition, stopLossCondition, stopLossQty, env )

                    print( tradeDate, name, self.tradeInfo, sep="  " )             #DEBUG
                    for ( price, qty, date ) in self.tradeInfo[ "triggered" ]:
                        qty = remaining if remaining < qty else qty
                        self.trades.loc[ tradeId ] = { 'Date': date, 'Type': type, 'Strategy' : name, 'Price': price, 'Quantity': qty, 'Ticker': self.ticker() }
                        tradeId += 1
                        remaining -= qty

                        if not remaining:
                            break
                    self.tradeInfo[ "triggered" ] = []

            print( "-------------------------" )                  #DEBUG
            return remaining


        if self.positions.empty:
            return
        print( "{}: calculating sell trades.".format( self.ticker() ) )

        type = TradeType.SELL
        tradeId = 1
        env = self.params
        endDate = None
        for p in self.positions.itertuples( index=True ):
            env.update( { 'Price' : p.Price } )
            maxSize = env[ "MAX_POSITION_SIZE" ]

            prevQty = self.tradeInfo[ "totalQty" ]
            tradeQty = p.Quantity
            tradeDate = p.Date

            # Figure out endDate, which is the trading day before the next buy trade unless there are no more buys left
            endDate = self.positions.at[ p.Index + 1, 'Date' ] if ( p.Index + 1 ) in self.positions.index else tradeDate

            # If making this trade exceeds the max size limits, we need to adjust the tradeQty down
            if prevQty + tradeQty > maxSize:
                tradeQty = maxSize - prevQty

            if tradeQty > 0:
                self.trades.loc[ tradeId ] = { 'Date': tradeDate, 'Type': p.Type, 'Strategy' : p.Strategy, 'Price': p.Price, 'Quantity': tradeQty, 'Ticker': self.ticker() }
                tradeId += 1

            totalQty = _runStrategies( tradeQty, prevQty )
            self.tradeInfo[ "totalQty" ] = totalQty

            if not totalQty:
                self.tradeInfo[ "liveStopLoss" ] = []

        self.trades[ 'Date' ] = pd.to_datetime( self.trades[ 'Date' ] )


    def consolidateTrades( self, trades ):
        consolidatedTrades = pd.DataFrame( columns=[ 'Date', 'SellDate', 'Type', 'BuyPrice', 'SellPrice', 'Quantity', 'OpenQty' ] )

        tradeId = 1
        stack = []
        BuyOrder = namedtuple( "BuyOrder", 'Date SellDate Type BuyPrice SellPrice Quantity OpenQty' )
        
        def _processSellTrade( qty ):
            nonlocal stack
            nonlocal consolidatedTrades
            nonlocal tradeId
            if len( stack ):
                buyOrder = stack.pop()
                if buyOrder.OpenQty == qty:
                    consolidatedTrades.loc[ tradeId ] = pd.Series( buyOrder._replace( SellPrice=t.Price, SellDate=t.Date ), index=consolidatedTrades.columns )
                    tradeId += 1

                elif buyOrder.OpenQty > qty:
                    consolidatedTrades.loc[ tradeId ] = pd.Series( buyOrder._replace( SellPrice=t.Price, Quantity=qty, SellDate=t.Date ), index=consolidatedTrades.columns )
                    tradeId += 1
                    buyOrder = buyOrder._replace( OpenQty=( buyOrder.OpenQty - qty ) )
                    stack.append( buyOrder )

                elif buyOrder.OpenQty < qty:
                    consolidatedTrades.loc[ tradeId ] = pd.Series( buyOrder._replace( SellPrice=t.Price, Quantity=buyOrder.OpenQty, SellDate=t.Date ), index=consolidatedTrades.columns )
                    tradeId += 1
                    qty = qty - buyOrder.OpenQty
                    _processSellTrade( qty )

        for t in trades.itertuples( index=True ):
            if t.Type == TradeType.BUY:
                buyOrder = BuyOrder( Date=t.Date, SellDate=t.Date, Type="LONG", BuyPrice=t.Price, SellPrice=0, Quantity=t.Quantity, OpenQty=t.Quantity )
                stack.append( buyOrder )

            elif t.Type == TradeType.SELL:
                qty = t.Quantity
                _processSellTrade( qty )

        consolidatedTrades.drop( columns=[ 'OpenQty' ], inplace=True )
        consolidatedTrades[ 'Ticker' ] = self.ticker()
        consolidatedTrades[ 'Profit' ] = ( consolidatedTrades[ 'SellPrice' ] - consolidatedTrades[ 'BuyPrice' ] ) / consolidatedTrades[ 'BuyPrice' ]
        return consolidatedTrades


    def tradeRange( self, startDate=None, endDate=None, consolidate=True ):
        if not startDate:
            startDate = self.trades.index[ 0 ]
        if not endDate:
            endDate = self.trades.index[ -1 ]
        trades = self.trades.query( 'Date > @startDate and Date < @endDate', inplace=False )

        #print( trades )
        if consolidate:
            return self.consolidateTrades( trades )
        else:
            return trades

    def trade( self, date, consolidate=True ):
        return self.tradeRange( date, date )


    def run( self, buy=True, sell=True ):
        if buy:
            self.getBuys( self.buyStrategy )
        if sell:
            self.getSales( self.sellStrategy )
        self.cleanup()

    def cleanup( self ):
        pass


from pathlib import Path
from dash_html_components.Hr import Hr
import pandas as pd
import numpy as np
import threading
import time
import json
import yaml

import plotly
import plotly.express as px
import plotly.graph_objs as go
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from ticker_data import Data_loader

from simulator_shell import Shell, ShellConfig

START_DATE = "1/1/2016"
END_DATE = "1/1/2022"
INIT_CAP = 100000
COMPOUND = 0

DATA_DIR = "./data"
CONFIG_FILE = "./.plumsim.config.json"
STRATEGY_FILE = "./Strategy1.simulate"

########################################################################
# Various classes to hold global settings etc. which can be accessed and 
# manupulated inside disconnected classes
########################################################################
class Params( object ):
    pass

params = Params()

class PlumsimConfig( object ):
    threads = []
    web_thread_active = True

plumsimConfig = PlumsimConfig()

########################################################################
# 
########################################################################
class Kernel( object ):
    def __init__( self ) -> None:
        self.code = None
        self.input = None

    def setInput( self, input ):
        self.input = input

    def run( self ):
        return self.input.query( self.code ).copy()

class AndLogicKernel( object ):
    def __init__( self ) -> None:
        pass

class Trader( object ):
    def __init__( self, ticker, buy_strategy, sell_strategy ):
        self._ticker = ticker
        self.buy_strategy = buy_strategy
        self.sell_strategy = sell_strategy
        self.loader = Data_loader( DATA_DIR )
        self.df = self.loader.data( ticker, START_DATE, END_DATE )
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
        self.calc_trend( [ 10, 20, 50, 100, 200 ] )
        self.df[ "Range" ] = self.df[ "High" ] / self.df[ "Low" ]
        self.df[ "ADR" ] = ( self.df[ "Range" ].rolling( 20 ).mean() - 1 ) * 100

    def ticker( self ):
        return self._ticker

    def calc_ma( self, mas ):
        for i in mas:
            ma_name = "MA%d" % i
            self.df[ ma_name ] = self.df[ "Close" ].ewm( span=i ).mean()

    def calc_trend( self, args ):
        for t in args:
            name = "Trend%d" % t
            ma = "MA%d" % t
            self.df[ name ] = self.df[ ma ].ewm( span=5 ).mean()

    def buyDailyCondition( self ):
        self.trades = self.df.query( self.buy_strategy_intraday ).copy()
        #self.trades[ 'BuyPrice' ] = 

    def calc_trades( self ):
        self.trades = self.df.query( self.buy_strategy ).copy()
        self.trades[ "BuyPrice" ] = self.trades.apply( lambda row : row[ "Open" ], axis=1 )
        self.trades.reset_index( drop=True )

        def sell_price( buy_price, drawdown, closing_price, adr ):
            if drawdown > params.STOP_LOSS/100:
                return ( 1 - params.STOP_LOSS/100 ) * buy_price
            else:
                return closing_price

        self.trades[ "Drawdown" ] = ( self.trades[ "BuyPrice" ] - self.trades[ "Low" ] ) / self.trades[ "BuyPrice" ]
        self.trades[ "SellPrice" ] = self.trades.apply( lambda row : sell_price( row[ "BuyPrice" ], row[ "Drawdown" ], row[ "Close" ], row[ "ADR" ] ), axis=1 )
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

class Stats( object ):
    numWin = 0
    numLoss = 0

class Simulator( object ):
    def __init__( self ) -> None:
        self.trades_master = pd.DataFrame()
        self.buyStrategy = None
        self.sellStrategy = None
        self.tickers = []
        self.stats = Stats()

    def setTickers( self, args ):
        def processArgs( args ):
            try:
                _args = eval( args )
                is_list = isinstance( _args, list )
            except:
                is_list = False

            if is_list:
                tickers = _args
            else:
                args = args.split()
                tickers = []
                for arg in args:
                    arg = arg.strip( " ,\"[]" )
                    if arg.endswith( ".csv" ):
                        df = pd.read_csv( arg, index_col=0 ).index.values
                    else:
                        ticker = [ arg.upper() ]
                        tickers += ticker
            return tickers

        self.tickers = processArgs( args )

    def clearTrades( self, args ):
        self.trades_master = pd.DataFrame()
        
    def print_summary( self, trades ):
        print( trades )
        print( "winning trades: %s" % self.stats.numWin )
        print( "losing trades: %s" % self.stats.numLoss )
        print( "winning %: {}".format( round( self.stats.numWin / ( self.stats.numWin + self.stats.numLoss ) * 100 ) ) )
        print( trades[ "Profits" ].describe() )
        print( "Total profit: %d" % trades[ "Profits" ].sum() )
        #print( "Max trades per day: %s" % trades.groupby( [ "Date" ] ).count().idxmax() )

    def read_strategy_file( self, txt_f ):
        with open( txt_f, 'r' ) as f:
            for l in f:
                print( l )
        self.strategy = None

    def simulate( self, args ):
        start_date = pd.to_datetime( START_DATE )
        end_date = pd.to_datetime( END_DATE )

        for t in self.tickers:
            trader = Trader( t, self.buyStrategy, None )
            trades = trader.trade_range( start_date, end_date )
            if trades is not None and not trades.empty:
                self.trades_master = pd.concat( [ self.trades_master, trades ] )

        self.trades_master.sort_values( by=[ "Date" ], inplace=True )

        self.calc_pnl()
        self.stats.numWin = self.trades_master[ self.trades_master[ "Profits" ] > 0 ][ "Profits" ].count()
        self.stats.numLoss = self.trades_master[ self.trades_master[ "Profits" ] < 0 ][ "Profits" ].count()

        self.print_summary( simulator.trades_master )

    def calc_pnl( self, args=None ):
        self.trades_master[ "Invested" ] = INIT_CAP
        if COMPOUND:
            amount = INIT_CAP
            for row in self.trades_master.itertuples():
                self.trades_master.loc[ row.Index, "Invested" ] = amount
                amount = amount * ( 1 + row.Profit )

        self.trades_master[ "Profits" ] =  self.trades_master[ "Invested" ] * self.trades_master[ "Profit" ]
        self.trades_master[ "AggregateProfits" ] = self.trades_master[ "Profits" ].cumsum()

    def exit( self ):
        print( "Exiting simulator" )

    def data_update_cache( self, args ):
        data_dir = "./data"
        loader = Data_loader( data_dir )
        for p in Path( data_dir ).iterdir():
            if p.is_dir():
                print( p.name )
                ticker = str( p.name )
                loader.data( ticker )

    def download_data( self, args ):
        wl_path = Path( args.strip() )
        if not wl_path.is_file():
            print( "Did not find file." )
            return

        wl = pd.read_csv( wl_path )
        loader = Data_loader( DATA_DIR )
        for t in wl[ "Symbols" ]:
            print( t )
            ticker = t.strip()
            try:
                loader.data( ticker )
            except:
                pass

    def saveConfig( self, args ):
        _config = {}
        _config[ 'tickers' ] = self.tickers
        
        jsonObject = json.dumps( _config )
        with open( CONFIG_FILE, 'w' ) as f:
            f.write( jsonObject )

    def printParams( self ):
        print( params.__dict__ )

    def loadStrategy( self, args ):

        def _parseLogic( key, value ):
            if isinstance( value, dict ):
                input = []

                for k, v in value.items():
                    input += [ _parseLogic( k, v ) ]

                if key == "AND":
                    gate = " and "
                elif key == "OR":
                    gate = " or "
                else:
                    gate = ""

                return "( {} )".format( gate.join( input ) )
            else:
                return "( {} )".format( value )

        strategyName = args.strip()

        with open( STRATEGY_FILE, 'r') as f:
            dictionary = yaml.load( f, Loader=yaml.FullLoader )

        if strategyName in dictionary:
            strategy = dictionary[ strategyName ]
        else:
            print( "Strategy not found." )
            return

        print( "strategy : {}".format( strategyName ) )
        print( "=====================================" )
        for key, value in strategy.items():
            if key.upper() == "PARAMS":
                for k, v in value.items():
                    setattr( params, k, v )
                    print( "{} : {}".format( k, v ) )

            if key.upper() == "BUY":
                self.buyStrategy = _parseLogic( key, value )
                print( "buy : {}".format( self.buyStrategy ) )

            if key.upper() == "SELL":
                self.sellStrategy = _parseLogic( key, value )
                print( "sell : {}".format( self.sellStrategy ) )


    def printStrategy( self, args ):
        with open( STRATEGY_FILE, 'r') as f:
            dictionary = yaml.load( f, Loader=yaml.FullLoader )
            for key, value in dictionary.items():
                print ( "{} : {}\n".format( key, str( value ) ) )



if __name__ == "__main__":
    pd.set_option( "display.max_rows", None )

    simulator = Simulator()

    app = dash.Dash( external_stylesheets=[dbc.themes.BOOTSTRAP] )
    app.layout = dbc.Container(
        [
            html.H1( "Strategy performance for %s" % simulator.tickers ),
            dbc.Tabs( 
                [
                    dbc.Tab( label="performance", tab_id="perf_graph" ),
                    dbc.Tab( label="chart", tab_id="data_chart" )
                ],
                id = "tabs",
                active_tab = "perf_graph",
            ),
            html.Div( id="tab-content", className="p-4" )        
        ]
    )

    @app.callback(
        Output( "tab-content", "children" ),
        Input( "tabs", "active_tab" ),
    )
    def render_tab_content( active_tab ):
        if active_tab == "perf_graph":
            fig = px.line( simulator.trades_master, x="Date", y="AggregateProfits" )
            return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )
        elif active_tab == "data_chart":
            fig = go.Figure( data=go.Ohlc( x=trader.df[ "Date" ],
                                            open=trader.df[ "Open" ],
                                            high=trader.df[ "High" ],
                                            low=trader.df[ "Low" ],
                                            close=trader.df[ "Close" ] ) 
                            )
            #fig.update( layout_xaxis_rangeslider_visible=False )
            return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )

    plumsimConfig.web_thread_active = True
    def start_web_server():
        app.run_server( debug=True, use_reloader=False, dev_tools_hot_reload=False )
        while plumsimConfig.web_thread_active:
            time.sleep( 5 )

    web_thread = threading.Thread( target=start_web_server )
    web_thread.start()

    config = ShellConfig()
    config.app = simulator
    config.config = plumsimConfig
    config.config.threads = [ web_thread ]
    shell = Shell( config )
    shell.prompt = '%s>> ' % ( "SIMULATOR" )
    shell._cmdloop( "" )
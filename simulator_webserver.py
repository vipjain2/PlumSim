from dash_html_components.Hr import Hr
import plotly.express as px
import plotly.graph_objs as go
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import threading

class WebServer( object ):
    def __init__( self, simulator ) -> None:
        self.simulator = simulator
        self.web_thread_active = False
        app = dash.Dash( external_stylesheets=[dbc.themes.BOOTSTRAP] )
        self.app = app

        app.layout = dbc.Container(
            [
                html.H1( "Strategy performance for %s" % simulator.tickers ),
                dbc.Tabs( 
                    [
                        dbc.Tab( label="performance", tab_id="perf_graph" ),
                        dbc.Tab( label="histogram", tab_id="histogram_chart" ),
                        dbc.Tab( label="custom", tab_id="custom_chart" )
                    ],
                    id = "tabs",
                    active_tab = "perf_graph",
                ),
                html.Div( id="tab-content", className="p-4" )        
            ]
        )

        self.connectCallbacks( app )

    def connectCallbacks( self, app ):
        @app.callback(
            Output( "tab-content", "children" ),
            Input( "tabs", "active_tab" ),
        )
        def render_tab_content( active_tab ):
            if active_tab == "perf_graph":
                fig = px.line( self.simulator.trades_master, x="Date", y="AggregateProfits" )
                return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )
            elif active_tab == "histogram_chart":
                fig = px.histogram( self.simulator.trades_master, x="Profits" )
                return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=fig )
            elif active_tab == "custom_chart":
                return dcc.Graph( style={ "width": "120vh", "height": "70vh" }, figure=self.simulator.custom_fig )

    def startServer( self ):
        self.web_thread = threading.Thread( target=self.app.run_server, kwargs={ "debug" : True, "use_reloader" : False, "dev_tools_hot_reload" : False } )
        self.web_thread_active = True
        self.web_thread.start()

    def stopServer( self ):
        pass
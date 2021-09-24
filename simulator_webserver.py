from dash_html_components.Hr import Hr
import plotly.express as px
import plotly.graph_objs as go
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import threading

class WebApp( object ):
    def __init__( self, simulator ) -> None:
        self.simulator = simulator
        self.web_thread_active = False
        app = dash.Dash( external_stylesheets=[dbc.themes.BOOTSTRAP] )
        self.app = app

        sidebar = html.Div(
            [
                html.H4( "Explorer" ),
                html.Hr(),
                html.P(
                    "", className="lead"
                ),
                dbc.Nav(
                    [
                        dbc.NavLink("Home", href="/", active="exact"),
                    ],
                    vertical=True,
                ),
            ],
            style={
            "position": "fixed",
            "top": 0,
            "left": 0,
            "bottom": 0,
            "width": "16rem",
            "padding": "2rem 1rem",
            "background-color": "#f8f9fa",
            }
        )

        app.layout = dbc.Container(
            [
                html.H1( "Strategy Performance %s" % simulator.tickers ),
                sidebar,
                dbc.Tabs( 
                    [
                        dbc.Tab( label="performance", tab_id="perf_graph" ),
                        dbc.Tab( label="histogram", tab_id="histogram_chart" ),
                        dbc.Tab( label="custom", tab_id="custom_chart" )
                    ],
                    id = "tabs",
                    active_tab = "perf_graph",
                ),
                html.Div( id="tab-content" ),
                dbc.Button( "Simulate", color="primary", id="simulate-button" ),
                html.Div( id="simulate-button-pressed", children="Press simulate button to start" )
            ]
        )

        self.connectCallbacks( app )

    def connectCallbacks( self, app ):
        @app.callback(
            Output( "tab-content", "children" ),
            [ Input( "tabs", "active_tab" ),
              Input( "simulate-button-pressed", "children" )
            ]
        )
        def render_tab_content( active_tab, simulate_complete ):
            
            if active_tab == "perf_graph":
                fig = px.line( self.simulator.trades_master, x="Date", y="AggregateProfits" )
                return dcc.Graph( style={ "width": "50vw", "height": "50vh" }, 
                                  config={ "displaylogo" : False },
                                  figure=fig )
            elif active_tab == "histogram_chart":
                fig = px.histogram( self.simulator.trades_master, x="Profits" )
                return dcc.Graph( style={ "width": "50vw", "height": "50vh" }, 
                                  config={ "displaylogo" : False },
                                  figure=fig )
            elif active_tab == "custom_chart":
                return dcc.Graph( style={ "width": "50vw", "height": "50vh" }, 
                                  config={ "displaylogo" : False },
                                  figure=self.simulator.custom_fig )

        @app.callback( 
            Output( "simulate-button-pressed", "children" ),
            Input( "simulate-button", "n_clicks" )
        )
        def simulateButton( n_clicks ):
            if n_clicks:
                self.simulator.clearTrades( None )
                self.simulator.simulate( None )
            return None

    def startServer( self ):
        self.web_thread = threading.Thread( target=self.app.run_server, kwargs={ "debug" : True, "host": "0.0.0.0", "use_reloader" : False, "dev_tools_hot_reload" : False } )
        self.web_thread_active = True
        self.web_thread.start()

    def stopServer( self ):
        pass
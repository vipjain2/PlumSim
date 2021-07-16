from shell_common import ShellCommon, ShellConfig

class Shell( ShellCommon ):
    def __init__(self, config):
        super().__init__( config )
        
    def do_quit(self, args):
        return super().do_quit(args)

    def do_update_cache( self, args ):
        self.config.app.data_update_cache( args )

    def do_download_data( self, args ):
        self.config.app.download_data( args )
    
    def do_load_strategy( self, args ):
        self.config.app.read_strategy_file( args )

    def do_set_tickers( self, args ):
        self.config.app.setTickers( args )
    
    do_set_ticker = do_set_tickers

    def do_clear_trades( self, args ):
        self.config.app.clearTrades( args )

    def do_simulate( self, args ):
        self.config.app.simulate( args )

    def do_show_pnl( self, args ):
        self.config.app.calc_pnl( args )
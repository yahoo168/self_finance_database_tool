def _download_data_from_Yahoo_Fin(ticker):
    return get_data(ticker)

def _download_sp500_ticker_list_from_yahoo():
    return tickers_sp500()

def _download_nasdaq_ticker_list_from_yahoo():
    return tickers_nasdaq()
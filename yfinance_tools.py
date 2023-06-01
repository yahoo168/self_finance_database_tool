from .utils import *
import pandas as pd

# yahoo_finance中，非美國的國家代碼間隔號使用‘.’，股類分隔號使用'-'，如：rds-b，但資料夾內部存檔時一律用‘_’
# if country == "US":
#     ticker_for_search = ticker.replace('.', '-')

# elif country =="TW":
#     ticker_for_search = ticker.replace('_', '.') 

# API Documentation1: https://pypi.org/project/yfinance/
# API Documentation2: https://aroussi.com/post/python-yahoo-finance
def save_stock_priceVolume_from_yfinance(save_folderPath, cache_folderPath, start_date, end_date, ticker_list, country, adjusted=True):
    threads = []  # 儲存線程以待關閉
    try:
        for index, ticker in enumerate(ticker_list, 1):
            t = Thread(target=_save_stock_priceVolume_from_yfinance_singleTicker, 
                        args=(cache_folderPath, ticker, start_date, country, adjusted))
            t.start()  # 開啟線程
            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            #time.sleep(random.random())
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(ticker_list), 2)            
            print("{ticker:<6} 股價資料下載中，完成度{percentage}%".format(ticker=ticker, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

    # 將cache資料夾中的塊狀資料(fileName:ticker, row:date, column:items)轉化為時序型資料
    raw_ticker_list = os.listdir(cache_folderPath)
    # 去除後綴檔名
    ticker_list = [ticker.split(".")[0] for ticker in raw_ticker_list]
    # 去除異常空值
    ticker_list = [ticker for ticker in ticker_list if len(ticker)!=0]
    # 指定需要轉化的資料項目（yfinance只有OHLCV五種資料）
    item_list = ["open", "high", "low", "close", "volume", "dividends", "stock_splits"]
    for item in item_list:
        item_folderPath = os.path.join(save_folderPath, item)
        make_folder(item_folderPath)
        df_list = list()
        for ticker in ticker_list:
            fileName = os.path.join(cache_folderPath, ticker+".csv")
            df = pd.read_csv(fileName, index_col=0)
            # 篩選出塊狀資料中的指定資料項目
            data_series = df[item].rename(ticker)
            df_list.append(data_series)

        # Trasform後，row:tickers, colunm:Date
        df = pd.concat(df_list, axis=1).sort_index()
        mask = (df.index >= start_date) & (df.index <= end_date)
        df = df[mask].T
        
        for num in range(len(df.columns)):
            # 依序將各column切出
            data_series = df.iloc[:, num]
            # 存檔至以指定資料項目為名的資料夾之下，檔名為該日日期
            fileName = os.path.join(item_folderPath, data_series.name+".csv")
            data_series.to_csv(fileName)

# 因資料筆數多（約15000筆），中途可能因網路問題導致報錯，為避免須重新下載而設計cache機制
def _save_stock_priceVolume_from_yfinance_singleTicker(folderPath, ticker, start_date, country, adjusted):
    filePath = os.path.join(folderPath, ticker+".csv")
    # 若所需檔案不在cache中，則透過yfinance載入資料；待改：若在cache中但時間過時，也應該刪掉
    if not os.path.exists(filePath):
        # yfinance預設會額外下載dividend與stock-split資料，可將actions設為False以改變設定
        # yfinance預設會自動調整OHLC，
        ticker_for_search = ticker.replace("_", "-")
        if adjusted:
            data_df = yf.Ticker(ticker_for_search).history(period="max", start=start_date, auto_adjust=True, actions=True)
        else:
            data_df = yf.Ticker(ticker_for_search).history(period="max", start=start_date, auto_adjust=False, actions=True)
        
        # yfinance預設為首字母大寫，此處調整為全小寫
        data_df.columns = [x.lower().replace(' ', '_') for x in data_df.columns]
        # 將datetime index轉為EOD類型的字串index
        data_df.index = data_df.index.to_series().apply(lambda x: datetime2str(x))
        if len(data_df) > 0:
            data_df.to_csv(filePath)
            logging.info("[PV][yfinance][{}]:資料已下載至cache".format(ticker))
    # 若所需檔案已在cache中，則透過cache載入資料
    else:
        #data_df = pd.read_csv(filePath, index_col=0)
        logging.info("[PV][yfinance][{}]:資料已存在於cache".format(ticker))

def _download_data_from_yfinance(ticker):
    ticker = yf.Ticker(ticker)
    return ticker.history(period="max", actions=False)
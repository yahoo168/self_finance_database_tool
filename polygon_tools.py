import requests
import os, sys, logging, time
from .utils import *
from threading import Thread, Lock
import pandas as pd

# 自polygon下載EOD價量相關資料，若無指定起始/結束日期，則自動以上次更新日期的後一日開始抓取資料，更新至今日
def save_stock_priceVolume_from_Polygon(folderPath, API_key, start_date=None, end_date=None, adjust=False):
    def _save_stock_priceVolume_from_Polygon_singleDate(folderPath, API_key, item_list, date, adjust):
        if adjust==True:
            adjust_flag = "true"
        elif adjust==False:
            adjust_flag = "false"

        url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjust_flag}&apiKey={API_key}".format(date=date, adjust_flag=adjust_flag, API_key=API_key)
        data_json = requests.get(url).json()
        if (data_json["status"] in ["NOT_AUTHORIZED", "DELAYED"]) or (data_json["queryCount"] == 0):
            return False

        #logging.info("資料源:Polygon {}: 下載狀態:{}".format(date, data_json["status"]))
        results = data_json["results"]
        df = pd.DataFrame(results)
        df = df.rename(columns={"T":"ticker", "v":"volume", "vw":"avg_price", "o":"open", "c":"close", "l":"low", "h":"high", "n":"transaction_num", "t":"timestamp"})
        df["ticker"] = df["ticker"].apply(lambda x : x.replace(".", "_"))
        df = df.set_index("ticker")
        # 依序將各column切出，轉為csv檔，檔名為該日日期
        for item in item_list:
            data_series = df.loc[:, item]
            data_series = data_series.rename(date)
            # 存檔至以指定資料項目為名的資料夾之下
            if adjust==True:
                item = "adj_"+item
            item_folder_path = os.path.join(folderPath, item)
            fileName = os.path.join(item_folder_path, date+".csv")
            data_series.to_csv(fileName)

    item_list = ["open", "high", "low", "close", "volume", "avg_price", "transaction_num"]
    for item in item_list:
        # 若項目資料夾不存在則自動建立資料夾
        make_folder(os.path.join(folderPath, item))

    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()
    # 逐日下載資料，儲存與字典當中
    for index, date in enumerate(date_range_list, 1):
        t = Thread(target=_save_stock_priceVolume_from_Polygon_singleDate, 
            args=(folderPath, API_key, item_list, date, adjust))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(date_range_list), 2)            
        logging.info("[{date}][OHLCV] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))

    for t in threads:
        t.join()

def save_stock_split_from_Polygon(folderPath, API_key, start_date=None, end_date=None):
    def _save_stock_split_from_Polygon_singleDate(folderPath, API_key, date):
        url = "https://api.polygon.io/v3/reference/splits?execution_date={date}&apiKey={API_key}".format(date=date, API_key=API_key)
        data_json = requests.get(url).json()
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df["adjust_factor"] = df["split_to"] / df["split_from"]
            df = df.pivot(index="execution_date", columns="ticker", values="adjust_factor").T
            filePath = os.path.join(folderPath, date+".csv")
            df.to_csv(filePath)

    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date,freq='d'))))
    threads = []  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_range_list, 1):
            t = Thread(target=_save_stock_split_from_Polygon_singleDate, 
                        args=(folderPath, API_key, date))
            t.start()  # 開啟線程

            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(date_range_list), 2)            
            logging.info("[{date}][split] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

def save_stock_cash_dividend_from_Polygon(folderPath, API_key, start_date, end_date, data_type="ex_dividend_date"):
    def _save_stock_cash_dividend_from_Polygon_singleDate(folderPath, data_type, date, API_key):
        #只下載現金股利（CD）
        url = "https://api.polygon.io/v3/reference/dividends?{0}={1}&apiKey={2}&dividend_type=CD".format(data_type, date, API_key)
        data_json = requests.get(url).json()
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df = df.pivot(index="ticker", columns="ex_dividend_date", values="cash_amount")
            filePath = os.path.join(folderPath, date+".csv")
            df.to_csv(filePath)
    
    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_range_list, 1):
            t = Thread(target=_save_stock_cash_dividend_from_Polygon_singleDate, 
                        args=(folderPath, data_type, date, API_key))
            t.start()  # 開啟線程
            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(date_range_list), 2)            
            logging.info("[{date}][dividends] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

def save_stock_shares_outstanding_from_Polygon(folderPath, cache_folderPath, API_key, ticker_list, tradeDate_list=None, start_date=None, end_date=None):
    # 流通股數係透過polygon中的ticker detail資訊取得，索取方式為給定ticker與date，故包裝為雙重函數
    def _save_stock_shares_outstanding_from_Polygon_singleDate(folderPath, cache_folderPath, API_key, ticker_list, date):
        def _save_stock_shares_outstanding_from_Polygon_singleTicker(API_key, ticker, date):
            url = "https://api.polygon.io/v3/reference/tickers/{}?date={}&apiKey={}".format(ticker, date, API_key)
            data_json = requests.get(url).json()
            data_dict[ticker] = data_json["results"]["weighted_shares_outstanding"]
        
        data_dict = dict()
        cache_filePath = os.path.join(cache_folderPath, date+".csv")
        # 待改：原設計理念為程式若在執行單日資料索取時中斷於某公司，會將已索取的公司資料存檔於cache，
        # 下次重啟時自動讀取cache，並接續爬取，然而因child-thread報錯並不會跳回parent-thread，故此處的try&except實際上無用
        # 意即即使部份個股不存在shares outstanding，最終當日資料仍會下載成功，但會缺少那些個股的資料
        if os.path.exists(cache_filePath):
            cache_data = pd.read_csv(cache_filePath, index_col=0).squeeze()
            cache_data_dict = dict(cache_data)
            cache_ticker_list = cache_data_dict.keys()
            data_dict.update(cache_data_dict)
            ticker_list = list(set(ticker_list) - set(cache_ticker_list))

        threads = list()
        try:
            for index, ticker in enumerate(ticker_list, 1):
                t = Thread(target=_save_stock_shares_outstanding_from_Polygon_singleTicker, 
                    args=(API_key, ticker, date))
                t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
                time.sleep(0.1)
                threads.append(t)
                percentage = 100*round(index/len(ticker_list), 2)            
                logging.info("[{date}][{ticker}]流通股數下載中，完成度{percentage}%".format(date=date, ticker=ticker, percentage=percentage))

            for t in threads:
                t.join()

        except Exception as e:
            logging.warning(e)
            pd.Series(data_dict).to_csv(cache_filePath)
            logging.info("[{date}]下載中斷，原資料已存於cache，再次啟動將讀取cache後繼續下載".format(date))
        
        data_series = pd.Series(data_dict)
        filePath = os.path.join(folderPath, date+".csv")
        data_series.to_csv(filePath)

    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date,freq='d'))))
    
    #若有給定交易日序列，則非交易日不用下載，可加快下載速度    
    if tradeDate_list != None:
        date_range_list = set(date_range_list).intersection(set(tradeDate_list))
        date_range_list = sorted(date_range_list)
    
    for date in date_range_list:
        _save_stock_shares_outstanding_from_Polygon_singleDate(folderPath, cache_folderPath, API_key, ticker_list, date)

# 自Polygon下載財報資料 - 主程式（待改：S/D問題）
def save_stock_financialReport_from_Polygon(folderPath, ticker_list, start_date, end_date):
    ticker_list = ticker_list
    cache_folderPath = os.path.join(self.cache_folderPath, "polygon_financial")
    make_folder(cache_folderPath)
    raw_df = _get_financial_raw_data(cache_folderPath, ticker_list, start_date, end_date)
    ticker_list = list(set(raw_df.columns))
    #待改：start/end的用途？
    preload_items_list = ["start_date", "end_date", "filing_date"]
    item_list = [item for item in raw_df.index if item not in preload_items_list]
    #待改：應該是只需要存交易日資料即可，之後清洗
    date_range_list = pd.date_range(start_date, end_date)
    for item in item_list:
        item_df = pd.DataFrame(index=date_range_list, columns=ticker_list)
        for ticker in ticker_list:
            item_df_per_ticker = raw_df.loc[preload_items_list+[item], ticker]
            if type(item_df_per_ticker) == pd.Series:
                item_df_per_ticker = item_df_per_ticker.to_frame()
            num_reports = len(item_df_per_ticker.columns)
            for i in range(num_reports):
                report_series = item_df_per_ticker.iloc[:, i]
                #若filing_date超出抓取結束日期，則以後者替代，以限定資料儲存區間
                fill_date = min(report_series["filing_date"], end_date)
                item_df.loc[fill_date, ticker] = report_series[item]
        # 以前值替補空值
        item_df = item_df.ffill()
        # 將塊狀資料儲存為時間序列資料
        _save_blockData_to_seriesData(folderPath, item_df, item, com_prev_data=False)

# 自Polygon下載財報資料 - 呼叫API取得資料（為塊狀資料，格式較雜亂）
def _get_financial_raw_data(cache_folderPath, ticker_list, start_date, end_date):
    df_list = list()
    '''
    因資料筆數多（約15000筆），中途可能因網路問題導致報錯，為避免須重新下載而設計cache機制；
    已抓取的資料會存於cache_financial_data資料夾，會自動讀取（每次下載前須清空cache）
    '''
    for index, ticker in enumerate(ticker_list, 0):
        cache_filePath = os.path.join(cache_folderPath, ticker+".json")
        if not os.path.exists(cache_filePath):
            url = "https://api.polygon.io/vX/reference/financials?ticker={ticker}&period_of_report_date.gte={start_date}&period_of_report_date.lte={end_date}&timeframe=quarterly&include_sources=false&apiKey=VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH". \
               format(start_date=start_date, end_date=end_date, ticker=ticker)
        
            data_json = requests.get(url).json()
            with open(cache_filePath, "w") as fp:
                json.dump(data_json, fp)
            logging.info("[FC][polygon][{tikcer}-{index}:資料已下載至cache".format(tikcer, index))
        else:
            with open(cache_filePath, 'r') as fp:
                data_json = json.load(fp)
            logging.info("[FC][polygon][{tikcer}-{index}:資料已存在於cache".format(tikcer, index))
    
        for i in range(len(data_json["results"])):
            data_json_per_quarter = data_json["results"][i]
            #回傳資料不一定有載明filing_date，若無載明則預設以財報代表區間結束日後90日，為filing_date
            rp_start_date, rp_end_date = data_json_per_quarter["start_date"], data_json_per_quarter["end_date"]
            filing_date = data_json_per_quarter.get("filing_date", shift_days_by_strDate(rp_end_date, days=90))

            series_list = list()
            for statement in ["income_statement", "balance_sheet", "cash_flow_statement", "comprehensive_income"]:
                if statement not in data_json_per_quarter["financials"].keys():
                    continue
                statement_dict = data_json_per_quarter["financials"][statement]
                statement_df = pd.DataFrame(statement_dict)
                series_list.append(statement_df.T["value"])

            data_series = pd.concat(series_list).rename(ticker)            
            data_series["start_date"], data_series["end_date"] = rp_start_date, rp_end_date
            data_series["filing_date"] = filing_date
            df_list.append(data_series)

    return pd.concat(df_list, axis=1)

# 自Polygon下載財報資料 - 將塊狀資料切分為時序資料，並依照需求補前資料 
def _save_blockData_to_seriesData(folderPath, df, item, com_prev_data):
    item_folderPath = os.path.join(folderPath, item)
    make_folder(item_folderPath)
    #待改：思考有需要補嗎？寫策略的時候再補是不是比較好？
    #補足前期資料
    if com_prev_data:
        prev_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
        count = 0
        while True:
        # 依照日期往前尋找最靠近的資料，若前一日非交易日，則繼續往前
            fileName = os.path.join(item_folderPath, prev_date.strftime("%Y-%m-%d")+".csv")
            if os.path.exists(fileName) == False:
                prev_date = prev_date - timedelta(days=1)
                count += 1
            else:
                break
                
            #待改：避免前面無資料會持續迴圈
            if count >= 10:
                break
        logging.warning("採用{date}資料補齊前值".format(date=prev_date.strftime("%Y-%m-%d")))
        
        if os.path.exists(fileName):
            prev_series = pd.read_csv(fileName, index_col=0)
            prev_series = prev_series.T
            for ticker in df.columns:
                prev_value = prev_series[ticker].values[0]
                df[ticker].fillna(prev_value, inplace=True)
        
    df = df.T
    for num in range(len(df.columns)):
        # 依序將各column切出，轉為csv檔，檔名為該日日期
        data_series = df.iloc[:, num]
        date = data_series.name.strftime("%Y-%m-%d")
        # 存檔至以指定資料項目為名的資料夾之下
        fileName = os.path.join(item_folderPath, date+".csv")
        data_series.to_csv(fileName)


### 基本面資料流程
# 1. 一次性歷史資料抓取（依照filing date抓）
# 2. 一次性建立1-12歷史資料
# 3. 資料路徑設定（依照4表分類）
# 4. 建立每日更新函數
# 5. debug
#       - 多數公司不公布Q4，而是含在年報中

### note：
# 六日也可能發布財報
# polygon的資料2009年後才開始（待確認）
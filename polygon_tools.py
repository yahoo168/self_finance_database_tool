import requests
import os, sys, logging, time
from .utils import *
from threading import Thread, Lock
import pandas as pd

# 自polygon下載EOD價量相關資料，若無指定起始/結束日期，則自動以上次更新日期的後一日開始抓取資料，更新至今日
def save_stock_priceVolume_from_Polygon(folder_path, API_key, start_date=None, end_date=None, adjust=False):
    def _save_stock_priceVolume_from_Polygon_singleDate(folder_path, API_key, item_list, date, adjust):
        if adjust==True:
            adjust_flag = "true"
        elif adjust==False:
            adjust_flag = "false"

        url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjust_flag}&apiKey={API_key}".format(date=date, adjust_flag=adjust_flag, API_key=API_key)
        data_json = requests.get(url).json()
        # DELAYED為盤後交易時段，大約台灣時間中午12點方能取得昨日的正式收盤價（確切時間待確定）
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
            item_folder_path = os.path.join(folder_path, item)
            fileName = os.path.join(item_folder_path, date+".csv")
            data_series.to_csv(fileName)

    item_list = ["open", "high", "low", "close", "volume", "avg_price", "transaction_num"]

    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()
    # 逐日下載資料，儲存與字典當中
    for index, date in enumerate(date_range_list, 1):
        t = Thread(target=_save_stock_priceVolume_from_Polygon_singleDate, 
            args=(folder_path, API_key, item_list, date, adjust))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(date_range_list), 2)            
        logging.info("[{date}][OHLCV] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))

    for t in threads:
        t.join()

def save_stock_split_from_Polygon(API_key, date_list):
    def _save_stock_split_from_Polygon_singleDate(API_key, date, data_dict):
        url = "https://api.polygon.io/v3/reference/splits?execution_date={date}&apiKey={API_key}".format(date=date, API_key=API_key)
        data_json = requests.get(url).json()
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df["adjust_factor"] = df["split_to"] / df["split_from"]
            df = df.pivot(index="execution_date", columns="ticker", values="adjust_factor").T
            data_dict[date] = df.sort_index()

    data_dict = dict()
    threads = list()  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_list, 1):
            t = Thread(target=_save_stock_split_from_Polygon_singleDate, 
                        args=(API_key, date, data_dict))
            t.start()  # 開啟線程

            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(date_list), 2)            
            logging.info("[{date}][split] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

    return data_dict

def save_stock_cash_dividend_from_Polygon(folder_path, API_key, start_date, end_date, div_type):
    def _save_stock_cash_dividend_from_Polygon_singleDate(folder_path, div_type, date, API_key):
        #只下載現金股利（CD）
        url = "https://api.polygon.io/v3/reference/dividends?{0}={1}&apiKey={2}&dividend_type=CD".format(div_type, date, API_key)
        data_json = requests.get(url).json()
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df = df.pivot(index="ticker", columns=div_type, values="cash_amount")
            filePath = os.path.join(folder_path, date+".csv")
            df.to_csv(filePath)
    
    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_range_list, 1):
            t = Thread(target=_save_stock_cash_dividend_from_Polygon_singleDate, 
                        args=(folder_path, div_type, date, API_key))
            t.start()  # 開啟線程
            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(date_range_list), 2)            
            logging.info("[{date}][dividends][{div_type}] 資料下載中，完成度{percentage}%".format(date=date, div_type=div_type, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)
 
def save_stock_shares_outstanding_from_Polygon(API_key, ticker_list, start_date, end_date):
    # 流通股數係透過polygon中的ticker detail資訊取得，索取方式為給定ticker與date，故包裝為雙重函數
    def _save_stock_shares_outstanding_from_Polygon_singleDate(API_key, ticker_list, date):
        def _save_stock_shares_outstanding_from_Polygon_singleTicker(API_key, ticker, date):
            url = "https://api.polygon.io/v3/reference/tickers/{}?date={}&apiKey={}".format(ticker, date, API_key)
            data_json = requests.get(url).json()
            #註：不能用weighted shares
            data_dict[ticker] = data_json["results"]["share_class_shares_outstanding"]
        
        data_dict = dict()
        threads = list()
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

        return pd.Series(data_dict)

    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date,freq='d'))))
    result_dict = dict()
    for date in date_range_list:
        data_series = _save_stock_shares_outstanding_from_Polygon_singleDate(API_key, ticker_list, date)
        result_dict[date] = data_series
    return result_dict

def save_stock_financialReport_data_from_Polygon(API_key, date_list):
    def _save_stock_financialReport_data_from_Polygon_singleDate(API_key, date, data_dict):
        data_list = list()
        #若直接用filing_date=date索引，polygon會同時返回後一天的資料，目前不知原因，故採lte與gte連用進行索引
        url = "https://api.polygon.io/vX/reference/financials?filing_date.gte={filing_date}&filing_date.lte={filing_date}&apiKey={API_key}&limit=100&timeframe=quarterly".format(filing_date=date, API_key=API_key)
        data_json = requests.get(url).json()
        
        if len(data_json["results"]) == 0:
            return 0
        # 因polygon財報資料有索引上限100，財報季時每日通常超過100家公司發布財報，故須進行翻頁索引
        data_list.extend(data_json["results"])
        while True:
            if "next_url" in data_json.keys():
                next_url = data_json["next_url"] + "&apiKey={API_key}&limit=100".format(API_key=API_key)
                data_json = requests.get(next_url).json()
                data_list.extend(data_json["results"])
            else:
                break
        
        # data_list儲存該日完成翻頁索引後，所有raw_data（dict）
        raw_data_dict = dict()
        for data in data_list:
            # 有的ticker會是空值或是None，故須進行例外處理
            if "tickers" in data.keys() and data["tickers"] != None:
                # data[tickers]為一list，正常情況僅有1個ticker，然而可能出現公司有多檔ticker（如Google）的情況
                ticker = data["tickers"][0]
                # raw_data中的財務數據是以財務四表為key，逐一索取
                # Note：資料庫中的基本面財報資料也以財務四表整理，然而在此處是將四表數據統一索取，入庫時參照data_path的分類再個別整理，並非於此處分類
                statement_dict = {}
                for statement in ["income_statement", "balance_sheet", "cash_flow_statement", "comprehensive_income"]:
                    # 部分公司可能不具有四表中的部分報表資料
                    if statement not in data["financials"].keys():
                        continue
                    statement_dict.update(data["financials"][statement])
                # raw_data尚包含unit...等資料，此處拋棄其餘資料，僅取值
                raw_data_dict[ticker] = pd.DataFrame(statement_dict).loc["value", :]
            else:
                #logging.warning("Ticker缺失")
                continue
        
        # row:各報表科目名稱； col: 當日有發布財報的公司ticker
        df = pd.DataFrame(raw_data_dict).fillna(0)
        data_dict[date] = df
    
    data_dict = dict()
    threads = list()
    for index, date in enumerate(date_list, 1):
        t = Thread(target=_save_stock_financialReport_data_from_Polygon_singleDate, 
            args=(API_key, date, data_dict))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(date_list), 2)            
        logging.info("[{date}]財報資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))

    for t in threads:
        t.join()

    return data_dict


def save_stock_universe_list_from_polygon(API_key, universe_name, date_list):    
    # 轉為polygon的URL辨識碼
    if universe_name=="univ_us_stock":
        ticker_type = "CS"
    elif universe_name=="univ_us_etf":
        ticker_type = "ETF"
    
    data_dict = dict()
    for date in date_list:
        ticker_list = list()
        url = "https://api.polygon.io/v3/reference/tickers?type={ticker_type}&date={date}&market=stocks&active=True&limit=1000&apiKey={API_key}".format(ticker_type=ticker_type, date=date, API_key=API_key)
        data_json = requests.get(url).json()
        # 因polygon標的資料有索引上限1000，故須進行翻頁索引
        part_ticker_list = list(pd.DataFrame(data_json["results"])["ticker"])
        ticker_list.extend(part_ticker_list)
        while True:
            if "next_url" in data_json.keys():
                next_url = data_json["next_url"] + "&apiKey={API_key}&limit=1000".format(API_key=API_key)
                data_json = requests.get(next_url).json()
                part_ticker_list = list(pd.DataFrame(data_json["results"])["ticker"])
                ticker_list.extend(part_ticker_list)
            else:
                break
    
        data_dict[date] = pd.Series(ticker_list)
        logging.info("[SAVE][{universe_name}][{date}]成分股資料抓取完成".format(universe_name=universe_name, date=date))

        #TEMP
        # folder_path = "/Users/yahoo168/Documents/工作管理/AlphaHelix/投資研究/Quant/回測系統/Database/raw_data/US_stock/universe/univ_us_stock"
        # file_path = os.path.join(folder_path, date+".csv")
        # data_dict[date].to_csv(file_path)
    
    return data_dict

def save_stock_delisted_info_from_polygon(folder_path, universe_type, API_key):
    df_list = list()
    url = "https://api.polygon.io/v3/reference/tickers?type={universe_type}&market=stocks&active=false&limit=1000&apiKey={API_key}".format(universe_type=universe_type, API_key=API_key)
    data_json = requests.get(url).json()

    # 因polygon標的資料有索引上限1000，故須進行翻頁索引
    df = pd.DataFrame(data_json["results"])
    df_list.append(df)
    while True:
        if "next_url" in data_json.keys():
            next_url = data_json["next_url"] + "&apiKey={API_key}&limit=1000".format(API_key=API_key)
            data_json = requests.get(next_url).json()
            df = pd.DataFrame(data_json["results"])
            df_list.append(df)
        else:
            break
    
    df = pd.concat(df_list)
    # 原日期編碼為utc字串，前10碼為年月日
    df["delisted_date"] = df["delisted_utc"].apply(lambda x:x[:10])
    df = df.loc[:, ["ticker", "name", "delisted_date"]]
    df = df.drop_duplicates(subset=["ticker"])
    df = df.sort_values(by="delisted_date")
    df = df.reset_index(drop=True)
    date = datetime2str(datetime.today())

    if universe_type=="CS":
        file_name = "delisted_stock_info"

    if universe_type=="ETF":
        file_name = "delisted_etf_info"

    filePath = os.path.join(folder_path, file_name+".csv")
    df.to_csv(filePath)

def save_stock_company_info_from_Polygon(API_key, ticker_list):
    def _save_stock_company_info_from_Polygon_singleTicker(API_key, ticker):
        url = "https://api.polygon.io/v3/reference/tickers/{}?&apiKey={}".format(ticker, API_key)
        data_json = requests.get(url).json()
        company_info_dict[ticker] = data_json["results"]
    
    company_info_dict = {ticker:dict() for ticker in ticker_list}
    threads = list()
    for index, ticker in enumerate(ticker_list, 1):
        t = Thread(target=_save_stock_company_info_from_Polygon_singleTicker, 
            args=(API_key, ticker))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(ticker_list), 2)            
        logging.info("[{ticker}]公司資訊下載中，完成度{percentage}%".format(ticker=ticker, percentage=percentage))

    for t in threads:
        t.join()

    company_info_df = pd.DataFrame(company_info_dict).T
    return company_info_df

# 最久一年執行一次即可，待改：未給明路徑
def save_stock_market_status_series(self):
    url = "https://api.polygon.io/v1/marketstatus/upcoming?apiKey=vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj"
    data_json = requests.get(url).json()
    holiday_status_df = df = pd.DataFrame(data_json)
    closed_status_df = df[df["status"]=="closed"]
    holiday_series = closed_status_df["date"].drop_duplicates()

    start_date = datetime2str(datetime.today())
    end_date = datetime2str(datetime.today()+timedelta(days=365))

    date_range_series = pd.Series(pd.date_range(start_date, end_date, freq='d'))
    market_status_df = pd.DataFrame(index=date_range_series, columns=["market_status"])
            
    is_weekend_series = date_range_series.apply(lambda x:x.day_name() in (['Saturday', 'Sunday']))
    weekend_series = date_range_series[is_weekend_series]

    # 1: 交易日, 0:六日休市, -1:非六日休市
    market_status_df.loc[weekend_series, :] = 0
    market_status_df.loc[holiday_series, :] = -1
    market_status_df = market_status_df.fillna(1)
    return market_status_df

# 自Polygon下載財報資料 - 主程式（待改：S/D問題）
# def save_stock_financialReport_from_Polygon(folder_path, ticker_list, start_date, end_date):
#     ticker_list = ticker_list
#     cache_folder_path = os.path.join(self.cache_folder_path, "polygon_financial")
#     make_folder(cache_folder_path)
#     raw_df = _get_financial_raw_data(cache_folder_path, ticker_list, start_date, end_date)
#     ticker_list = list(set(raw_df.columns))
#     #待改：start/end的用途？
#     preload_items_list = ["start_date", "end_date", "filing_date"]
#     item_list = [item for item in raw_df.index if item not in preload_items_list]
#     #待改：應該是只需要存交易日資料即可，之後清洗
#     date_range_list = pd.date_range(start_date, end_date)
#     for item in item_list:
#         item_df = pd.DataFrame(index=date_range_list, columns=ticker_list)
#         for ticker in ticker_list:
#             item_df_per_ticker = raw_df.loc[preload_items_list+[item], ticker]
#             if type(item_df_per_ticker) == pd.Series:
#                 item_df_per_ticker = item_df_per_ticker.to_frame()
#             num_reports = len(item_df_per_ticker.columns)
#             for i in range(num_reports):
#                 report_series = item_df_per_ticker.iloc[:, i]
#                 #若filing_date超出抓取結束日期，則以後者替代，以限定資料儲存區間
#                 fill_date = min(report_series["filing_date"], end_date)
#                 item_df.loc[fill_date, ticker] = report_series[item]
#         # 以前值替補空值
#         item_df = item_df.ffill()
#         # 將塊狀資料儲存為時間序列資料
#         _save_blockData_to_seriesData(folder_path, item_df, item, com_prev_data=False)

# # 自Polygon下載財報資料 - 呼叫API取得資料（為塊狀資料，格式較雜亂）
# def _get_financial_raw_data(cache_folder_path, ticker_list, start_date, end_date):
#     df_list = list()
#     '''
#     因資料筆數多（約15000筆），中途可能因網路問題導致報錯，為避免須重新下載而設計cache機制；
#     已抓取的資料會存於cache_financial_data資料夾，會自動讀取（每次下載前須清空cache）
#     '''
#     for index, ticker in enumerate(ticker_list, 0):
#         cache_filePath = os.path.join(cache_folder_path, ticker+".json")
#         if not os.path.exists(cache_filePath):
#             url = "https://api.polygon.io/vX/reference/financials?ticker={ticker}&period_of_report_date.gte={start_date}&period_of_report_date.lte={end_date}&timeframe=quarterly&include_sources=false&apiKey=VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH". \
#                format(start_date=start_date, end_date=end_date, ticker=ticker)
        
#             data_json = requests.get(url).json()
#             with open(cache_filePath, "w") as fp:
#                 json.dump(data_json, fp)
#             logging.info("[FC][polygon][{tikcer}-{index}:資料已下載至cache".format(tikcer, index))
#         else:
#             with open(cache_filePath, 'r') as fp:
#                 data_json = json.load(fp)
#             logging.info("[FC][polygon][{tikcer}-{index}:資料已存在於cache".format(tikcer, index))
    
#         for i in range(len(data_json["results"])):
#             data_json_per_quarter = data_json["results"][i]
#             #回傳資料不一定有載明filing_date，若無載明則預設以財報代表區間結束日後90日，為filing_date
#             rp_start_date, rp_end_date = data_json_per_quarter["start_date"], data_json_per_quarter["end_date"]
#             filing_date = data_json_per_quarter.get("filing_date", shift_days_by_strDate(rp_end_date, days=90))

#             series_list = list()
#             for statement in ["income_statement", "balance_sheet", "cash_flow_statement", "comprehensive_income"]:
#                 if statement not in data_json_per_quarter["financials"].keys():
#                     continue
#                 statement_dict = data_json_per_quarter["financials"][statement]
#                 statement_df = pd.DataFrame(statement_dict)
#                 series_list.append(statement_df.T["value"])

#             data_series = pd.concat(series_list).rename(ticker)            
#             data_series["start_date"], data_series["end_date"] = rp_start_date, rp_end_date
#             data_series["filing_date"] = filing_date
#             df_list.append(data_series)

#     return pd.concat(df_list, axis=1)

# # 自Polygon下載財報資料 - 將塊狀資料切分為時序資料，並依照需求補前資料 
# def _save_blockData_to_seriesData(folder_path, df, item, com_prev_data):
#     item_folder_path = os.path.join(folder_path, item)
#     make_folder(item_folder_path)
#     #待改：思考有需要補嗎？寫策略的時候再補是不是比較好？
#     #補足前期資料
#     if com_prev_data:
#         prev_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
#         count = 0
#         while True:
#         # 依照日期往前尋找最靠近的資料，若前一日非交易日，則繼續往前
#             fileName = os.path.join(item_folder_path, prev_date.strftime("%Y-%m-%d")+".csv")
#             if os.path.exists(fileName) == False:
#                 prev_date = prev_date - timedelta(days=1)
#                 count += 1
#             else:
#                 break
                
#             #待改：避免前面無資料會持續迴圈
#             if count >= 10:
#                 break
#         logging.warning("採用{date}資料補齊前值".format(date=prev_date.strftime("%Y-%m-%d")))
        
#         if os.path.exists(fileName):
#             prev_series = pd.read_csv(fileName, index_col=0)
#             prev_series = prev_series.T
#             for ticker in df.columns:
#                 prev_value = prev_series[ticker].values[0]
#                 df[ticker].fillna(prev_value, inplace=True)
        
#     df = df.T
#     for num in range(len(df.columns)):
#         # 依序將各column切出，轉為csv檔，檔名為該日日期
#         data_series = df.iloc[:, num]
#         date = data_series.name.strftime("%Y-%m-%d")
#         # 存檔至以指定資料項目為名的資料夾之下
#         fileName = os.path.join(item_folder_path, date+".csv")
#         data_series.to_csv(fileName)
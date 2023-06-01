import os, sys, logging, time
import pandas as pd
import yfinance as yf

import time
import pickle, json, string
import shutil, random
from threading import Thread, Lock
from datetime import datetime, date, timedelta
from yahoo_fin.stock_info import *
from fredapi import Fred

from .utils import *
from .polygon_tools import *
from .yfinance_tools import *
from .yahoo_fin_tools import *

logging.basicConfig(level=logging.INFO,
    format='[%(asctime)s %(levelname)-8s] %(message)s',
    datefmt='%Y%m%d %H:%M:%S',)

class Database(object):
    '''
    - Database為一資料庫物件，透過給定的folder_path對指定的資料庫（資料夾）進行操作
    - 功能可分為下載(save)與呼叫(get)資料兩大類
    - 資料庫中股號間的特殊字符一律以 "-" 作為連接符，如：BRK_B, TW_0050, ...
    '''
    def __init__(self, folder_path):
        self.database_folder_path = folder_path
        self.US_stock_folderName = "US_stock"
        self.TW_stock_folderName = "TW_stock"  
        self.ticker_folderName = "Ticker"


        self.macro_folderName = "Macro"
        self.macro_token_trans_folderName = "Token_trans"
        self.cache_folderName = "Cache"
        self.polygon_API_key = "VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH"

        #待改：可去掉folderName直接用folderPath，逐步改完
        self.US_stock_folderPath = os.path.join(self.database_folder_path, self.US_stock_folderName)
        self.TW_stock_folderPath = os.path.join(self.database_folder_path, self.TW_stock_folderName)
        self.ticker_folderPath = os.path.join(self.database_folder_path, self.ticker_folderName)
        self.merged_table_folderPath = os.path.join(self.database_folder_path, "Merged_Table")

        self.macro_folderPath = os.path.join(self.database_folder_path, self.macro_folderName)
        self.cache_folderPath = os.path.join(self.database_folder_path, self.cache_folderName)

        # 若某一類資料會頻繁被重複呼叫，可存放在cache_dict中 (可參考get_next_tradeDate對於cache的使用方式)
        self.cache_dict = {}

        make_folder(self.US_stock_folderPath)
        make_folder(self.TW_stock_folderPath)
        make_folder(self.ticker_folderPath)
        make_folder(self.macro_folderPath)
        make_folder(self.cache_folderPath)

    # 目前name的選項：US_all, US_SP500, US_NASDAQ, TW_all, TW_0050,
    def get_ticker_list(self, name="US_all"):
        folderPath = self.ticker_folderPath
        data_status_df = self.get_data_status([name], data_class="ticker")
        date = data_status_df.at[name, "end_date"]
        fileName = os.path.join(folderPath, name , date+".csv")  
        ticker_series = pd.read_csv(fileName, index_col=0).squeeze()
        return ticker_series.to_list()

    def save_ticker_list(self, name="US_all", source="polygon"):
        # 待改：多資料源管理
        if (name=="US_all") and (source=="polygon"):
            ticker_list = list()
            letter_list = list(string.ascii_uppercase)
            interval_letter_list = list()
            for i in range(len(letter_list)-1):
                interval_letter_list.append([letter_list[i], letter_list[i+1]])
            
            for interval in interval_letter_list:
                interval_ticker_list = list()
                start_letter, end_letter = interval[0], interval[1]
                for ticker_type in ["CS", "ETF"]:
                    url = "https://api.polygon.io/v3/reference/tickers?type={}&market=stocks&active=true&limit=1000&apiKey=VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH&ticker.gte={}&ticker.lt={}" \
                          .format(ticker_type, start_letter, end_letter)
                    data_json = requests.get(url).json()
                    interval_ticker_list.extend(pd.DataFrame(data_json["results"])["ticker"].to_list())
                
                logging.info("{}:標的列表下載，字段區間[{}~{}]共{}檔標的".format(name, start_letter, end_letter, len(interval_ticker_list)))
                ticker_list.extend(interval_ticker_list)
                ticker_series = pd.Series(list(set(ticker_list)))
        
        date = datetime2str(datetime.today())
        fileName = os.path.join(self.ticker_folderPath, name, date+".csv")
        ticker_series.to_csv(fileName)
        return ticker_series

    # 取得特定資料儲存狀況：起始日期/最新更新日期/資料筆數
    def _get_single_data_status(self, item, country, data_class):
        if data_class == "ticker":
            folderPath = self.ticker_folderPath

        if data_class=="stock":
            if country == "US":
                folderPath = self.US_stock_folderPath
            elif country == "TW":
                folderPath = self.TW_stock_folderPath

        item_folderPath = os.path.join(folderPath, item)
        
        status_dict = dict()
        
        #若資料夾不存在或雖存在但其中沒有檔案，則返回空值
        if not os.path.exists(item_folderPath) or len(os.listdir(item_folderPath))==0:
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = None, None, 0
        #待改：部分資料非時序型資料，待處理
        else:
            # 取得指定項目資料夾中的檔案名
            raw_date_list = os.listdir(item_folderPath)
            # 去除後綴檔名
            date_list = [date.split(".")[0] for date in raw_date_list]
            # 去除異常空值
            date_list = [date for date in date_list if len(date)!=0]
            # 建立Series以篩選時間
            date_series = pd.Series(date_list)
            date_series = date_series.apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
            start_date = date_series.iloc[0].strftime("%Y-%m-%d")
            end_date = date_series.iloc[-1].strftime("%Y-%m-%d")
            date_num = len(date_series)
            
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = start_date, end_date, date_num
        return status_dict

    # 取得給定資料儲存狀況：起始日期/最新更新日期/資料筆數 (Note：須為序列資料)
    def get_data_status(self, item_list, country="US", data_class="stock"):
        status_dict = dict()
        for item in item_list:
            status = self._get_single_data_status(item, country, data_class)
            status_dict[item] = status
        return pd.DataFrame(status_dict).T

    # 取得特定區域的交易日日期序列，區域預設為美國（US），其他選項包括：TW（台灣）
    def get_tradeDate_list(self, country="US"):
        if country == "US":
            filePath = os.path.join(self.US_stock_folderPath, "tradeDate", "tradeDate.csv")

        elif country == "TW":
            filePath = os.path.join(self.TW_stock_folderPath, "tradeDate", "tradeDate.csv")
        #待改：直接回傳series後續應該比較好操作才對
        tradeDate_series = pd.read_csv(filePath).squeeze()
        tradeDate_list = tradeDate_series.to_list()
        self.cache_dict["tradeDate_list"] = tradeDate_list
        return tradeDate_list

    # 待改：get_next_tradeDate與get_last_tradeDate應該合併
    # 若給定日期並非交易日，則取得距離最近的下一個實際交易日（區域預設為美國（US）），其他選項包括：TW（台灣）
    def get_next_tradeDate(self, date, shift_days=0, country="US"):
        # 若輸入date為字串，先轉為datetime物件
        if type(date) is str:
            date = str2datetime(date)

        date = date + timedelta(days=shift_days)
        # 待改：多區域時，資料會被覆寫
        if "tradeDate_list" in self.cache_dict.keys():
            tradeDate_list = self.cache_dict["tradeDate_list"]

        else:
            tradeDate_list = self.get_tradeDate_list(country=country)

        if date not in tradeDate_list:
            virtual_date_list = tradeDate_list.copy()
            virtual_date_list.append(date)
            virtual_date_list.sort()
            latest_tradeDate_index = virtual_date_list.index(date)
            return tradeDate_list[latest_tradeDate_index]
        else:
            return date

    # 若給定日期並非交易日，則取得距離最近的上一個實際交易日（區域預設為美國（US））
    def get_last_tradeDate(self, date, shift_days=0, country="US"):
        # 若輸入date為字串，先轉為datetime物件
        if type(date) is str:
            date = str2datetime(date)

        date = date+timedelta(days=shift_days)
        if "tradeDate_list" in self.cache_dict.keys():
            tradeDate_list = self.cache_dict["tradeDate_list"]
        else:
            tradeDate_list = self.get_tradeDate_list(country=country)

        if date not in tradeDate_list:
            virtual_date_list = tradeDate_list.copy()
            virtual_date_list.append(date)
            virtual_date_list.sort()        
            latest_tradeDate_index = virtual_date_list.index(date)
            return tradeDate_list[latest_tradeDate_index-1]
        else:
            return date

    # 取得FRED官網公布的總經數據，name的可用選項，見Macro/Token_trans/fred.txt
    def get_fred_data(self, name):
        folderPath = self.macro_folderPath
        filePath = os.path.join(folderPath, name+".pkl")
        data_df = pd.read_pickle(filePath)  
        return data_df

    def get_cache_df(self, asset_class, universe_name, item, start_date, end_date, country, pre_fetch_nums=0):
        target_ticker_list = self.get_ticker_list(universe_name)
        
        folderPath = os.path.join(self.cache_folderPath, "get", asset_class)
        filePath = os.path.join(folderPath, item+".csv")

        start_date = start_date + timedelta(days = -pre_fetch_nums)
        
        if os.path.exists(filePath):
            logging.warning("Cache存在")
            cache_df = pd.read_csv(filePath, index_col=0)
            cache_df.index = pd.to_datetime(cache_df.index)

            lost_ticker_list = list(set(target_ticker_list) - set(cache_df.columns))
            re_target_ticker_list = list(set(target_ticker_list) - set(lost_ticker_list))
            
            if len(lost_ticker_list) > 0:
                logging.warning("資料項目:{}共{}檔標的缺失，缺失標的如下:".format(item, len(lost_ticker_list)))
                logging.warning(lost_ticker_list)

            mask = (cache_df.index>=start_date) & (cache_df.index<=end_date)
            cache_df = cache_df.loc[mask, re_target_ticker_list]
            print(cache_df)

        else:
            logging.warning("Cache不存在")
            cache_df = pd.DataFrame()
        return cache_df

    # 將時序資料取出，組合為塊狀資料（Row:date, column: ticker)，若不指定資料時間區段，則預設為全部取出
    def get_stock_data_df(self, item, target_ticker_list=None, start_date="1900-01-01", 
                                end_date="2100-12-31", pre_fetch_nums=0, country="US"):
        if country == "US":
            folderPath = self.US_stock_folderPath
        elif country == "TW":
            folderPath = self.TW_stock_folderPath

        # 待改：應該直接使用dateime?
        if type(start_date) is not str:
            start_date = datetime2str(start_date)
        if type(end_date) is not str:
            end_date = datetime2str(end_date)

        item_folderPath = os.path.join(folderPath, item)
        # 取得指定資料夾中所有檔案名(以時間戳記命名）
        raw_date_list = os.listdir(item_folderPath)
        # 去除檔案名中的後綴名（.csv)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（可能由.DS_store之類的隱藏檔所導致）
        date_list = [date for date in date_list if len(date) != 0]
        # 將時間戳記組合成date series，以篩選出指定的資料區間（同時進行排序與重設index）
        date_series = pd.Series(date_list).apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        # 向前額外取N日資料，以使策略起始時便有前期資料可供計算
        start_date = shift_days_by_strDate(start_date, -pre_fetch_nums)
        mask = (date_series >= start_date) & (date_series <= end_date) 
        # 將date series轉為將date list（字串列表），以用於後續讀取資料
        date_list = list(map(lambda x:datetime2str(x), date_series[mask]))
        # 依照date list逐日取出資料，組合為DataFrame
        df_list = list()
        for date in date_list:
            fileName = os.path.join(item_folderPath, date+".csv")
            df_list.append(pd.read_csv(fileName, index_col=0))
        df = pd.concat(df_list, axis=1).T.sort_index()
        
        # 若不指定ticker，則預設為全部取出
        if target_ticker_list == None:
            return df

        lost_ticker_list = list(set(target_ticker_list) - set(df.columns))
        re_target_ticker_list = list(set(target_ticker_list) - set(lost_ticker_list))
        if len(lost_ticker_list) > 0:
            logging.warning("資料項目:{}共{}檔標的缺失，缺失標的如下:".format(item, len(lost_ticker_list)))
            logging.warning(lost_ticker_list)

        df.index = pd.to_datetime(df.index)
        return df.loc[:, re_target_ticker_list]
    
    ## 呼叫資料的函數區域（結束）

    ## 下載資料的函數區域（開始）
    def save_stock_priceVolume_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, adjust=False, source="polygon", country="US"):
        if start_date == None:
            status_df = self.get_data_status(["open"], country=country, data_class="stock")
            if status_df.at["open", "date_num"] > 0:
                #待改：目前以「open」的資料儲存狀況作為判斷最新資料更新日期，應改為config
                start_date = status_df.at["open", "end_date"]
                start_date = str2datetime(start_date) + timedelta(days=1)
                start_date = datetime2str(start_date)
            else:
                start_date = "1900-01-01"

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            folderPath = self.US_stock_folderPath

        if source == "polygon":
            save_stock_priceVolume_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, adjust)
        
        elif source == "yfinance":
            cache_folderPath = os.path.join(self.cache_folderPath, "yfinance_priceVolume")
            make_folder(cache_folderPath)
            save_stock_priceVolume_from_yfinance(folderPath, cache_folderPath, start_date, end_date, ticker_list, country, adjust)

        elif source == "yahoo_fin":
            pass
    
    def save_stock_financialReport_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, source="polygon", country="US"):
        if ticker_list == None:
            ticker_list = self.get_ticker_list("US_all")

        if start_date == None:
            status_df = self.get_data_status(["revenues"], country=country, data_class="stock")
            #待改：目前以「revenues」的資料儲存狀況作為判斷最新資料更新日期，應改為config
            start_date = status_df.at["revenues", "end_date"]
            start_date = str2datetime(start_date) + timedelta(days=1)
            start_date = datetime2str(start_date)

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            folderPath = self.US_stock_folderPath
        else:
            pass

        if source == "polygon":
            save_stock_financialReport_from_Polygon(folderPath, ticker_list, start_date, end_date)
    
    def save_stock_tradeDate(self, source="yfinance", country="US"):
        #待改：多資料源處理 + 應標記非交易日是六日or假日(holidays)，可用1、0、-1標記
        if source=="yfinance":
            if country=="US":
                #SPX
                reference_df = yf.Ticker("^GSPC").history(period="max")
                folderPath = os.path.join(self.US_stock_folderPath, "tradeDate")

            elif country=="TW":
                #0050(暫時找不到台指在yahoo的代碼)
                reference_df = yf.Ticker("0050.TW").history(period="max")
                folderPath = os.path.join(self.TW_stock_folderPath, "tradeDate")

            tradeDate_series = reference_df.index.strftime("%Y-%m-%d").to_series()

        elif source=="yahoo_fin":
           reference_df = _download_data_from_Yahoo_Fin("^GSPC")

        make_folder(folderPath)
        filePath = os.path.join(folderPath, "tradeDate.csv")

        tradeDate_series.to_csv(filePath, index=False)
        logging.info("[{country} TradeDate] 已儲存".format(country=country))

    def save_fred_data(self, name):
        folder_path = os.path.join(self.database_folder_path, self.macro_folderName)
        token_trans_folderName = os.path.join(folder_path, self.macro_token_trans_folderName)
        
        api_key = token_trans("API_Key", source="fred", folder_path=token_trans_folderName)
        data_key = token_trans(name, source="fred", folder_path=token_trans_folderName)
        
        fred = Fred(api_key=api_key)
        
        try:
            data_df = fred.get_series(data_key)
            print(name, " has been saved.")
        
        except Exception as e:
            print(e)
            print(name, " doesn't exist in token trans table.")

        file_position = os.path.join(folder_path, name+".pkl")
        data_df.to_pickle(file_position)
        return data_df

    def save_daily_return(self, start_date=None, end_date=None, method="C2C", cal_dividend=False, country="US"):
        if country == "US":
            folderPath = self.US_stock_folderPath
        elif country == "TW":
            folderPath = self.TW_stock_folderPath

        if method == "C2C":
            folderName, price_item = "daily_return_C2C", "close"

        elif method == "O2O":
            folderName, price_item = "daily_return_O2O", "open"

        if cal_dividend == True:
            # example:"daily_return_O2O_with_dividend"
            folderName += "_with_dividend"

        make_folder(os.path.join(folderPath, folderName))
        if start_date == None:
            # 若未指定start_date，則以對應的價格資料（open或close）所存在最早的日期作為start_date
            status_df = self.get_data_status([folderName], country=country, data_class="stock")
            if status_df.at[folderName, "date_num"] > 0:
                #不遞延一天，因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
                start_date = status_df.at[folderName, "start_date"]
            else:
                start_date = "1900-01-01"
        
        if end_date == None:
            end_date = datetime2str(datetime.today())

        price_df = self.get_stock_data_df(item=price_item, start_date=start_date, end_date=end_date, country=country)
        stock_splits_df = self.get_stock_data_df(item="stock_splits", start_date=start_date, end_date=end_date, country=country)
        adjust_factor_df = cal_adjust_factor_df(stock_splits_df, date_list=price_df.index, method="forward")
        # 只針對有分割資料的個股作股價調整
        adjust_ticker_list = price_df.columns.intersection(stock_splits_df.columns)        
        adjusted_item_df = price_df[adjust_ticker_list] * adjust_factor_df
        # 若不給定ticker_list，會導致對賦值時，columns沒有對齊
        price_df[adjust_ticker_list] = adjusted_item_df[adjust_ticker_list]
        # 將股價加上當日除息的現金股利
        if cal_dividend == True:
            dividends_df = self.get_stock_data_df(item="dividends", start_date=start_date, end_date=end_date, country=country)
            dividends_ticker_list = price_df.columns.intersection(dividends_df.columns)
            adjusted_dividends_df = price_df[dividends_ticker_list] + dividends_df[dividends_ticker_list]
            price_df[dividends_ticker_list] = adjusted_dividends_df[dividends_ticker_list]

        # 去除第一row
        return_df = price_df.pct_change().iloc[1:,:]
        # 切分return
        for i in range(len(return_df)):
            data = return_df.iloc[i,:]
            fileName = data.name
            filePath = os.path.join(folderPath, folderName, fileName+".csv")
            data.to_csv(filePath)

    def save_stock_split_data(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if start_date == None:
            status_df = self.get_data_status(["stock_splits"], country=country, data_class="stock")
            if status_df.at["stock_splits", "date_num"] > 0:
                #待改：目前以「stock_splits」的資料儲存狀況作為判斷最新資料更新日期，應改為config
                start_date = status_df.at["stock_splits", "end_date"]
                start_date = str2datetime(start_date) + timedelta(days=1)
                start_date = datetime2str(start_date)
            else:
                start_date = "2000-01-01"

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            folderPath = os.path.join(self.US_stock_folderPath, "stock_splits")
            make_folder(folderPath)
            if source == "polygon":
                save_stock_split_from_Polygon(folderPath, self.polygon_API_key, start_date=start_date, end_date=end_date)

    #儲存現金股利資料
    def save_stock_cash_dividend(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if start_date == None:
            status_df = self.get_data_status(["dividends"], country=country, data_class="stock")
            if status_df.at["dividends", "date_num"] > 0:
                #待改：目前以「dividend」的資料儲存狀況作為判斷最新資料更新日期，應改為config
                start_date = status_df.at["dividends", "end_date"]
                start_date = str2datetime(start_date) + timedelta(days=1)
                start_date = datetime2str(start_date)
            else:
                start_date = "2000-01-01"

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            folderPath = os.path.join(self.US_stock_folderPath, "dividends")
        
        make_folder(folderPath)
        save_stock_cash_dividend_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, date_type="ex_dividend_date")

    #儲存現金股利
    def save_stock_shares_outstanding(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if start_date == None:
            status_df = self.get_data_status(["shares_outstanding"], country=country, data_class="stock")
            if status_df.at["shares_outstanding", "date_num"] > 0:
                #待改：目前以「dividend」的資料儲存狀況作為判斷最新資料更新日期，應改為config
                start_date = status_df.at["shares_outstanding", "end_date"]
                start_date = str2datetime(start_date) + timedelta(days=1)
                start_date = datetime2str(start_date)
            else:
                start_date = "2000-01-01"

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            folderPath = os.path.join(self.US_stock_folderPath, "shares_outstanding")
            make_folder(folderPath)

        if source == "polygon":
            cache_folderPath = os.path.join(self.cache_folderPath, "polygon_shares_outstanding")
            make_folder(cache_folderPath)
            save_stock_shares_outstanding_from_Polygon(folderPath, cache_folderPath, self.polygon_API_key, ticker_list, start_date, end_date)

    def check_ticker_change(self, item, benchmark_date, start_date="2000-01-01", end_date="2100-12-31", country="US"):
        data_df = self.get_stock_data_df(item=item, start_date=start_date, end_date=end_date, country=country)
        benchmark_df = self.get_stock_data_df(item=item, start_date=benchmark_date, end_date=benchmark_date, country=country)
        common_ticker_list, new_ticker_list, disappear_ticker_list = compare_component(data_df.columns.dropna(), benchmark_df.columns.dropna())
        
        logging.info("[Check][{item}]本區間資料相較{date}，共新增{n1}檔標的/減少{n2}檔標的".format(item=item, date=benchmark_date, n1=len(new_ticker_list), n2=len(disappear_ticker_list)))
        
        # if len(new_ticker_list)>0:
        #     logging.info("新增標的:")
        #     #logging.info(new_ticker_list)
        if len(disappear_ticker_list)>0:
        #     logging.info("建議確認以下消失標的之變動情況，是否下市或改名，並進行資料回填：")
        #     logging.info("消失標的:")
        #     logging.info(disappear_ticker_list)
            index_ticker_list = self.get_ticker_list("US_RAY3000")
            disappear_index_ticker_list, _, _ = compare_component(disappear_ticker_list, index_ticker_list)
            if len(disappear_index_ticker_list) > 0:
                logging.info("[Check][{item}]消失標的中，共{n}檔為Russel 3000成份股，清單如下：".format(item=item, n=len(disappear_index_ticker_list)))
                logging.info(disappear_index_ticker_list)

    def save_stock_item_table(self, item_list, start_date=None, end_date=None, ticker_list=None, country="US", update=False):
        for item in item_list:
            if start_date == None:
                status_df = self.get_data_status(item_list=[item], country=country, data_class="stock")
                if status_df.at[item, "date_num"] > 0:
                    start_date = status_df.at[item, "start_date"]
                else:
                    logging.warning("[{item}] 資料不存在，無法轉化為Table型態".format(item=item))
                    continue
            
            if end_date == None:
                end_date = datetime2str(datetime.today())

            item_df = self.get_stock_data_df(item=item, start_date=start_date, end_date=end_date, country=country)
            
            if country == "US":
                folderPath = os.path.join(self.merged_table_folderPath, "US_Stock")

            elif country == "TW":
                folderPath = os.path.join(self.merged_table_folderPath, "TW_Stock")

            make_folder(folderPath)
            filePath = os.path.join(folderPath, item+".csv")

            if update == True:
                old_item_df = pd.read_csv(filePath, index_col=0)
                old_data_lastest_date, new_data_earliest_date = old_item_df.index[-1], item_df.index[0]
                if old_data_lastest_date >= new_data_earliest_date:
                    #待改：其實未必不能往前補資料
                    logging.warning("[{item}]「原資料最後更新日」不得晚於或等於「新資料起始日」，區間錯誤，已略過此Table型態轉換".format(item=item))
                    continue
                
                interval_start_date = shift_days_by_strDate(old_data_lastest_date, 1)
                interval_end_date = shift_days_by_strDate(new_data_earliest_date, -1)
                date_range_list = list(pd.date_range(interval_start_date, interval_end_date, freq='d'))
                date_range_list = list(map(datetime2str, date_range_list))
                tradeDate_list = self.get_tradeDate_list(country=country)
                interval_tradeDate = list(set(date_range_list) & set(tradeDate_list))
                
                if len(interval_tradeDate) > 0:
                    logging.warning("[{item}]「原資料最後更新日」與「新資料起始日」之區間包含交易日，資料有所缺失，已略過此資料項目之Table轉換".format(item=item))
                    continue

                item_df = pd.concat([item_df, old_item_df]).sort_index()

            if ticker_list != None:
                item_df = item_df.loc[:, ticker_list]

            item_df.to_csv(filePath)
            logging.info("[{item}][{start_date}-{end_date}] Table資料轉換&儲存完成".format(item=item, start_date=item_df.index[0], end_date=item_df.index[-1]))
    
    # 前調法：將原始OHLCV依照split轉化為adjusted OHLCV
    # def save_adjusted_data_by_stock_splits(self, start_date, item_list, method="forward", country="US", cal_dividend=False):
    #     if country == "US":
    #         folderPath = self.US_stock_folderPath
    #     elif country == "TW":
    #         folderPath = self.TW_stock_folderPath

    #     stock_splits_df = self.get_stock_data_df(item="stock_splits", start_date=start_date, country=country)
    #     adjusted_item_df_dict = dict()
    #     item_ticker_list = list()
    #     RAY3000_ticker_list = self.get_ticker_list("US_RAY3000")

    #     for item in item_list:
    #         item_df = self.get_stock_data_df(item=item, start_date=start_date)
    #         item_df = item_df.ffill()
    #         adjust_factor_df = cal_adjust_factor_df(stock_splits_df, date_list=item_df.index, method=method)
    #         adjust_ticker_list = item_df.columns.intersection(stock_splits_df.columns)        
    #         adjusted_item_df = item_df[adjust_ticker_list] * adjust_factor_df
    #         # adjusted_close_df後面若不給定ticker，會導致對item_df賦值時，columns對不齊
    #         item_df[adjust_ticker_list] = adjusted_item_df[adjust_ticker_list]
    #         potential_error_ticker_list = list()
    #         if item not in ["volume", "dividends"]:
    #             # threshold不設定為1的原因為負向極端變動不可能超過1（如100元隔日跌至1元）
    #             huge_change_ticker_index = check_potential_error_by_change(item_df, threshold=0.9)
    #             never_split_ticker_index = check_potential_error_by_split(stock_splits_df)
    #             potential_error_ticker_list = list(set(huge_change_ticker_index) & set(never_split_ticker_index)) 
    #             item_df = item_df.drop(potential_error_ticker_list, axis=1)
    #             # 注意：此法無法避免split資料存在但錯誤的情況（如分割日相差前後N日），且目前確實有此問題
    #         component_potential_error_ticker_list = list(set(potential_error_ticker_list).intersection(RAY3000_ticker_list))
    #         if len(potential_error_ticker_list) > 0:
    #             logging.warning("[AdjTrans][{item}]共{n}檔標的可能存在split資料缺失(pct_change異常)，阻止轉換".format(item=item, n=len(potential_error_ticker_list)))
    #             logging.warning("[AdjTrans][{item}]共{n}檔缺失標的屬於Russel 3000成分股\n".format(item=item, n=len(component_potential_error_ticker_list)))
            
    #         adjusted_item_df_dict[item] = item_df
    #         item_ticker_list.append(list(item_df.columns))
        
    #     #只將各資料項目皆通過檢驗的標的入庫，以使調整後資料標的具一致性
    #     complete_ticker_list = list(set(item_ticker_list[0]).intersection(*item_ticker_list[1:]))
    #     lost_ticker_list = list(set(RAY3000_ticker_list) - set(complete_ticker_list))
    #     # 待改：此處的item_df是以前出iter的最後項目（dividends）為基準，此法未必精準
    #     logging.info("[AdjTrans]共{n}檔標的調整後數據將入庫(轉換比率{ratio}%)" \
    #                     .format(n=len(complete_ticker_list), ratio=round(100*len(complete_ticker_list) / len(item_df.columns),2)))
    #     RAY3000_intersect = list(set(complete_ticker_list).intersection(RAY3000_ticker_list))
    #     logging.info("[AdjTrans][US_RAY3000]成分股覆蓋率{ratio}%\n".format(ratio = round(100*len(RAY3000_intersect) / len(RAY3000_ticker_list),2)))
    #     # logging.info("[AdjTrans][US_RAY3000]缺失標的如下:")
    #     # print(lost_ticker_list)
        
    #     for item in item_list:
    #         item_folderPath = os.path.join(folderPath, "adj_"+item)
    #         if os.path.exists(item_folderPath):
    #             shutil.rmtree(item_folderPath) 
    #         make_folder(item_folderPath)

    #         adjitem_df = adjusted_item_df_dict[item].loc[:, complete_ticker_list]
    #         for i in range(len(adjitem_df)):
    #             data_series = adjitem_df.iloc[i, :]
    #             date = data_series.name
    #             filePath = os.path.join(item_folderPath, date+".csv")
    #             data_series.to_csv(filePath)
import os, sys, logging, time
import pandas as pd
import numpy as np
import pickle, json, string
import shutil, random
from threading import Thread, Lock
from datetime import datetime, date, timedelta
from yahoo_fin.stock_info import *
from fredapi import Fred

import yfinance as yf
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
    - 資料庫中股號間的特殊字符一律以 "_" 作為連接符，如：BRK_B, TW_0050, ...
    - 編寫原則，非特定資產類別專用的函數，以data_stack作為參數，特定資產類別專用函數（如stock），以國別作為參數
    '''
    def __init__(self, database_folder_path):
        # 最上層之資料夾
        self.database_folder_path = database_folder_path
        self.data_path_folder_path = os.path.join(database_folder_path, "data_path")
        self.raw_data_folder_path = os.path.join(database_folder_path, "raw_data")
        self.raw_table_folder_path = os.path.join(database_folder_path, "raw_table")
        self.table_folder_path = os.path.join(database_folder_path, "table")
        self.cache_folder_path = os.path.join(database_folder_path, "cache")
        self.data_stack_list = ["US_stock"]
        self.data_path_dict = self._load_and_build_data_path()
        
        # 待改：API應另行統一管理
        self.polygon_API_key = "vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj" # 新的polygon

    # 依照資料屬性取得其item名稱
    def _get_item_list_by_layer(self, data_stack, criteria_dict=dict()):
        file_path = os.path.join(self.data_path_folder_path, data_stack+".xlsx")
        path_df = pd.read_excel(file_path, index_col=0)
        # path_df = path_df[path_df["type"]!= "folder"]
        # path_df = path_df[path_df["type"]!= "block"]
        path_df = path_df[path_df["type"]== "series"]
        for key, value in criteria_dict.items():
            path_df = path_df[path_df[key]==value]
        return list(path_df.index)
    
    # 依照data_path載入資料路徑，並建立對應的資料夾層級結構
    def _load_and_build_data_path(self):
        # 已註冊之資料集
        data_path_dict = {key: dict() for key in self.data_stack_list}
        # 依照資料集的path檔，建構data_path_dict
        for data_stack in self.data_stack_list:
            file_path = os.path.join(self.data_path_folder_path, data_stack+".xlsx")
            path_df = pd.read_excel(file_path, index_col=0).drop("type", axis=1)

            for i in range(len(path_df)):
                path_series = path_df.iloc[i,:]
                item, path_list = path_series.name, list(path_series.dropna())
                item_folder_path = os.path.join(*path_list)
                data_path_dict[data_stack][item] = item_folder_path    

        # 依照data_path_dict，建構資料夾（每級data level皆有相同結構）
        data_level_list = ["raw_data", "raw_table", "table"]
        for data_level in data_level_list:
            for data_stack in self.data_stack_list:
                sub_path_dict = data_path_dict[data_stack]
                for sub_path in list(sub_path_dict.values()):
                    folder_path = os.path.join(self.database_folder_path, data_level, data_stack, sub_path)
                    make_folder(folder_path)

        # cache應該也要納入統一管理，待改
        make_folder(self.cache_folder_path)
        return data_path_dict
    
    # ——————————————————————————————————————————————————————————
    # 資料儲存狀態相關函數（開始）

    # 指定data_stack、item、data_level，返回對應的資料路徑
    def _get_data_path(self, data_stack, item, data_level="raw_data"):
        item_folder_path = self.data_path_dict[data_stack][item]
        if data_level == "raw_data":
            return os.path.join(self.raw_data_folder_path, data_stack, item_folder_path)
        elif data_level == "raw_table":
            return os.path.join(self.raw_table_folder_path, data_stack, item_folder_path)
        elif data_level == "table":
            return os.path.join(self.table_folder_path, data_stack, item_folder_path)

    # 取得整個data_stack中每個item的完整資料夾地址（通常用於傳遞給外部爬蟲函數）
    # 待改：應該透過_get_data_path呼喚
    def _get_data_path_dict(self, data_stack, data_level="raw_data"):
        data_path_dict = dict()
        for item, data_path in self.data_path_dict[data_stack].items():
            if data_level == "raw_data":
                data_path = os.path.join(self.raw_data_folder_path, data_stack, data_path)
            data_path_dict[item] = data_path
        return data_path_dict

    ## 取得單一資料項目的儲存狀況：起始日期/最新更新日期/資料筆數，回傳dict
    def get_single_data_status(self, data_stack, item, data_level="raw_data"):
        item_folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
        status_dict = dict()
        # 若該項目不存在raw_data，則返回空值（項目資料夾不存在 & 雖存在但其中沒有檔案，皆視為raw_data不存在）
        if not os.path.exists(item_folder_path) or len(os.listdir(item_folder_path))==0:
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = None, None, 0
        
        #待改：部分資料非時序型資料，如trade_date，待處理
        else:
            # 取得指定項目資料夾中的檔案名
            raw_date_list = os.listdir(item_folder_path)
            # 去除後綴檔名（如.csv）
            date_list = [date.split(".")[0] for date in raw_date_list]
            # 去除異常空值（由隱藏檔所導致）
            date_list = [date for date in date_list if len(date)!=0]
            # 建立date series後排序，以取得資料的起始、結束日
            date_series = pd.Series(date_list)
            date_series = date_series.apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
            start_date, end_date = date_series.iloc[0].strftime("%Y-%m-%d"), date_series.iloc[-1].strftime("%Y-%m-%d")
            date_num = len(date_series)
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = start_date, end_date, date_num

        return status_dict

    ## 取得多項資料儲存狀況：起始日期/最新更新日期/資料筆數，回傳Dataframe
    def get_data_status(self, data_stack, item_list, data_level="raw_data"):
        status_dict = dict()
        for item in item_list:
            sub_status_dict = self.get_single_data_status(data_stack=data_stack, item=item, data_level="raw_data")
            status_dict[item] = sub_status_dict
        return pd.DataFrame(status_dict).T

    # 給定結束日與資料筆數，回傳對應的起始日（因部分資料取用需求須給定資料筆數，須
    def _get_start_date_by_num(self, item, data_stack, end_date, num):
        item_folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_data")
        raw_date_list = os.listdir(item_folder_path)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（由隱藏檔所導致）
        date_list = [date for date in date_list if len(date)!=0]
        date_series = pd.Series(date_list)
        date_series = date_series.apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        date_series = date_series[date_series <= end_date]
        # 依照結束日截取N筆資料，回傳起始日
        start_date = datetime2str(list(date_series.iloc[-num:,])[0])
        return start_date
    
    # 資料儲存狀態相關函數（結束）
    # ——————————————————————————————————————————————————————————
    # trade_date處理相關函數（開始）
    
    # 取得交易日日期序列（字串列表），國家預設為US，若不指定時間區間預設為全部取出
    def get_stock_trade_date_list(self, start_date=None, end_date=None, country="US"):
        # 若未給定起始/結束日期則回傳所有交易日(2000-01-01起)
        market_status_series = self.get_stock_market_status_series(start_date=start_date, end_date=end_date, country=country) 
        # 1: 交易日, 0:六日休市, -1:非六日休市
        trade_date_series = market_status_series[market_status_series==True]
        trade_date_list = list(map(datetime2str, trade_date_series.index))
        return trade_date_list
    
    # Note：須定時補新的market status，目前最新資料至2024-07-06
    # Index為日期，Value:1: 交易日, 0:六日休市, -1:非六日休市
    def get_stock_market_status_series(self, start_date=None, end_date=None, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        folder_path = self._get_data_path(data_stack=data_stack, item="trade_date", data_level="raw_data")
        file_path = os.path.join(folder_path, "trade_date.csv")
        market_status_series = pd.read_csv(file_path, index_col=0).squeeze()
        # 原csv檔案的日期的日期為字串，格式為2000/01/01，須轉換為datetime
        market_status_series.index = pd.DatetimeIndex(market_status_series.index)

        if start_date != None and end_date != None:
            mask = (market_status_series.index >= start_date) & (market_status_series.index <= end_date)
            return market_status_series[mask]

        return market_status_series

    # 取得距離給定日最近的實際交易日（last：上一個，next：下一個），cal_self可選擇給定的日期本身是否算作交易日
    def get_closest_trade_date(self, date, country="US", direction="last", cal_self=True):
        date = str2datetime(date)
        market_status_series = self.get_stock_market_status_series(country=country)
        
        if cal_self == True:
            market_status = market_status_series[date]
        else:
            market_status = 0
        
        if direction == "last":
            step = -1
        elif direction == "next":
            step = 1
        
        while True:
            if market_status != 1:
                date = date + timedelta(days=step)
                market_status = market_status_series[date]
            else:
                return datetime2str(date)

    # trade_date處理相關函數（結束）
    # ——————————————————————————————————————————————————————————
    
    # ——————————————————————————————————————————————————————————
    # Universe_ticker處理相關函數（開始）
    
    ## 儲存當前正在交易的所有ticker，預設資料源：polygon
    def save_stock_universe_list(self, universe_name="univ_us_stock", start_date=None, end_date=None, source="polygon", country="US"):        
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            start_date = self.get_single_data_status(data_stack=data_stack, item=universe_name)["end_date"]

        if end_date == None:
            end_date = datetime2str(datetime.today())

        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        if source=="polygon":
            data_dict = save_stock_universe_list_from_polygon(API_key=self.polygon_API_key, universe_name=universe_name, date_list=trade_date_list)
        
        folder_path = self._get_data_path(data_stack=data_stack, item=universe_name, data_level="raw_data")
        for date in data_dict.keys():
            file_path = os.path.join(folder_path, date+".csv")
            # data_dict：{date: ticker_series}
            data_dict[date].to_csv(file_path)

        logging.info("[SAVE][{universe_name}]成分股資料[{start_date}~{end_date}]儲存完成".format(universe_name=universe_name, start_date=start_date, end_date=end_date))        

    ## 取得指定日期下，特定universe的成分股list（底層函數為self.get_stock_ticker_df）
    def get_stock_ticker_list(self, universe_name, date=None, exclude_delist=False, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
        
        # 若未給定日期，則預設取出最新一筆資料
        if date == None:
            date = self.get_single_data_status(data_stack=data_stack, item=universe_name)["end_date"]

        ticker_series = self.get_stock_ticker_df(universe_name=universe_name, start_date=date, end_date=date, 
                                                  exclude_delist=exclude_delist, country=country).squeeze()
        return ticker_series.index.to_list()

    ## 取得特定universe的df資料，row:日期，columns:曾經存在此universe的所有ticker，value:True/False
    def get_stock_ticker_df(self, universe_name, start_date, end_date, exclude_delist=False, country="US"):
        # 待改：應透過下市股票對照表處理
        def remove_delist_ticker(ticker_list):
            # BBG對下市ticker會更改為7位數字+一個字母（D或Q），如2078185D，可以此判別
            ticker_list = [ticker for ticker in ticker_list if len(ticker)<8]
            return ticker_list

        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
        
        raw_ticker_df = self._get_item_data_df_by_date(item=universe_name, data_stack=data_stack, 
                                                      start_date=start_date, end_date=end_date)

        # raw_data中每日儲存的為universe中的ticker序列，故須取出時間區段中所有存在過的ticker
        all_universe_ticker_series = pd.Series(raw_ticker_df.values.flatten()).drop_duplicates().dropna()
        date_list = raw_ticker_df.index.to_list()
        # row：時間序列，column：完整的ticker序列，value：True/False
        ticker_df = pd.DataFrame(index=date_list, columns=all_universe_ticker_series)
        
        # 參照raw_data，若ticker在當日被包含於universe中，便填寫True
        for date in date_list:
            ticker_series = raw_ticker_df.loc[date, :].dropna()
            ticker_df.loc[date, ticker_series] = True
        # 空值則補Fasle，代表該ticker當日未被包含於universe中
        ticker_df = ticker_df.fillna(False)
        # 可選擇是否去除已下市股票
        if exclude_delist == True:
            ticker_list = list(ticker_df.columns)
            ticker_list = remove_delist_ticker(ticker_list)
            return ticker_df.loc[:, ticker_list]
        
        else:
            return ticker_df

    # Universe_ticker處理相關函數（結束）
    # ——————————————————————————————————————————————————————————
    
    # ——————————————————————————————————————————————————————————
    # 各類股票資料項目處理相關函數（開始）
    
    # 取得raw_table/table資料（取出前須先由raw_data轉換為table）
    def _get_item_table_df(self, item, data_stack="US_stock", data_level="raw_table"):
        folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
        file_path = os.path.join(folder_path, item+".pkl")
        df = pd.read_pickle(file_path)            
        return df
    
    ## 將時序資料取出組合為DataFrame
    def _get_item_data_df_by_date(self, item, data_stack="US_stock", target_stock_ticker_list=None, start_date=None, end_date=None, pre_fetch_nums=0):
        item_folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_data")
        # 待改：應該直接使用dateime?
        if type(start_date) is not str:
            start_date = datetime2str(start_date)
        if type(end_date) is not str:
            end_date = datetime2str(end_date)

        #item_folder_path = os.path.join(folder_path, item)
        # 取得指定資料夾中所有檔案名(以時間戳記命名）
        raw_date_list = os.listdir(item_folder_path)
        # 去除檔案名中的後綴名（.csv)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（可能由.DS_store等隱藏檔所導致）
        date_list = [date for date in date_list if len(date) != 0]
        # 將時間戳記組合成date series，以篩選出指定的資料區間（同時進行排序與重設index）
        date_series = pd.Series(date_list).apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        # 向前額外取N日資料，以使策略起始時便有前期資料可供計算
        start_date = shift_days_by_strDate(start_date, -pre_fetch_nums)
        mask = (date_series >= start_date) & (date_series <= end_date) 
        # 將date series轉為將date list（字串列表），以用於後續讀取資料
        date_list = list(map(lambda x:datetime2str(x), date_series[mask]))
        # 依照date list逐日取出每日資料，並組合為DataFrame
        df_list = list()
        for date in date_list:
            fileName = os.path.join(item_folder_path, date+".csv")
            df = pd.read_csv(fileName, index_col=0)
            df_list.append(df)

        if len(df_list) == 0:
            logging.warning("[{data_stack}][{item}][{start_date}-{end_date}]區間不存在資料".format(data_stack=data_stack, item=item,
                                                                                       start_date=start_date, end_date=end_date))
            return pd.DataFrame()
        
        df = pd.concat(df_list, axis=1).T
        # 對Index以日期序列賦值並排序
        df.index = pd.to_datetime(date_list)
        df = df.sort_index()

        # 若不指定ticker，則預設為全部取出
        if target_stock_ticker_list == None:
            return df
        # 若指定ticker，比對取出的資料所包含的ticker與目標ticker是否存在差距
        else:
            lost_ticker_list = list(set(target_stock_ticker_list) - set(df.columns))
            # 篩選出目標ticker與資料庫ticker的交集，以避免loc報錯
            re_target_stock_ticker_list = list(set(target_stock_ticker_list) - set(lost_ticker_list))
            if len(lost_ticker_list) > 0:
                logging.warning("資料項目:{}共{}檔標的缺失，缺失標的如下:".format(item, len(lost_ticker_list)))
                logging.warning(lost_ticker_list)

            return df.loc[:, re_target_stock_ticker_list]

    ## 儲存股票分割資料raw_data
    def save_stock_split_data(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"

        elif country == "TW":
            data_stack = "TW_stack"

        if start_date == None:
            # 若未給定起始日，則自最新一筆資料的隔日開始抓取
            start_date = self.get_single_data_status(data_stack=data_stack, item="stock_splits")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            # 若未給定結束日，則預設會抓取至明日；因下單須預知隔日分割情況以計算下單參考價
            end_date = datetime2str(datetime.today() + timedelta(days=1))

        folder_path = self._get_data_path(data_stack=data_stack, item="stock_splits", data_level="raw_data")
        # 取得交易日序列，並以此索引資料
        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        if source == "polygon":
            data_dict = save_stock_split_from_Polygon(API_key=self.polygon_API_key, date_list=trade_date_list)
        
        # 儲存檔案
        for date in data_dict.keys():
            file_path = os.path.join(folder_path, date+".csv")
            data_dict[date].to_csv(file_path)
    
    ## 取得調整因子df，預設為backward方法（使當前adjclose等同於close）
    ## 股價 * 調整因子 = 調整價
    def _get_stock_adjust_factor_df(self, start_date=None, end_date=None, country="US", method="backward"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date, country=country)
        stock_splits_df = self._get_item_data_df_by_date(item="stock_splits", data_stack=data_stack, start_date=start_date, end_date=end_date)
        adjust_factor_df = cal_adjust_factor_df(stock_splits_df, date_list=trade_date_list, method=method)
        return adjust_factor_df

    # 儲存股票價量資料raw_data
    def save_stock_priceVolume_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, adjust=False, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"

        elif country == "TW":
            data_stack = "TW_stock"

        # 若未指定起始/結束日期，則自open最新儲存資料日的下一日開始抓取
        if start_date == None:
            start_date = self.get_single_data_status(data_stack=data_stack, item="open")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        folder_path = self._get_data_path(data_stack=data_stack, item="priceVolume", data_level="raw_data")
        
        if source == "polygon":
            save_stock_priceVolume_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, adjust)
        
        elif source == "yfinance":
            cache_folder_path = os.path.join(self.cache_folder_path, "yfinance_priceVolume")
            #待改：cache folder須統一管理
            make_folder(cache_folder_path)
            save_stock_priceVolume_from_yfinance(folder_path, cache_folder_path, start_date, end_date, ticker_list, country, adjust)

        elif source == "yahoo_fin":
            pass
    
    # 計算並儲存open_to_open以及close_to_close的raw_data
    def save_stock_daily_return(self, start_date=None, end_date=None, method="c2c", cal_dividend=True, country="US"):        
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if method == "c2c":
            item_name, price_item = "c2c_ret", "close"

        elif method == "o2o":
            item_name, price_item = "o2o_ret", "open"

        folder_path = self._get_data_path(data_stack=data_stack, item=item_name, data_level="raw_data")
        
        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            # 不遞延一天，係因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
            start_date = self.get_single_data_status(data_stack=data_stack, item=item_name)["end_date"]

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if start_date == end_date:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_date))

            return 0

        price_df = self._get_item_data_df_by_date(item=price_item, data_stack=data_stack, start_date=start_date, end_date=end_date)
        adjust_factor_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
        
        # 只針對有分割資料的個股作股價調整
        adjust_ticker_list = price_df.columns.intersection(adjust_factor_df.columns)        
        adjusted_item_df = price_df[adjust_ticker_list] * adjust_factor_df
        # 若不給定ticker_list，會導致賦值時，columns沒有對齊
        price_df[adjust_ticker_list] = adjusted_item_df[adjust_ticker_list]
        # 將股價加上當日除息的現金股利
        dividends_df = self._get_item_data_df_by_date(item="ex_dividends", data_stack=data_stack, start_date=start_date, end_date=end_date)
        dividends_df = dividends_df.fillna(0)
        dividends_ticker_list = price_df.columns.intersection(dividends_df.columns)
        adjusted_dividends_df = price_df[dividends_ticker_list] + dividends_df[dividends_ticker_list]
        price_df[dividends_ticker_list] = adjusted_dividends_df[dividends_ticker_list]
                
        logging.info("[daily return][{method}] 資料計算中".format(method=method))
        # 去除第一row：因取pct後為空值
        return_df = price_df.pct_change().iloc[1:,:]

        # 切分return
        for i in range(len(return_df)):
            data_series = return_df.iloc[i,:].dropna()
            #去除index為NaN的情況（可能源自price資料異常）
            data_series = data_series[data_series.index.notna()]
            data_series = data_series.sort_index()
            fileName = datetime2str(data_series.name)
            file_path = os.path.join(folder_path, fileName+".csv")
            data_series.to_csv(file_path)
    
    # 儲存現金股利raw_data
    def save_stock_cash_dividend(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if start_date == None:
            # 待改：一律使用ex_divi好像不太好（？
            start_date = self.get_single_data_status(data_stack=data_stack, item="ex_dividends")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today()+timedelta(days=1))
        
        # 待改，資料源管理
        folder_path = self._get_data_path(data_stack=data_stack, item="ex_dividends", data_level="raw_data")
        save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="ex_dividend_date")
        
        folder_path = self._get_data_path(data_stack=data_stack, item="pay_dividends", data_level="raw_data")
        save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="pay_date")
        # 因同一天公司可能會宣布N筆股利（不同發放日），較難儲存故先略過
        #folder_path = self._get_data_path(data_stack="US_stock", item="declaration_dividends", data_level="raw_data")
        #save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="declaration_date")
        
    # 儲存流通股數raw_data
    def save_stock_shares_outstanding(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if start_date == None:
            start_date = self.get_single_data_status(data_stack=data_stack, item="shares_outstanding")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())
        # 因polygon須依據ticker逐一抓取股數，若每日更新較為費時，故僅在每月最後一日重新抓取
        # 其他日則依據前一日股數，參考split調整
        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date, country=country)
        folder_path = self._get_data_path(data_stack=data_stack, item="shares_outstanding", data_level="raw_data")
        
        for date in trade_date_list:
            last_trade_date = self.get_closest_trade_date(date, direction="last", cal_self=False)
            next_trade_date = self.get_closest_trade_date(date, direction="next", cal_self=False)
            last_date_shares_series = self._get_item_data_df_by_date(item="shares_outstanding", data_stack="US_stock", start_date=last_trade_date, end_date=last_trade_date).squeeze()
            stock_splits_df = self._get_item_data_df_by_date(item="stock_splits", data_stack="US_stock", start_date=date, end_date=date)
                    
            if len(stock_splits_df.columns) == 0:
                if_splits_exist = False
            else:
                if_splits_exist = True
                #須設定axis為0，否則當日僅有一檔標的split時，ticker會消失，降為成純量
                stock_splits_series = stock_splits_df.squeeze(axis=0)
            
            # 月底最後一交易日，重新抓取
            if str2datetime(date).month != str2datetime(next_trade_date).month:
                logging.info("[NOTE][本日是本月最後一交易日，須重新更新股數計算基礎]")
                # 取得最新一筆美股所有上市公司清單
                last_univ_us_stock_update_date = self._get_start_date_by_num(item="univ_us_stock", data_stack="US_stock", end_date=date, num=1)
                univ_us_ticker_list = list(self._get_item_data_df_by_date(item="univ_us_stock", data_stack="US_stock", start_date=last_univ_us_stock_update_date, end_date=last_univ_us_stock_update_date).squeeze())
                # 因自polygon下載的股數，通常會延遲1~2日反應分割調整
                # 直接採用當日數據會導致出錯，故若重新下載日前2日曾進行分割，則該股不重新下載
                recent_splits_start_date = self._get_start_date_by_num(item="stock_splits", data_stack="US_stock", end_date=date, num=2)
                recent_splits_ticker_list = list(self._get_item_data_df_by_date(item="stock_splits", data_stack="US_stock", start_date=recent_splits_start_date, end_date=date).columns)
                ticker_list = sorted(list(set(univ_us_ticker_list) - set(recent_splits_ticker_list)))                
                # ticker_list = ticker_list[:3]
                result_dict = save_stock_shares_outstanding_from_Polygon(self.polygon_API_key, ticker_list=ticker_list, start_date=date, end_date=date)
                shares_series = result_dict[date]
                # 確認缺漏的標的：近日有作分割 + polygon資料源缺漏
                lost_ticker_set = set(univ_us_ticker_list) - set(list(shares_series.index))
                lost_ticker_list = list(lost_ticker_set.intersection(last_date_shares_series.index))
                # 針對缺漏的標的，改為依據前一日股數，參考split調整（即依照一般日算法）
                lost_ticker_last_date_shares_series = last_date_shares_series[lost_ticker_list]
                if if_splits_exist == False:
                    lost_ticker_shares_series = lost_ticker_last_date_shares_series
                else:
                    lost_ticker_shares_series = lost_ticker_last_date_shares_series.mul(stock_splits_series, fill_value=1).dropna()
                    lost_ticker_shares_series = lost_ticker_shares_series[lost_ticker_last_date_shares_series.index]
                    
                # 將缺漏標的資料，併入polygon重新下載而得的資料
                shares_series = pd.concat([shares_series, lost_ticker_shares_series])
                logging.info("[SAVE][{date}][流通股數計算完成（重新下載更新）]".format(date=date))

            # 非月底最後一交易日，依據前一日股數，參考split調整
            else:
                if if_splits_exist == False:
                    shares_series = last_date_shares_series
                else:
                    shares_series = last_date_shares_series.mul(stock_splits_series, fill_value=1).dropna()
                    # 若不重取index，在stock_split存在，但last_date_shares不存在的ticker會導致錯誤
                    # 因為fill value=1，該標的的股數會 = 1 * 分割數，故須剔除
                    shares_series = shares_series[last_date_shares_series.index]
                    
                logging.info("[SAVE][{date}][流通股數計算完成（依據前日基礎計算）]".format(date=date))

            file_name = os.path.join(folder_path, date+".csv")
            
            common_ticker_list = list(set(shares_series.index).intersection(last_date_shares_series.index))
            share_change_series = shares_series[common_ticker_list] / last_date_shares_series[common_ticker_list]
            share_change_series = share_change_series[share_change_series!=1]

            if len(share_change_series) > 0:
                logging.info("[NOTE][{date}][本日流通股數變化如下：本日股數/前日股數]".format(date=date))
                logging.info(dict(share_change_series))
            
            new_ticker_list = list(set(shares_series.index) - set(common_ticker_list))
            if len(new_ticker_list) > 0:
                logging.info("[NOTE][{date}][本日新增標的如下]".format(date=date))
                logging.info(new_ticker_list)
            
            shares_series.name = date
            shares_series.to_csv(file_name)
    
    def get_stock_financialReport_df(self, item, start_date, end_date, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        #須往前多取資料，以便填補值
        pre_fetch_date = self._get_start_date_by_num(item=item, data_stack=data_stack, end_date=start_date, num=90)
        raw_financialReport_df = self._get_item_data_df_by_date(item=item, data_stack=data_stack, start_date=pre_fetch_date, end_date=end_date)
        trade_date_list = self.get_stock_trade_date_list(start_date=pre_fetch_date, end_date=end_date)
        financialReport_df = pd.DataFrame(index=trade_date_list, columns=sorted(raw_financialReport_df.columns))
        financialReport_df.update(raw_financialReport_df)
        financialReport_df = financialReport_df.ffill()

        mask = (financialReport_df.index >= start_date) & (financialReport_df.index <= end_date)
        return financialReport_df[mask]

    def get_stock_item_df_dict(self, item_list, start_date=None, end_date=datetime2str(datetime.today()), 
                                    num=None, method="by_date", country="US", if_align=False, if_save=False):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
                    
        item_df_list = list()
        for item in item_list:
            logging.info("[GET][{item}][正在組合資料][資料區間{start_date}-{end_date}]".format(item=item, start_date=start_date, end_date=end_date))
            if method=="by_num":
                start_date = self._get_start_date_by_num(item=item, data_stack=data_stack, end_date=end_date, num=num)
            
            # 部分資料組合為塊狀資料後，尚須額外處理，如adjust_factor, universe_ticker...等
            if item in ["univ_ray3000", "univ_ndx100", "univ_dow30", "univ_spx500"]:
                item_df = self.get_stock_ticker_df(universe_name=item, start_date=start_date, end_date=end_date, exclude_delist=False)
            
            # raw data中stock_splits為該日有分割才有資料，在轉化為raw table時進行補日期 & 計算調整係數
            elif item == "stock_splits":
                item_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
            
            elif item == "trade_date":
                #將series轉為df
                item_df = self.get_stock_market_status_series(start_date=start_date, end_date=end_date, country=country).to_frame()
            
            # 待改：如何對基本面資料列表？
            else:
                item_df = self._get_item_data_df_by_date(item=item, data_stack=data_stack, start_date=start_date, end_date=end_date)

            # 部分column（ticker）可能為Nan
            if np.nan in item_df.columns:
                item_df = item_df.drop(np.nan, axis=1)
            
            item_df_list.append(item_df)

            if if_save == True:
                folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_table")            
                file_path = os.path.join(folder_path, item+".pkl")
                item_df.to_pickle(file_path)
                logging.info("[SAVE][{item}][資料Raw Table儲存完畢][資料區間{start_date}-{end_date}]".format(item=item, start_date=start_date, end_date=end_date))

        if if_align == True:
            item_df_list = get_aligned_df_list(item_df_list)

        return dict(zip(item_list, item_df_list))

    # 待完成的函數（開始）

    # 資料品質控管（Raw_Table -> Table）
    def _get_filtered_raw_table_item_df(self, item, data_stack):
        raw_table_folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_table")
        raw_table_file_path = os.path.join(raw_table_folder_path, item+".pkl")
        item_df = pd.read_pickle(raw_table_file_path)
        OHLC_list = ["open", "high", "low", "close"]
        # OHLC檢查項目: 中段空值檢查、上下市日期核對、分割資料核對
        if item in OHLC_list:
            adjust_factor_df = self._get_item_table_df(item="stock_splits", data_stack="US_stock", data_level="raw_table")
            interval_nan_ratio = cal_interval_nan_value(item_df)
            max_abs_zscore_series = cal_return_max_abs_zscore(item_df.copy(), adjust_factor_df)

            delete_ticker_list_1 = list(interval_nan_ratio[interval_nan_ratio > 0.1].index)
            delete_ticker_list_2 = list(max_abs_zscore_series[max_abs_zscore_series > 5].index)
            delete_ticker_list_all = list(set(delete_ticker_list_1) | set(delete_ticker_list_2))
            item_df = item_df.drop(delete_ticker_list_all, axis=1)

        elif item == "dividends":
            price_df = self._get_item_table_df(item="close", data_stack="US_stock", data_level="raw_table")
            dividends_ratio_df = item_df / price_df
            max_dividends_ratio_series = dividends_ratio_df.max()
            max_dividends_ratio_series = max_dividends_ratio_series[max_dividends_ratio_series > 0.8]
            delete_ticker_list_all = list(max_dividends_ratio_series.index)
            item_df = item_df.drop(delete_ticker_list_all, axis=1)

        elif item == "volume":
            pass

        return item_df

    def trans_stock_item_raw_table_to_table(self, item_list, country="US"):
        if country == "US":
            data_stack =  "US_stock"
        elif country == "TW":
            data_stack =  "TW_stock"

        for item in item_list:
            item_df = self._get_filtered_raw_table_item_df(item, data_stack=data_stack)
            table_folder_path = self._get_data_path(data_stack=data_stack, item=item, data_level="table")
            table_file_path = os.path.join(table_folder_path, item+".pkl")
            item_df.to_pickle(table_file_path)
    
    # 確認標的成分股變化（預設為univ_us_stock）
    # compare_date_num：和幾天前的資料比，預設為1
    def _check_component_change(self, date=datetime2str(datetime.today()), universe_name="univ_us_stock", country="US", compare_date_num=1):
        if country == "US":
            data_stack =  "US_stock"
        elif country == "TW":
            data_stack =  "TW_stock"
        
        last_update_date = self.get_single_data_status(data_stack=data_stack, item=universe_name)["end_date"]
        if date > last_update_date:
            logging.warning("[CHECK]成分股資料未更新，比對結果可能出錯")
        
        # 取得universe資料中，指定日期（date）的前2筆資料日期
        benchmark_date = self._get_start_date_by_num(item=universe_name, data_stack=data_stack, end_date=date, num=compare_date_num+1)
        ticker_df = self.get_stock_ticker_df(universe_name=universe_name, start_date=benchmark_date, end_date=date, exclude_delist=False, country=country)
        
        #因ticker_df為T/F形式，錯位相減後為0代表情況不變，為1代表新上市（前日為0，此日為1），為-1代表新下市（前日為-1，此日為0）
        change_signal_series = (ticker_df - ticker_df.shift(compare_date_num)).iloc[-1, :]
        new_component_list = list(change_signal_series[change_signal_series==1].index)
        deleted_component_list = list(change_signal_series[change_signal_series==-1].index)
        return new_component_list, deleted_component_list
    
    # 待新增的資料項目：基本面資料、總經資料
    def save_stock_financialReport_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if start_date == None:
            start_date = str2datetime(self.get_single_data_status(data_stack=data_stack, item="filing_date")["end_date"])
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        folder_path_dict = self.data_path_dict
        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        if source == "polygon":
            data_dict = save_stock_financialReport_data_from_Polygon(API_key=self.polygon_API_key, date_list=trade_date_list)
        
        for date in data_dict.keys():
            data_df = data_dict[date]
            # 儲存發布日資料
            filing_date_folder_path = self._get_data_path(data_stack=data_stack, item="filing_date", data_level="raw_data")
            filing_date_file_path = os.path.join(filing_date_folder_path, date+".csv")
            filing_date = pd.Series(data_df.columns).rename("filing_date")
            filing_date.to_csv(filing_date_file_path)
        
            # 儲存財報資料
            for i in range(len(data_df)):
                data_series = data_df.iloc[i, :]
                item_name = data_series.name
                # 部分科目過於冷門，若不在2023/07/10已註冊的70多個科目當中，便略過
                try:
                    folder_path = self._get_data_path(data_stack=data_stack, item=item_name, data_level="raw_data")
                    file_path = os.path.join(folder_path, date+".csv")
                    data_series.to_csv(file_path)
                except Exception as e:
                    print(e)
                    continue
    
    def save_stock_company_info(self, ticker_list=None, source="polygon", country="US"):
        folder_path = self._get_data_path(data_stack="US_stock", item="company_info")
        file_path = os.path.join(folder_path, "company_info.csv")
        # 取得自檔案上次更新以來，新增的ticker列表
        if ticker_list == None:
            old_company_info_df = pd.read_csv(file_path, index_col=0)
            old_ticker_list = list(old_company_info_df["ticker"])
            new_ticker_list = self.get_stock_ticker_list("univ_us_stock", date=TODAY_DATE_STR, exclude_delist=False, country="US")
            add_ticker_list = sorted(list(set(new_ticker_list) - set(old_ticker_list)))
            ticker_list = add_ticker_list.copy()

        # 取得新增標的的公司資料df
        if source == "polygon":
            new_company_info_df = save_stock_company_info_from_Polygon(API_key=self.polygon_API_key, ticker_list=ticker_list) 
        
        # 與原公司資料df結合，一併儲存
        old_company_info_df = pd.read_csv(file_path, index_col=0)
        
        if len(ticker_list)>0 and len(new_company_info_df)>0:
            company_info_df = pd.concat([old_company_info_df, new_company_info_df], ignore_index=True)
            logging.info("[SAVE] 更新{N}檔標的之公司資訊，標的如下：".format(N=len(ticker_list)))
            logging.info(ticker_list)
            company_info_df.to_csv(file_path)

    # 為表狀資料
    def save_stock_delisted_info(self, source="polygon", country="US"):
        if source == "polygon":
            folder_path = self._get_data_path(data_stack="US_stock", item="delisted_stock_info")
            save_stock_delisted_info_from_polygon(folder_path=folder_path, API_key=self.polygon_API_key, universe_type="CS")
            
            folder_path = self._get_data_path(data_stack="US_stock", item="delisted_etf_info")
            save_stock_delisted_info_from_polygon(folder_path=folder_path, API_key=self.polygon_API_key, universe_type="ETF")

    # 取得FRED官網公布的總經數據，name的可用選項，見Macro/Token_trans/fred.txt
    # def get_fred_data(self, name):
    #     folder_path = self.raw_data_macro_folder_path
    #     file_path = os.path.join(folder_path, name+".pkl")
    #     data_df = pd.read_pickle(file_path)  
    #     return data_df

    # def save_fred_data(self, name):
    #     folder_path = self.raw_data_macro_folder_path
    #     token_trans_folderName = os.path.join(folder_path, "Token_trans")
        
    #     api_key = token_trans("API_Key", source="fred", folder_path=token_trans_folderName)
    #     data_key = token_trans(name, source="fred", folder_path=token_trans_folderName)
        
    #     fred = Fred(api_key=api_key)
        
    #     try:
    #         data_df = fred.get_series(data_key)
    #         print(name, " has been saved.")
        
    #     except Exception as e:
    #         print(e)
    #         print(name, " doesn't exist in token trans table.")

    #     file_position = os.path.join(folder_path, name+".pkl")
    #     data_df.to_pickle(file_position)
    #     return data_df
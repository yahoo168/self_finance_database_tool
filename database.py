import os, sys, logging, time
import pandas as pd
import numpy as np
import time
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
    def __init__(self, database_folderPath):
        # 最上層之資料夾
        self.raw_data_folderPath = os.path.join(database_folderPath, "raw_data")
        self.raw_table_folderPath = os.path.join(database_folderPath, "raw_table")
        self.table_folderPath = os.path.join(database_folderPath, "table")
        self.cache_folderPath = os.path.join(database_folderPath, "cache")
        
        self.data_path_dict, self.data_path_df_dict = self._load_and_build_data_path(database_folderPath)
        
        # 待改：API應另行統一管理
        # self.polygon_API_key = "VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH" # 舊的polygon
        self.polygon_API_key = "vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj" # 新的polygon
    
    def _filter_item_by_layer(self, data_stack, criteria_dict):
        data_path_df = self.data_path_df_dict[data_stack]
        for key, value in criteria_dict.items():
            data_path_df = data_path_df[data_path_df[key]==value]
        return list(data_path_df.index)

    def _load_and_build_data_path(self, database_folderPath):
        # 已註冊之資料集
        data_stack_list = ["US_stock"]
        data_path_dict = {key: dict() for key in data_stack_list}
        data_path_df_dict = {key: dict() for key in data_stack_list}

        # 依照資料集的path檔，建構data_path_dict
        for data_stack in data_stack_list:
            filePath = os.path.join(database_folderPath, "data_path", data_stack+".xlsx")
            path_df = pd.read_excel(filePath, index_col=0).drop("type", axis=1)
            data_path_df_dict[data_stack] = path_df
            for i in range(len(path_df)):
                path_series = path_df.iloc[i,:]
                item, path_list = path_series.name, list(path_series.dropna())
                item_folderPath = os.path.join(*path_list)
                data_path_dict[data_stack][item] = item_folderPath    

        # 依照data_path_dict，建構資料夾（每級data level皆有相同結構）
        data_level_list = ["raw_data", "raw_table", "table"]
        for data_level in data_level_list:
            for data_stack in data_stack_list:
                sub_path_dict = data_path_dict[data_stack]
                for sub_path in list(sub_path_dict.values()):
                    folderPath = os.path.join(database_folderPath, data_level, data_stack, sub_path)
                    make_folder(folderPath)

        # cache應該也要納入統一管理，待改
        make_folder(self.cache_folderPath)
        return data_path_dict, data_path_df_dict
    
    # ——————————————————————————————————————————————————————————
    # 資料儲存狀態相關函數（開始）
    def _get_data_path(self, data_stack, item, data_level="raw_data"):
        item_folderPath = self.data_path_dict[data_stack][item]
        if data_level == "raw_data":
            return os.path.join(self.raw_data_folderPath, data_stack, item_folderPath)
        elif data_level == "raw_table":
            return os.path.join(self.raw_table_folderPath, data_stack, item_folderPath)
        elif data_level == "table":
            return os.path.join(self.table_folderPath, data_stack, item_folderPath)

    #取得整個data_stack中每個item的完整資料夾地址（通常用於傳遞給外部爬蟲函數）
    def _get_data_path_dict(self, data_stack, data_level="raw_data"):
        data_path_dict = dict()
        for item, data_path in self.data_path_dict[data_stack].items():
            if data_level == "raw_data":
                data_path = os.path.join(self.raw_data_folderPath, data_stack, data_path)
            data_path_dict[item] = data_path
        return data_path_dict

    ## 取得單一資料項目的儲存狀況：起始日期/最新更新日期/資料筆數，回傳dict
    def _get_single_data_status(self, data_stack, item, data_level="raw_data"):
        item_folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
        status_dict = dict()
        # 若該項目不存在raw_data，則返回空值（項目資料夾不存在 & 雖存在但其中沒有檔案，皆視為raw_data不存在）
        if not os.path.exists(item_folderPath) or len(os.listdir(item_folderPath))==0:
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = None, None, 0
        
        #待改：部分資料非時序型資料，如trade_date，待處理
        else:
            # 取得指定項目資料夾中的檔案名
            raw_date_list = os.listdir(item_folderPath)
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
            sub_status_dict = self._get_single_data_status(data_stack=data_stack, item=item, data_level="raw_data")
            status_dict[item] = sub_status_dict
        return pd.DataFrame(status_dict).T
    
    # 資料儲存狀態相關函數（結束）
    # ——————————————————————————————————————————————————————————
    # trade_date處理相關函數（開始）
    
    # 取得交易日日期序列（字串列表），國家預設為US，若不指定時間區間預設為全部取出
    def _get_stock_trade_date_list(self, start_date=None, end_date=None, country="US"):
        # 若未給定起始/結束日期則回傳所有交易日(2000-01-01起)
        market_status_series = self._get_stock_market_status_series(start_date=start_date, end_date=end_date, country=country) 
        # 1: 交易日, 0:六日休市, -1:非六日休市
        trade_date_series = market_status_series[market_status_series==True]
        trade_date_list = list(map(datetime2str, trade_date_series.index))
        return trade_date_list
    
    # Note：須定時補新的market status，目前最新資料至2024-07-06
    # Index為日期，Value:1: 交易日, 0:六日休市, -1:非六日休市
    def _get_stock_market_status_series(self, start_date=None, end_date=None, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        folderPath = self._get_data_path(data_stack=data_stack, item="trade_date", data_level="raw_data")
        filePath = os.path.join(folderPath, "trade_date.csv")
        market_status_series = pd.read_csv(filePath, index_col=0).squeeze()
        # 原csv檔案的日期的日期為字串，格式為2000/01/01，須轉換為datetime
        market_status_series.index = pd.DatetimeIndex(market_status_series.index)

        if start_date != None and end_date != None:
            mask = (market_status_series.index >= start_date) & (market_status_series.index <= end_date)
            return market_status_series[mask]

        return market_status_series

    # 取得距離給定日最近的實際交易日（last：上一個，next：下一個），cal_self可選擇給定的日期本身是否算作交易日
    def get_closest_trade_date(self, date, country="US", direction="last", cal_self=True):
        date = str2datetime(date)
        market_status_series = self._get_stock_market_status_series(country=country)
        
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
    def save_stock_ticker_list(self, universe_name="US_all", source="polygon"):
        # 待改：多資料源管理
        if universe_name=="US_all" and source=="polygon":
            ticker_list, letter_list = list(), list(string.ascii_uppercase)
            interval_letter_list = list()
            for i in range(len(letter_list)-1):
                interval_letter_list.append([letter_list[i], letter_list[i+1]])
            
            for interval in interval_letter_list[:1]:
                interval_ticker_list = list()
                start_letter, end_letter = interval[0], interval[1]
                # 限定爬取普通股 & ETF的所有ticker，因polygon尚包含其餘資產ticker，如warrant...等
                for ticker_type in ["CS", "ETF"]:
                    url = "https://api.polygon.io/v3/reference/tickers?type={}&market=stocks&active=true&limit=1000&apiKey=VzFtRb0w6lQcm1HNm4dDly5fHr_xfviH&ticker.gte={}&ticker.lt={}" \
                          .format(ticker_type, start_letter, end_letter)
                    data_json = requests.get(url).json()
                    interval_ticker_list.extend(pd.DataFrame(data_json["results"])["ticker"].to_list())
                
                logging.info("{}:標的列表下載，字段區間[{}~{}]共{}檔標的".format(universe_name, start_letter, end_letter, len(interval_ticker_list)))
                ticker_list.extend(interval_ticker_list)
                ticker_series = pd.Series(list(set(ticker_list)))
        
        date = datetime2str(datetime.today())
        # 待改：國別管理
        folderPath = self._get_data_path(data_stack="US_stock", item=universe_name, data_level="raw_data")
        filePath = os.path.join(folderPath, date+".csv")
        ticker_series.to_csv(filePath)

    ## 取得單一日期下，特定universe的成分股list，底層函數為self._get_stock_ticker_df
    def get_stock_ticker_list(self, universe_name, date=None, exclude_delist=False, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
        
        # 若未給定日期，則預設取出最新一筆資料
        if date == None:
            date = self._get_single_data_status(data_stack=data_stack, item=universe_name)["end_date"]

        ticker_series = self._get_stock_ticker_df(universe_name=universe_name, start_date=date, end_date=date, 
                                                  exclude_delist=exclude_delist, country=country).squeeze()
        return ticker_series.index.to_list()

    ## 取得特定universe的df資料，row:日期，columns:曾經存在此universe的所有ticker，value:True/False
    def _get_stock_ticker_df(self, universe_name, start_date, end_date, exclude_delist=False, country="US"):
        def remove_delist_ticker(ticker_list):
            # BBG對下市ticker會更改為7位數字+一個字母（D或Q），如2078185D，可以此判別
            # 待改：之後應建立下市股票對照表，載明日期
            ticker_list = [ticker for ticker in ticker_list if len(ticker)<8]
            return ticker_list

        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
        
        raw_ticker_df = self._get_item_data_df_byDate(item=universe_name, data_stack=data_stack, 
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
    def get_item_table_df(self, item, data_stack="US_stock", data_level="raw_table"):
        folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
        filePath = os.path.join(folderPath, item+".pkl")
        df = pd.read_pickle(filePath)            
        return df
    
    # 給定要取出的item_list和所在的data_stack，可指定是否對齊(align)
    def get_item_data_df_list(self, method, item_list, data_stack, start_date=None, end_date=None, latest_num=None,
                               target_stock_ticker_list=None, data_level="raw_data", if_align=False):
        item_df_list = list()
        for item in item_list:
            if data_level == "raw_data":
                if method == "byNum":
                    item_df = self._get_item_data_df_byNum(item, data_stack=data_stack, latest_num=latest_num, target_stock_ticker_list=target_stock_ticker_list)
                
                elif method == "byDate":
                    item_df = self._get_item_data_df_byDate(item, data_stack=data_stack, start_date=start_date, end_date=end_date, target_stock_ticker_list=target_stock_ticker_list)
            
            elif (data_level == "raw_table") or (data_level == "table"):
                folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
                filePath = os.path.join(folderPath, item+".pkl")
                df = pd.read_pickle(filePath)
            
            item_df_list.append(item_df)

        if if_align == True:
            return self._get_aligned_df_list(item_df_list)
        else:
            return item_df_list

    # 將給定的數個df，對齊coloumn與index，空值補Nan
    def _get_aligned_df_list(self, df_list):
        index_list, columns_list = list(), list()
        
        for item_df in df_list:
            index_list.append(set(item_df.index))
            columns_list.append(set(item_df.columns))

        index_series = pd.Series(list(index_list[0].union(*index_list))).dropna()
        columns_series = pd.Series(list(columns_list[0].union(*columns_list))).dropna()
        
        aligned_item_df_list = list()
        for item_df in df_list:
            aligned_item_df = item_df.reindex(index=index_series, columns=columns_series)
            aligned_item_df = aligned_item_df.sort_index()
            aligned_item_df_list.append(aligned_item_df)

        return aligned_item_df_list
    
    # 取得最新K筆資料
    def _get_item_data_df_byNum(self, item, data_stack, latest_num=1, target_stock_ticker_list=None):
        df_list = list()
        item_folderPath = self._get_data_path(item=item, data_stack=data_stack, data_level="raw_data")
        # 取得指定資料夾中所有檔案名(以時間戳記命名）
        raw_date_list = os.listdir(item_folderPath)
        # 去除檔案名中的後綴名（.csv)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（可能由.DS_store等隱藏檔所導致）
        date_list = [date for date in date_list if len(date) != 0]
        # 將時間戳記組合成date series，以篩選出指定的資料區間（同時進行排序與重設index）
        date_series = pd.Series(date_list).apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        # 將date series轉為將date list（字串列表），以用於後續讀取資料
        date_list = list(map(lambda x:datetime2str(x), date_series[-latest_num:]))
        # 依照date list逐日取出每日資料，並組合為DataFrame
        df_list = list()
        for date in date_list:
            fileName = os.path.join(item_folderPath, date+".csv")
            df = pd.read_csv(fileName, index_col=0)
            df_list.append(df)

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
    
    ## 將時序資料取出組合為DataFrame
    def _get_item_data_df_byDate(self, item, data_stack="US_stock", target_stock_ticker_list=None, start_date=None, end_date=None, pre_fetch_nums=0):
        item_folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_data")

        # 待改：應該直接使用dateime?
        if type(start_date) is not str:
            start_date = datetime2str(start_date)
        if type(end_date) is not str:
            end_date = datetime2str(end_date)

        #item_folderPath = os.path.join(folderPath, item)
        # 取得指定資料夾中所有檔案名(以時間戳記命名）
        raw_date_list = os.listdir(item_folderPath)
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
            fileName = os.path.join(item_folderPath, date+".csv")
            df = pd.read_csv(fileName, index_col=0)
            df_list.append(df)

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
            start_date = self._get_single_data_status(data_stack=data_stack, item="stock_splits")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            # 若未給定結束日，則預設會抓取至明日；因下單須預知隔日分割情況以計算下單參考價
            end_date = datetime2str(datetime.today() + timedelta(days=1))

        folderPath = self._get_data_path(data_stack=data_stack, item="stock_splits", data_level="raw_data")
        if source == "polygon":
            save_stock_split_from_Polygon(folderPath, self.polygon_API_key, start_date=start_date, end_date=end_date)    
    
    ## 取得調整因子df，預設為backward（使當前adjclose等同於close）
    def _get_stock_adjust_factor_df(self, start_date=None, end_date=None, country="US", method="backward"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        trade_date_list = self._get_stock_trade_date_list(start_date=start_date, end_date=end_date, country=country)
        stock_splits_df = self._get_item_data_df_byDate(item="stock_splits", data_stack=data_stack, start_date=start_date, end_date=end_date)
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
            start_date = self._get_single_data_status(data_stack=data_stack, item="open")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        folderPath = self._get_data_path(data_stack=data_stack, item="priceVolume", data_level="raw_data")
        
        if source == "polygon":
            save_stock_priceVolume_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, adjust)
        
        elif source == "yfinance":
            cache_folderPath = os.path.join(self.cache_folderPath, "yfinance_priceVolume")
            #待改：cache folder須統一管理
            make_folder(cache_folderPath)
            save_stock_priceVolume_from_yfinance(folderPath, cache_folderPath, start_date, end_date, ticker_list, country, adjust)

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

        if cal_dividend == False:
            # example:"o2o_ret_wo_div"
            item_name += "_wo_div"

        folderPath = self._get_data_path(data_stack=data_stack, item=item_name, data_level="raw_data")
        
        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            # 不遞延一天，係因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
            start_date = self._get_single_data_status(data_stack=data_stack, item=item_name)["end_date"]

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if start_date == end_date:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_date))

        price_df = self._get_item_data_df_byDate(item=price_item, data_stack=data_stack, start_date=start_date, end_date=end_date)
        adjust_factor_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
        
        # 只針對有分割資料的個股作股價調整
        adjust_ticker_list = price_df.columns.intersection(adjust_factor_df.columns)        
        adjusted_item_df = price_df[adjust_ticker_list] * adjust_factor_df
        # 若不給定ticker_list，會導致對賦值時，columns沒有對齊
        price_df[adjust_ticker_list] = adjusted_item_df[adjust_ticker_list]
        # 將股價加上當日除息的現金股利
        if cal_dividend == True:
            dividends_df = self._get_item_data_df_byDate(item="ex_dividends", data_stack=data_stack, start_date=start_date, end_date=end_date)
            dividends_df = dividends_df.fillna(0)
            dividends_ticker_list = price_df.columns.intersection(dividends_df.columns)
            adjusted_dividends_df = price_df[dividends_ticker_list] + dividends_df[dividends_ticker_list]
            price_df[dividends_ticker_list] = adjusted_dividends_df[dividends_ticker_list]
        
        logging.info("[daily return][{method}] 資料計算中".format(method=method))
        # 去除第一row：因取pct後為空值
        return_df = price_df.pct_change().iloc[1:,:]
        # 切分return
        for i in range(len(return_df)):
            data = return_df.iloc[i,:]
            fileName = datetime2str(data.name)
            filePath = os.path.join(folderPath, fileName+".csv")
            data.to_csv(filePath)
    
    # 儲存現金股利raw_data
    def save_stock_cash_dividend(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        if start_date == None:
            start_date = self._get_single_data_status(data_stack=data_stack, item="ex_dividends")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today()+timedelta(days=1))
        
        # 待改，資料源管理
        #folderPath = self._get_data_path(data_stack=data_stack, item="ex_dividends", data_level="raw_data")
        #save_stock_cash_dividend_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, div_type="ex_dividend_date")
        
        # 因同一天公司可能會宣布N筆股利（不同發放日），較難儲存故先略過
        #folderPath = self._get_data_path(data_stack="US_stock", item="declaration_dividends", data_level="raw_data")
        #save_stock_cash_dividend_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, div_type="declaration_date")
        
        folderPath = self._get_data_path(data_stack=data_stack, item="pay_dividends", data_level="raw_data")
        save_stock_cash_dividend_from_Polygon(folderPath, self.polygon_API_key, start_date, end_date, div_type="pay_date")
        
    # 儲存流通股數raw_data
    def save_stock_shares_outstanding(self, ticker_list=None, start_date=None, end_date=None, source="polygon", country="US"):
        if start_date == None:
            start_date = self._get_single_data_status(data_stack=data_stack, item="shares_outstanding")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        folderPath = self._get_data_path(data_stack=data_stack, item="shares_outstanding", data_level="raw_data")
        if source == "polygon":
            cache_folderPath = os.path.join(self.cache_folderPath, "polygon_shares_outstanding")
            trade_date_list = self._get_stock_trade_date_list(start_date=start_date, end_date=end_date, country=country)
            save_stock_shares_outstanding_from_Polygon(folderPath, cache_folderPath, self.polygon_API_key, ticker_list, trade_date_list, start_date, end_date)
    
    # 將各類股票資料的raw_data轉化為raw_table並儲存
    def trans_stock_item_raw_data_to_raw_table(self, item_list, start_date=None, end_date=None, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"
        
        for item in item_list:
            #若不指定起始日，則自該資料所儲存之最早日期，開始合成
            if start_date == None:
                status_dict = self._get_single_data_status(data_stack=data_stack, item=item)
                if status_dict["date_num"] == 0:
                    logging.warning("[{item}] 資料不存在，無法轉化為Table型態".format(item=item))
                    continue
                start_date = status_dict["start_date"]
                    
            if end_date == None:
                end_date = datetime2str(datetime.today())

            # 部分資料組合為塊狀資料後，尚須額外處理，如adjust_factor, universe_ticker...等
            if item in ["univ_ray3000", "univ_ndx100", "univ_dow30", "univ_spx500"]:
                item_df = self._get_stock_ticker_df(universe_name=item, start_date=start_date, end_date=end_date, exclude_delist=False)
            
            # raw data中stock_splits為該日有分割才有資料，在轉化為raw table時進行補日期 & 計算調整係數
            elif item == "stock_splits":
                item_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
            elif item == "trade_date":
                item_df = self._get_stock_market_status_series(start_date=start_date, end_date=end_date, country=country)
            else:
                item_df = self._get_item_data_df_byDate(item=item, data_stack=data_stack, start_date=start_date, end_date=end_date)

            folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_table")            
            filePath = os.path.join(folderPath, item+".pkl")
            item_df.to_pickle(filePath)
            logging.info("[{item}][{start_date}-{end_date}] Raw Table資料轉換&儲存完成".format(item=item, 
                                start_date=datetime2str(item_df.index[0]), end_date=datetime2str(item_df.index[-1])))
    
    # 更新各類股票資料的raw_table，# 若為update模式，將原存在的merged_table與新讀取的資料進行合併
    def update_stock_item_raw_table(self, item_list, country="US"):
        if country == "US":
            data_stack = "US_stock"
        elif country == "TW":
            data_stack = "TW_stock"

        for item in item_list:
            old_item_df = self.get_item_table_df(item=item, data_stack=data_stack, data_level="raw_table")
            old_item_end_date = datetime2str(old_item_df.index[-1] + timedelta(days=1))
            start_date, end_date = old_item_end_date, datetime2str(datetime.today())
            
            if item in ["univ_ray3000", "univ_ndx100", "univ_dow30", "univ_spx500"]:
                item_df = self._get_stock_ticker_df(universe_name=item, start_date=start_date, end_date=end_date, exclude_delist=False)
            elif item == "stock_splits":
                item_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
            elif item == "trade_date":
                item_df = self._get_stock_market_status_series(start_date=start_date, end_date=end_date, country=country)
            else:
                item_df = self._get_item_data_df_byDate(item=item, data_stack=data_stack, start_date=start_date, end_date=end_date)

            # 確認新、舊資料row和column皆未重複，避免concat時報錯
            #item_df = item_df.drop_duplicates()
            item_df = item_df.T.drop_duplicates().T
            #old_item_df = old_item_df.drop_duplicates()
            old_item_df = old_item_df.T.drop_duplicates().T
            item_df = pd.concat([item_df, old_item_df]).sort_index()
            # 確認合併後的資料區間是否已涵蓋該區間內的所有交易日
            trade_date_list = self._get_stock_trade_date_list(start_date=start_date, end_date=end_date)
            item_df_index = list(map(lambda x:datetime2str(x), item_df.index))
            interval_trade_date = list(set(trade_date_list) - set(item_df_index))

            # 待改：應補充
            # if len(interval_trade_date) > 0:
            #     logging.warning("[{item}] 合併後資料區間未涵蓋部分交易日，已略過此資料項目之Raw Table轉換".format(item=item))
            #     logging.warning("[{item}] 缺漏之交易日，列表如下：{interval_trade_date}".format(item=item, interval_trade_date=sorted(interval_trade_date)))
            #     continue
            
            folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_table")
            filePath = os.path.join(folderPath, item+".pkl")
            item_df.to_pickle(filePath)
            logging.info("[{item}][{start_date}-{end_date}] Raw Table資料轉換&儲存完成".format(item=item, 
                            start_date=datetime2str(item_df.index[0]), end_date=datetime2str(item_df.index[-1])))

    # ——————————————————————————————————————————————————————————
    # 待完成的函數（開始）

    # 資料品質控管（Raw_Table -> Table）
    def _get_filtered_raw_table_item_df(self, item, data_stack):
        raw_table_folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level="raw_table")
        raw_table_filePath = os.path.join(raw_table_folderPath, item+".pkl")
        item_df = pd.read_pickle(raw_table_filePath)
        OHLC_list = ["open", "high", "low", "close"]
        # OHLC檢查項目: 中段空值檢查、上下市日期核對、分割資料核對
        if item in OHLC_list:
            adjust_factor_df = self.get_item_table_df(item="stock_splits", data_stack="US_stock", data_level="raw_table")
            interval_nan_ratio = cal_interval_nan_value(item_df)
            max_abs_zscore_series = cal_return_max_abs_zscore(item_df.copy(), adjust_factor_df)

            delete_ticker_list_1 = list(interval_nan_ratio[interval_nan_ratio > 0.1].index)
            delete_ticker_list_2 = list(max_abs_zscore_series[max_abs_zscore_series > 5].index)
            delete_ticker_list_all = list(set(delete_ticker_list_1) | set(delete_ticker_list_2))
            item_df = item_df.drop(delete_ticker_list_all, axis=1)

        elif item == "dividends":
            price_df = self.get_item_table_df(item="close", data_stack="US_stock", data_level="raw_table")
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
            table_folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level="table")
            table_filePath = os.path.join(table_folderPath, item+".pkl")
            item_df.to_pickle(table_filePath)
    
    # 確認是否有股票改名、下市、上市等狀況
    def check_ticker_change(self, item, benchmark_date, start_date="2000-01-01", end_date="2100-12-31", country="US"):
        data_df = self._get_item_data_df_byDate(item=item, start_date=start_date, end_date=end_date)
        benchmark_df = self._get_item_data_df_byDate(item=item, start_date=benchmark_date, end_date=benchmark_date)
        common_ticker_list, new_ticker_list, disappear_ticker_list = compare_component(data_df.columns.dropna(), benchmark_df.columns.dropna())
        
        logging.info("[Check][{item}]本區間資料相較{date}，共新增{n1}檔標的/減少{n2}檔標的".format(item=item, date=benchmark_date, n1=len(new_ticker_list), n2=len(disappear_ticker_list)))
        
        # if len(new_ticker_list)>0:
        #     logging.info("新增標的:")
        #     #logging.info(new_ticker_list)
        if len(disappear_ticker_list)>0:
        #     logging.info("建議確認以下消失標的之變動情況，是否下市或改名，並進行資料回填：")
        #     logging.info("消失標的:")
        #     logging.info(disappear_ticker_list)
            index_ticker_list = self.get_stock_ticker_list("univ_ray3000")
            disappear_index_ticker_list, _, _ = compare_component(disappear_ticker_list, index_ticker_list)
            if len(disappear_index_ticker_list) > 0:
                logging.info("[Check][{item}]消失標的中，共{n}檔為Russel 3000成份股，清單如下：".format(item=item, n=len(disappear_index_ticker_list)))
                logging.info(disappear_index_ticker_list)
    
    # 待新增的資料項目：基本面資料、總經資料
    def save_stock_financialReport_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, source="polygon", country="US"):
        # if ticker_list == None:
        #     ticker_list = self.get_stock_ticker_list("univ_us_all")

        if start_date == None:
            start_date = str2datetime(self._get_single_data_status(data_stack=data_stack, item="revenues")["end_date"])
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if source == "polygon":
            # stock_trade_date_list = self._get_stock_trade_date_list(start_date=start_date, end_date=end_date, country="US")
            # print(stock_trade_date_list)
            if country == "US":
                folderPath_dict = self._get_data_path_dict(data_stack="US_stock")
            elif  country == "TW":
                folderPath_dict = self._get_data_path_dict(data_stack="TW_stock")

            save_stock_financialReport_data_from_Polygon(folderPath_dict=folderPath_dict, API_key=self.polygon_API_key, start_date=start_date, end_date=end_date)
            
    # 取得FRED官網公布的總經數據，name的可用選項，見Macro/Token_trans/fred.txt
    # def get_fred_data(self, name):
    #     folderPath = self.raw_data_macro_folderPath
    #     filePath = os.path.join(folderPath, name+".pkl")
    #     data_df = pd.read_pickle(filePath)  
    #     return data_df

    # def save_fred_data(self, name):
    #     folder_path = self.raw_data_macro_folderPath
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
import pickle
import os, sys, logging, time
from threading import Thread, Lock
from datetime import datetime, timedelta
import pandas as pd
from yahoo_fin.stock_info import *
from fredapi import Fred
from .utils import *

logging.basicConfig(level=logging.INFO,
  format='[%(asctime)s %(levelname)-8s] %(message)s',
  datefmt='%Y%m%d %H:%M:%S',)

class Database(object):
    '''
    - Database為一資料庫物件，功能可分為下載與呼叫資料兩大類
    - 透過給定的database_folder_path對指定的資料庫（資料夾）進行操作
    - 本資料庫中，股號之間的特殊字符一律以 "-" 作為連接符，如：BRK_B, TW_0050, ...
    '''
    def __init__(self, folder_path):
        self.database_folder_path = folder_path
        self.stock_folder_name = "Stock"
        self.tradedate_folder_name = "TradeDate"
        self.ticker_folder_name = "Ticker"
        self.macro_folder_name = "Macro"
        self.macro_token_trans_folder_name = "Token_trans"
        self.test_folder_name = "Test"
        # 若某一類資料會頻繁被重複呼叫，可存放在cache_dict中 (可參考get_next_tradeDate對於cache的使用方式)
        self.cache_dict = {}
        
    ## 呼叫資料的函數區域（開始）

    # 取得給定單一標的之價格資料（開高低收量）並返回df
    # auto_download：找不到此標的資料時會自動下載，下載完成後回傳資料（預設為False）
    # show_warning：找不到此標的資料時會顯示警告訊息（預設為True）
    def get_stock_data(self, ticker, auto_download=False, show_warning=True):
        folder_name = self.stock_folder_name
        ticker_for_downloadFile = ticker.replace('.', '_')
        file_position = os.path.join(self.database_folder_path, folder_name, ticker_for_downloadFile+".pkl")
        
        if os.path.isfile(file_position):
            stockPrice = pd.read_pickle(file_position)
            return stockPrice

        else:
            if show_warning:
                logging.warning(ticker + ": Data has not been downloaded.")

            if auto_download == True:
                if show_warning:
                    logging.warning(ticker+ ": Try to fill the data...")

                self.save_stockPrice_to_pkl([ticker])
                stockPrice = pd.read_pickle(file_position)
                return stockPrice

            return None
            
    # 取得大盤指數成分股ticker的list
    # name的選項：SP500, NASAQ, TW50, TW_all
    def get_ticker_list(self, name):
        folder_name = self.ticker_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, name + "_ticker_list.pkl")
        #若ticker list存在，則返回list
        if os.path.isfile(file_position):
            with open(file_position, 'rb') as f:
                ticker_list = pickle.load(f)
                f.close()
            return ticker_list
        #若ticker list不存在，則返回空list
        else:
            logging.warning(name+ "：資料尚未下載")
            return []
    
    # 取得特定區域的交易日日期序列，區域預設為美國（US），其他選項包括：TW（台灣）
    def get_tradeDate_list(self, country="US"):
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, country+"_trade_date.pkl")
        tradeDate_list = pd.read_pickle(file_position)
        tradeDate_list = tradeDate_list.to_list()
        self.cache_dict["tradeDate_list"] = tradeDate_list
        return tradeDate_list

    # 待改：get_next_tradeDate與get_last_tradeDate應該合併
    # 若給定日期並非交易日，則取得距離最近的下一個實際交易日（區域預設為美國（US）），其他選項包括：TW（台灣）
    def get_next_tradeDate(self, date, shift_days=0, country="US"):
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
        date = date+timedelta(days=shift_days)

        if "tradeDate_list" in self.cache_dict.keys():
            tradeDate_list = self.cache_dict["tradeDate_list"]
        else:
            tradeDate_list = self.get_tradeDate_list(country=country)

        if date not in tradeDate_list:
            virtual_date_list =tradeDate_list.copy()
            virtual_date_list.append(date)
            virtual_date_list.sort()        
            latest_tradeDate_index = virtual_date_list.index(date)
            return tradeDate_list[latest_tradeDate_index-1]
        else:
            return date

    # 取得FRED官網公布的總經數據，name的可用選項，見Macro/Token_trans/fred.txt
    def get_fred_data(self, name):
        folder_path = os.path.join(self.database_folder_path, self.macro_folder_name)
        file_position = os.path.join(folder_path, name+".pkl")
        data_df = pd.read_pickle(file_position)  
        return data_df

    # 取得Universe成分股的價格資料，依照data_type合併成完整的df（data_type預設為調整價）
    # start_date和end_date決定資料的時間區段，在self._slice_df透過指定的data_format進行資料切割
    def get_universe_df(self, universe_ticker_list, data_type="adjclose", start_date="1900-01-01", end_date="2100-12-31", data_format="all"):
        universe_df_list = list()
        # logging.info("Universe資料合併中(資料類型："+data_type + ")")
        for index, ticker in enumerate(universe_ticker_list, 1):
            ticker_df = self.get_stock_data(ticker)
            ticker_df_specific_data = ticker_df.loc[:, [data_type]]
            universe_df_list.append(ticker_df_specific_data.rename(columns={data_type:ticker}))       
            percentage = 100*index/len(universe_ticker_list)
            # sys.stdout.write('\r'+"資料合併完成度{percentage:.2f}%".format(percentage=percentage))

        #待改：這邊真的需要print嗎
        print()
        
        universe_df = pd.concat(universe_df_list, axis=1)
        return self._slice_df(universe_df, start_date, end_date, data_format)

    # 依據時間區段切割資料，並依data_format選取資料
    # 因各檔股票的資料起始日期不同，data_format可指定是否剔除不存在的標的資料
    def _slice_df(self, df, start_date, end_date, data_format):
        df = df[df.index >= start_date]
        df = df[df.index <= end_date]
        
        # 不作調整，直接回傳原始資料
        if data_format == "raw":
            pass

        #僅擷取日期後作ffill回傳，開頭可能有大量空值
        elif data_format == "all":
            df.ffill(inplace=True, axis ='rows')
            
        #只留下start_date當日便有資料的ticker，當日不存在的資料即全數刪除
        elif data_format == "only_exist_ticker":
            df.ffill(inplace=True, axis ='rows')
            df.dropna(inplace=True, axis='columns')
            df.dropna(inplace=True, axis='rows')

        #延後start_date至所有ticker的資料皆存在的那天
        elif data_format == "all_ticker_latest":
            df.ffill(inplace=True, axis ='rows')
            df.dropna(inplace=True, axis='rows')
            df.dropna(inplace=True, axis='columns')

        return df
    
    ## 呼叫資料的函數區域（結束）
    ## 下載資料的函數區域（開始）
    
    # 由self.save_stockPrice_to_pkl進行multithread調用，依據指定的資料來源，下載個股資料
    def _save_stockPrice(self, ticker, folder_name, country, data_source):
        if data_source == "yahoo_fin":
            # yahoo_finance的國家代碼間隔號使用‘.’，股類分隔號使用'-'，如：rds-b，但資料夾內部存檔時一律用‘_’
            if country == "US":
                ticker_for_search = ticker.replace('.', '-')

            elif country =="TW":
                ticker_for_search = ticker.replace('_', '.')
            
            ticker_for_saveFile = ticker.replace('.', '_')
            file_position = os.path.join(self.database_folder_path, folder_name, ticker_for_saveFile+".pkl")
            price_data = _download_data_from_yahoo(ticker_for_search)
            price_data.to_pickle(file_position)

        else:
            raise Exception("目前不支援此資料下載來源({data_source})".format(data_source=data_source))

    # 呼叫self._save_stockPrice進行multithread調用，以提升下載速度
    # 待改：計算下載失敗次數的功能目前有問題，且應顯示下載失敗的標的，產生list
    def save_stockPrice_to_pkl(self, ticker_list, country="US", data_source="yahoo_fin"):
        folder_name = self.stock_folder_name
        threads = []  # 儲存線程以待關閉
        num_completed = 0
        num_uncompleted = 0

        try:
            start_time = time.time()
            for index, ticker in enumerate(ticker_list, 1):
                t = Thread(target=self._save_stockPrice, args=(ticker, folder_name, country, data_source))
                t.start()  # 開啟線程
                threads.append(t)
                num_completed +=1
                percentage = 100*round(index/len(ticker_list), 2)            
                print("{ticker:<6} 股價資料下載中，完成度{percentage}%".format(ticker=ticker, percentage=percentage))                
            
            # 關閉線程
            for t in threads:
                t.join()
        
        except Exception as e:
            num_uncompleted +=1
            logging.warning(e)

        finally:    
            end_time = time.time()
            logging.info("嘗試下載了{0}筆，其中共{1}筆不成功".format(num_completed, num_uncompleted))
            logging.info('共耗費{:.3f}秒'.format(end_time - start_time))

    def save_US_tradeDate_to_pkl(self):
        SPX = _download_data_from_yahoo("^GSPC")
        trade_date = SPX.index.to_series()
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, "US_trade_date.pkl")
        trade_date.to_pickle(file_position)
        print("US TradeDate has been saved. (Referenced by SPX 500)")
        return trade_date

    def save_TW_tradeDate_to_pkl(self):
        TW_0050 = _download_data_from_yahoo("0050.TW")
        trade_date = TW_0050.index.to_series()
        folder_name=self.tradedate_folder_name
        file_position = os.path.join(self.database_folder_path, folder_name, "TW_trade_date.pkl")
        trade_date.to_pickle(file_position)
        print("TW TradeDate has been saved. (Referenced by TW_0050)")
        return trade_date

    #待改：尋找自動抓取的來源
    def save_TW50_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = TW_ticker_list = ['2330_TW', '2454_TW', '2317_TW', '2308_TW', '2412_TW', '1301_TW', '2303_TW', '1303_TW', '2891_TW', '3008_TW', '2882_TW', '2881_TW', '2886_TW', '1216_TW', '2884_TW', '2002_TW', '1326_TW', '3711_TW', '2885_TW', '1101_TW', '2892_TW', '2207_TW', '2382_TW', '5880_TW', '5871_TW', '2379_TW', '2357_TW', '2880_TW', '3045_TW', '2912_TW', '2887_TW', '5876_TW', '4938_TW', '2395_TW', '2883_TW', '2890_TW', '2801_TW', '6415_TW', '6505_TW', '1402_TW', '2301_TW', '4904_TW', '1102_TW', '9910_TW', '2105_TW', '6669_TW', '2408_TW']
        file_position = os.path.join(self.database_folder_path, folder_name, "TW50_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()
        
        print("TW50 tickers has been saved.")
        return ticker_list

    def save_sp500_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = _download_sp500_ticker_list_from_yahoo()
        file_position = os.path.join(self.database_folder_path, folder_name, "SP500_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()
        
        print("SP500 tickers has been saved.")
        return ticker_list

    def save_nasdaq_ticker_list(self):
        folder_name=self.ticker_folder_name
        ticker_list = _download_nasdaq_ticker_list_from_yahoo()
        file_position = os.path.join(self.database_folder_path, folder_name, "NASDAQ_ticker_list.pkl")
        
        with open(file_position, 'wb') as f:
            pickle.dump(ticker_list, f)
            f.close()

        print("NASDSQ tickers has been saved.")
        return ticker_list

    def save_fred_data(self, name):
        folder_path = os.path.join(self.database_folder_path, self.macro_folder_name)
        token_trans_folder_name = os.path.join(folder_path, self.macro_token_trans_folder_name)
        
        api_key = token_trans("API_Key", source="fred", folder_path=token_trans_folder_name)
        data_key = token_trans(name, source="fred", folder_path=token_trans_folder_name)
        
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

# 資料爬蟲包裝區
# 抓取資料的底層使用yahoo_fin的模組，以下將其包裝起來，以方便日後修改
def _download_data_from_yahoo(ticker):
    return get_data(ticker)

def _download_sp500_ticker_list_from_yahoo():
    return tickers_sp500()

def _download_nasdaq_ticker_list_from_yahoo():
    return tickers_nasdaq()
    
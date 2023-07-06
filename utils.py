import os
from datetime import datetime, date, timedelta
import pandas as pd

def token_trans(name, source, folder_path):
    trans_table_file_path = os.path.join(folder_path, source+".txt")
    
    item_name_list = []
    key_list = []
    with open(trans_table_file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n')
            item_name, key = line.split(":")
            item_name_list.append(item_name)
            key_list.append(key)
    
    trans_dict = dict(zip(item_name_list, key_list))
    return trans_dict[name]

def shift_days_by_strDate(strdate, days):
    shifted_date = datetime.strptime(strdate, "%Y-%m-%d") + timedelta(days)
    shifted_date = shifted_date.strftime("%Y-%m-%d")
    return shifted_date

def str2datetime(strdate):
    return datetime.strptime(strdate, "%Y-%m-%d")

def datetime2str(date):
    return date.strftime("%Y-%m-%d")

def make_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def cal_adjust_factor_df(stock_splits_df, date_list, method):
    adjust_ticker_list = stock_splits_df.columns
    adjust_factor_df = pd.DataFrame(index=date_list, columns=adjust_ticker_list)
    adjust_factor_df[adjust_ticker_list] = stock_splits_df[adjust_ticker_list]    

    adjust_factor_df = adjust_factor_df.fillna(1)
    adjust_factor_df[adjust_factor_df==0] = 1
    
    if method == "forward":
        adjust_factor_df = adjust_factor_df.cumprod()
        
    elif method == "backward":
        cumulative_splits = adjust_factor_df.cumprod().iloc[-1,:]
        adjust_factor_df = (1/adjust_factor_df).cumprod() * cumulative_splits
        adjust_factor_df = 1/adjust_factor_df

    else:
        raise Exception("method typo")

    adjust_factor_df.index = pd.to_datetime(adjust_factor_df.index)
    return adjust_factor_df

# 若每日變動超過門檻（預設為100%），列為可能出錯清單
def check_potential_error_by_change(price_df, threshold=1):
    change_df = price_df.pct_change()
    max_change = abs(change_df).max()
    huge_change_ticker_index = max_change[max_change > threshold].index
    return huge_change_ticker_index

#若數據顯示從未進行股票分割，列為可能出錯清單
def check_potential_error_by_split(split_df):
    total_split = split_df.sum()
    never_split_ticker_index = total_split[total_split==0].index
    return never_split_ticker_index

# 比對兩序列的交集/差集，並列出序列1、2獨有的資料
def compare_component(s1, s2):
    common_part_list = sorted(list(set(s1) & set(s2)))
    only_1_part_list = sorted(list(set(s1) - set(s2)))
    only_2_part_list = sorted(list(set(s2) - set(s1)))
    return common_part_list, only_1_part_list, only_2_part_list

# 塊狀資料轉化為時序資料:

# # 將指定資料夾中的塊狀資料(fileName:ticker, row:date, column:items)轉化為時序型資料
# source_folder_path = "/Users/yahoo168/Documents/programming/Quant/Database/Stock"
# # 取得指定資料夾中的檔案名作為ticker_list
# raw_ticker_list = os.listdir(source_folder_path)
# # 去除後綴檔名
# ticker_list = [ticker.split(".")[0] for ticker in raw_ticker_list]
# # 去除異常空值
# ticker_list = [ticker for ticker in ticker_list if len(ticker)!=0]

# # 指定需要轉化的資料項目
# item_list = ["open", "high", "low", "close", "adjclose", "volume"]
# for item in item_list:
#     df_list = []
#     for ticker in ticker_list:
#         fileName = os.path.join(source_folder_path, ticker+".pkl")
#         df = pd.read_pickle(fileName)
#         # 篩選出塊狀資料中的指定資料項目
#         data_series = df[item]
#         data_series = data_series.rename(ticker)
#         df_list.append(data_series)
#     # Trasform後，row:tickers, colunm:Date
#     df = pd.concat(df_list, axis=1).T
#     for num in range(len(df.columns)):
#         # 依序將各column切出，轉為csv檔，檔名為該日日期
#         data_series = df.iloc[:,num]
#         date = data_series.name.strftime("%Y-%m-%d")
#         # 存檔至以指定資料項目為名的資料夾之下
#         fileName = os.path.join(save_folder_path, item, date+".csv")
#         data_series.to_csv(fileName)

# Ticker轉換
# ticker_path = "/Users/yahoo168/Documents/programming/Quant/Database/Ticker/TW50_ticker_list.pkl"
# NASDAQ_ticker_list = pd.read_pickle(ticker_path)
# fileName = "/Users/yahoo168/Documents/programming/Quant/Database/Ticker/ticker_list.csv"
# pd.Series(NASDAQ_ticker_list).to_csv(fileName)


    # 建立資料夾路徑，母資料夾下含Raw_Data、Raw_Table、Table三資料夾
    # def _build_folderPath2(self):
    #     self.data_path_folderPath = os.path.join(self.database_folderPath, "data_path")
    #     make_folder(self.data_path_folderPath)

    #     self.raw_data_folderPath = os.path.join(self.database_folderPath, "raw_data")
    #     self.raw_table_folderPath = os.path.join(self.database_folderPath, "raw_table")
    #     self.table_folderPath = os.path.join(self.database_folderPath, "table")
    #     self.cache_folderPath = os.path.join(self.database_folderPath, "cache")
    #     make_folder(self.cache_folderPath)
        
    #     self.raw_data_US_stock_folderPath = os.path.join(self.raw_data_folderPath, "US_stock")
    #     self.raw_data_TW_stock_folderPath = os.path.join(self.raw_data_folderPath, "TW_stock")
    #     self.raw_data_macro_folderPath = os.path.join(self.raw_data_folderPath, "US_macro")
    #     make_folder(self.raw_data_US_stock_folderPath)
    #     make_folder(self.raw_data_TW_stock_folderPath)
    #     make_folder(self.raw_data_macro_folderPath)
        
    #     self.raw_table_US_stock_folderPath = os.path.join(self.raw_table_folderPath, "US_stock")
    #     self.raw_table_TW_stock_folderPath = os.path.join(self.raw_table_folderPath, "TW_stock")
    #     self.raw_table_macro_folderPath = os.path.join(self.raw_table_folderPath, "US_macro")
    #     make_folder(self.raw_table_US_stock_folderPath)
    #     make_folder(self.raw_table_TW_stock_folderPath)
    #     make_folder(self.raw_table_macro_folderPath)

    #     self.table_US_stock_folderPath = os.path.join(self.table_folderPath, "US_stock")
    #     self.table_TW_stock_folderPath = os.path.join(self.table_folderPath, "TW_stock")
    #     self.table_macro_folderPath = os.path.join(self.table_folderPath, "US_macro")
    #     make_folder(self.table_US_stock_folderPath)
    #     make_folder(self.table_TW_stock_folderPath)
    #     make_folder(self.table_macro_folderPath)
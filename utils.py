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

def str2datetime_list(strdate_list):
    return list(map(lambda x:str2datetime(x), strdate_list))

def datetime2str_list(date_list):
    return list(map(lambda x:datetime2str(x), date_list))

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

# 將給定的數個df，對齊coloumn與index，空值補Nan
def get_aligned_df_list(df_list):
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

TODAY_DATE_STR = datetime2str(datetime.today())
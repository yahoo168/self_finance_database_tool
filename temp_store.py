# 確認是否有股票改名、下市、上市等狀況（via 價格資料）
def _check_ticker_change2(self, item, benchmark_date, data_stack="US_stock", start_date="2000-01-01", end_date="2100-12-31", country="US"):
    data_df = self._get_item_data_df_by_date(item=item, data_stack=data_stack, start_date=start_date, end_date=end_date)
    benchmark_df = self._get_item_data_df_by_date(item=item, data_stack=data_stack, start_date=benchmark_date, end_date=benchmark_date)
    common_ticker_list, new_ticker_list, disappear_ticker_list = compare_component(data_df.columns.dropna(), benchmark_df.columns.dropna())
    
    logging.info("[Check][{item}]本區間資料相較{date}，共新增{n1}檔標的/減少{n2}檔標的".format(item=item, date=benchmark_date, n1=len(new_ticker_list), n2=len(disappear_ticker_list)))
    if len(new_ticker_list)>0:
        logging.info("新增標的:")
        logging.info(new_ticker_list)
    if len(disappear_ticker_list) > 0:
        logging.info("建議確認以下消失標的之變動情況，是否下市或改名，並進行資料回填：")
        logging.info("消失標的:")
        logging.info(disappear_ticker_list)
        index_ticker_list = self.get_stock_ticker_list("univ_ray3000")
        disappear_index_ticker_list, _, _ = compare_component(disappear_ticker_list, index_ticker_list)
        if len(disappear_index_ticker_list) > 0:
            logging.info("[Check][{item}]消失標的中，共{n}檔為Russel 3000成份股，清單如下：".format(item=item, n=len(disappear_index_ticker_list)))
            logging.info(disappear_index_ticker_list)

# 給定要取出的item_list和所在的data_stack，可指定是否對齊(align)
def get_item_data_df_list(self, method, item_list, data_stack, start_date=None, end_date=None, latest_num=None,
                           target_stock_ticker_list=None, data_level="raw_data", if_align=False):
    item_df_list = list()
    for item in item_list:
        if data_level == "raw_data":
            if method == "byNum":
                item_df = self._get_item_data_df_by_date_byNum(item, data_stack=data_stack, latest_num=latest_num, target_stock_ticker_list=target_stock_ticker_list)
            
            elif method == "byDate":
                item_df = self._get_item_data_df_by_date(item, data_stack=data_stack, start_date=start_date, end_date=end_date, target_stock_ticker_list=target_stock_ticker_list)
        
        elif (data_level == "raw_table") or (data_level == "table"):
            folderPath = self._get_data_path(data_stack=data_stack, item=item, data_level=data_level)
            filePath = os.path.join(folderPath, item+".pkl")
            df = pd.read_pickle(filePath)
        
        item_df_list.append(item_df)

    if if_align == True:
        return self._get_aligned_df_list(item_df_list)
    else:
        return item_df_list

# 計算檔案更新日期距離共幾日
days_from_last_update = (datetime.today() - datetime.fromtimestamp(os.path.getmtime(filePath))).days
# 取得自檔案上次更新以來，新增的標的數量
new_component_list, deleted_component_list = self._check_component_change(date=TODAY_DATE_STR, country="US", compare_date_num=days_from_last_update+1)
#print(days_from_last_update)
#print(new_component_list)
ticker_list = new_component_list.copy()

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


## 儲存trade_date，目前以yfinance中ETF有報價的日期作為交易日，美國參考SPY，台灣參考0050
# def save_stock_trade_date(self, source="yfinance", country="US"):
#     #待改：應標記非交易日是六日or假日(holidays)，可用1、0、-1標記
#     if country == "US":
#         folderPath = self._get_data_path(data_stack="US_stock", item="trade_date", data_level="raw_data")
#         reference_ticker = "SPY"
    
#     elif country == "TW":
#         folderPath = self._get_data_path(data_stack="TW_stock", item="trade_date", data_level="raw_data")
#         reference_ticker = "0050.TW" 
            
#     if source == "yfinance":
#         reference_df = yf.Ticker(reference_ticker).history(period="max")

#     elif source == "yahoo_fin":
#         reference_df = _download_data_from_Yahoo_Fin(reference_ticker)

#     trade_date_series = reference_df.index.strftime("%Y-%m-%d").to_series()
#     filePath = os.path.join(folderPath, "trade_date.csv")
#     trade_date_series.to_csv(filePath, index=False)
#     logging.info("[{country} trade_date] 已儲存".format(country=country))
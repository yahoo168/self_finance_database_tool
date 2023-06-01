#將BBG的指數成分股資料（塊狀），轉為時序資料存於資料庫中
def trans_BBG_index_component_to_DB(self, BBG_data_filePath, index_name):
    data = pd.read_csv(BBG_data_filePath, index_col=0).T
    folderPath = os.path.join(self.ticker_folderPath, index_name)
    for i in range(len(data.columns)):
        cross_sect_ticker_series = data.iloc[:, i]
        index_ticker_series = cross_sect_ticker_series[cross_sect_ticker_series==True].index.to_series()
        #BBG內建ticker連結符為“/”，以“_”替代
        index_ticker_series = index_ticker_series.apply(lambda x:x.split(" ")[0].replace('/', '_'))
        date = cross_sect_ticker_series.name
        filePath = os.path.join(folderPath, date+".csv")
        index_ticker_series.to_csv(filePath)
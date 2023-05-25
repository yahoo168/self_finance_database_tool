def compare_database(db_1, db_2):
	print("Data Status:")
	item_list = ["open", "high", "low", "close", "volume"]
	data_status_1 = db_1.get_data_status(item_list, country="US", data_class="stock")
	data_status_2 = db_2.get_data_status(item_list, country="US", data_class="stock")
	print(data_status_1)
	print("\n")
	print(data_status_2)
	print("-"*50)

	db_1.get_stock_data_df(item, country="US")
	db_2.get_stock_data_df(item, country="US")
import os

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
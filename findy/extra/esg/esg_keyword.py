import os

import pandas as pd

from findy import findy_env


def esg_news_key():
    # opening the file in read mode 
    my_file = open(os.path.join(findy_env['source_path'], 'esg', "esg_news_keyword.txt"), "r") 
    
    # reading the file 
    data = my_file.read() 
    
    # replacing end splitting the text  
    # when newline ('\n') is seen. 
    keyword = data.split("\n") 
    # print(data_into_list) 
    my_file.close() 

    return keyword
    
    
def esg_companys_key():
    df_2002 = pd.read_csv(os.path.join(findy_env['source_path'], 'esg', "ESG-2022-top-rank.csv"))
    df_2003 = pd.read_csv(os.path.join(findy_env['source_path'], 'esg', "ESG-2023-top-rank.csv"))
    
    companylist = list(set(df_2002['Company'].to_list() + df_2003['Company'].to_list()))
    # print(companylist)
    
    return companylist
    
    
if __name__ == "__main__":
    esg_companys_key()
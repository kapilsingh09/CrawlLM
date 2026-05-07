import pandas as pd
import numpy as np


df = pd.read_csv(r'C:\Users\karan\Desktop\Finalapp\CrawlLM_v1\output\crawl_20260507_211351.csv')

df.drop(['type'],axis=1,inplace=True)



print(df.head())
print(df.columns)
"""
期交所爬蟲程式
程式碼撰寫: 蘇彥庭
日期: 20210203
程式說明: 此程式主要下載期交所台指期資料
"""

# 載入套件
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

# 參數設定
# 設定下載日期
downloadDate = '2021/02/01'

# post參數
post = {
    'queryType': '2',
    'marketCode': '0',
    'dateaddcnt': '',
    'commodity_id': 'TX',
    'commodity_id2': '',
    'queryDate': downloadDate,
    'MarketCode': '0',
    'commodity_idt': 'TX',
    'commodity_id2t': ''
}

# 目標網址
url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'

# 下載網頁
response = requests.post(url, data=post)
soup = BeautifulSoup(response.text, 'html.parser')

# 資料清洗
datas = soup.select('table.table_f')[0].select('tr')  # 行情表
rows = list()
for i in range(len(datas)):
    if i == 0:
        # 處理標題
        columns = [elem.text for elem in datas[i].select('th')]
    else:
        # 處理數據
        rows.append([re.sub(r'[\t\n\r]', ' ', elem.text).strip() for elem in datas[i].select('td')])
df = pd.DataFrame(data=rows, columns=columns)
print(df)

# # 確認資料
# fileName = downloadDate.replace('/', '-') + '_TX_future_data.csv'
# df.to_csv(fileName, encoding='big5')


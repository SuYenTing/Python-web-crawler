"""
證交所及櫃買中心爬蟲程式
程式碼撰寫: 蘇彥庭
日期: 20210105
程式說明: 此程式主要用於下載股價資料表(爬取最近7個交易日資料)
"""

# 載入套件
import datetime
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup

# 產生近7個實際日期
todayDate = datetime.datetime.now()
dateList = []
for i in range(7):
    iDate = todayDate - datetime.timedelta(days=i)
    dateList.append(iDate.strftime('%Y%m%d'))

# 建立儲存表
stockPriceData = pd.DataFrame()

# 迴圈日期下載資料
for iDate in dateList:

    # 下載證交所資料
    # 取得目標日期資料
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=' + iDate + '&type=ALLBUT0999'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 判斷是否有空資料存在 若存在則跳離此次迴圈
    if ('很抱歉，沒有符合條件的資料!' in soup.text):
        continue

    # 整理證交所每日收盤行情表
    table = soup.find_all('table')[8]
    columnNames = table.find('thead').find_all('tr')[2].find_all('td')
    columnNames = [elem.getText() for elem in columnNames]
    rowDatas = table.find('tbody').find_all('tr')
    rows = list()
    for row in rowDatas:
        rows.append([elem.getText().replace(',', '').replace('--', '') for elem in row.find_all('td')])
    df = pd.DataFrame(data=rows, columns=columnNames)
    df = df[['證券代號', '證券名稱', '開盤價', '最高價', '最低價', '收盤價', '成交股數', '成交金額']]
    df = df.rename({'證券代號': 'code', '證券名稱': 'name', '開盤價': 'open', '最高價': 'high',
                    '最低價': 'low', '收盤價': 'close', '成交股數': 'volume', '成交金額': 'value'}, axis=1)
    df.insert(0, 'date', iDate, True)
    df.insert(1, 'mkt', 'tse', True)

    # 儲存證交所資料
    stockPriceData = pd.concat([stockPriceData, df])
    time.sleep(1)

    # 下載櫃買中心資料
    # 取得目標日期資料
    url = ('https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?'
           'l=zh-tw&o=htm&d=' + str(int(iDate[0:4])-1911) + '/' + iDate[4:6] + '/' + iDate[6:8] + '&se=EW&s=0,asc,0')
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 整理櫃買中心每日收盤行情表
    table = soup.find('table')
    columnNames = table.find('thead').find_all('tr')[1].find_all('th')
    columnNames = [elem.getText() for elem in columnNames]
    rowDatas = table.find('tbody').find_all('tr')
    rows = list()
    for row in rowDatas:
        rows.append([elem.getText().replace(',', '').replace('----', '') for elem in row.find_all('td')])
    df = pd.DataFrame(data=rows, columns=columnNames)
    df = df[['代號', '名稱', '開盤', '最高', '最低', '收盤', '成交股數', '成交金額(元)']]
    df = df.rename({'代號': 'code', '名稱': 'name', '開盤': 'open', '最高': 'high',
                    '最低': 'low', '收盤': 'close', '成交股數': 'volume', '成交金額(元)': 'value'}, axis=1)
    df.insert(0, 'date', iDate, True)
    df.insert(1, 'mkt', 'otc', True)

    # 儲存櫃買中心資料
    stockPriceData = pd.concat([stockPriceData, df])
    time.sleep(1)

# 檢查資料完整度
# print(stockPriceData.groupby('date').count())

# 呈現結果
print(stockPriceData)
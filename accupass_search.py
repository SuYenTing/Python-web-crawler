# Accupass網站活動查詢
# 2021/02/21
# 程式撰寫: 蘇彥庭
# 程式說明:
# * 此程式主要爬取在Accupass網站以關鍵字查詢活動後 返回的相關活動資訊
# * 此程式未測試Accupass網站的反爬蟲機制 若大量爬取有可能會出錯
import time
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup

# 設定查詢關鍵字
searchKeyWord = '草莓季'

# 設定最大查詢頁數
maxSearchPage = 3

# 建立儲存表
allEventRows = list()

# 迴圈下載各搜尋頁面資訊
for iPage in range(maxSearchPage):

    # 目標網址
    url = 'https://old.accupass.com/search/changeconditions/r/0/0/0/0/4/' + str(iPage) + \
          '/00010101/99991231/?q=' + searchKeyWord

    # 取得活動查詢頁面資訊
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # 取出活動資訊
    eventRows = soup.select('div.col-xs-12.col-sm-6.col-md-4')
    eventRows = [elem.select('div')[0]['event-row'] for elem in eventRows]
    eventRows = [json.loads(elem) for elem in eventRows]

    # 儲存活動資訊
    allEventRows.extend(eventRows)

    # 暫緩速度
    time.sleep(1)

# 轉為Pandas格式
eventInfoTable = pd.DataFrame(allEventRows)

# 輸出csv檔案
fileName = 'accupass_' + searchKeyWord + '_搜尋活動結果報表.csv'
eventInfoTable.to_csv(fileName, index=False, encoding='utf-8-sig')

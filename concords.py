# 康和證券盤後資料專區-外資上市/上櫃買超排行
# 頁面來源: https://concords.moneydj.com/z/zg/zg_D_0_-1.djhtm
# 程式碼撰寫: 蘇彥庭
# 日期: 2023/05/28
import requests
from bs4 import BeautifulSoup
import pandas as pd

########## 參數設定 ##########
# url: https://concords.moneydj.com/z/zg/zg_D_{mktType}_{days}.djhtm

# mktType: 市場類別
# 0: 上市 1:上櫃
mktType = 0

# days: 計算日數
# -1: 1周以來 
# 1: 1日
# 2: 2日
# 3: 3日
# 4: 4日
# 5: 5日
# 10: 10日
# 20: 20日
# 30: 30日
days = 1

########## 主程式 ##########
# 目標網址
url = f"https://concords.moneydj.com/z/zg/zg_D_{mktType}_{days}.djhtm"
headers = {
    'Content-Type': 'text/html;Charset=big5'
}

# 執行爬蟲取得資訊
# 此處採坑: 若用html.parser解析會發生錯誤 不知道為什麼解析出來會自動添加不必要的頁籤
# 讓後續整理資料時發生錯誤 改用lxml解析即不會出錯
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'lxml')

# 取得table頁籤資料
table = soup.select('table#oMainTable')

assert len(table) == 1, "無法在頁面找到目標表格, 請確認爬蟲程式是否有問題!"

# 選擇tr標籤
tableRows = table[0].select('tr')

# 排除第0個tr標籤 因為是非目標資料
# 迴圈整理每個tr標籤內的td標籤資料
output = list()
for i in range(1, len(tableRows)):
    output.append([elem.text for elem in tableRows[i].select('td')])

# 轉為Panda資料表格式 此即為最後所需的排行榜資料
output = pd.DataFrame(output[1:], columns=output[0])

# 輸出資料
output.to_csv('output.csv', index=False, encoding='utf-8-sig')
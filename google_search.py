"""
Google搜尋結果爬蟲程式
程式碼撰寫: 蘇彥庭
日期: 20210116
"""

# 載入套件
import sys
import requests
from bs4 import BeautifulSoup

# 搜尋字詞
query = 'tibame'

# 設定hearers
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/87.0.4280.141 Safari/537.36'}

# 執行爬蟲下載搜尋結果頁面標題
url = 'https://www.google.com/search?q=' + query
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')
content = soup.find_all('div', class_='g')
title = [elem.find('h3').getText() for elem in content]

# 輸出查詢結果
print('Google搜尋結果頁面共有以下標題:')
[print(elem) for elem in title]


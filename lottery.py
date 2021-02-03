"""
威力彩及大樂透開獎結果爬蟲程式碼
程式碼撰寫: 蘇彥庭
日期: 20210113
"""

# 載入套件
import requests
from bs4 import BeautifulSoup

# 下載台灣彩券首頁原始碼
url = 'https://www.taiwanlottery.com.tw/'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# 整理威力彩開獎號碼
content = soup.find_all('div', class_='contents_box02')[0].find_all('div')
result = [elem.getText() for elem in content][-7:]
print('威力彩開獎號碼: ' + ', '.join(result))

# 整理大樂透開獎號碼
content = soup.find_all('div', class_='contents_box02')[2].find_all('div')
result = [elem.getText() for elem in content][-7:]
print('大樂透開獎號碼: ' + ', '.join(result))




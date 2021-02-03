"""
公開資訊觀測站-重大訊息爬蟲程式碼
程式碼撰寫: 蘇彥庭
日期: 20210108
"""

# 載入套件
import datetime
import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup
import re


# 確認是否有正常連線
def CheckConnect(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        if '查詢過於頻繁' in soup:  # 查詢過於頻繁視為下載失敗
            print('查詢過於頻繁!')
            soup = None
            checkSuccess = False
        else:
            checkSuccess = True
        return soup, checkSuccess
    except Exception as e:
        print('下載失敗!')
        soup = None
        checkSuccess = False
        return soup, checkSuccess


# 將ROC日期轉換為西元日期
def ConvertDate(idate):
    if '年' in idate:
        return str((int(re.findall('民國(\\d+)年', idate)[0]) + 1911) * 10000 + \
                   int(re.findall('年(\\d+)月', idate)[0]) * 100 + \
                   int(re.findall('月(\\d+)日', idate)[0]))
    elif '/' in idate:
        idate = idate.split('/')
        return str((int(idate[0]) + 1911) * 10000 + int(idate[1]) * 100 + int(idate[2]))
    # else:
    #     return str((int(idate[0:3]) + 1911) * 10000 + int(idate[3:5]) * 100 + int(idate[5:7]))


# 整理url參數名稱與值之函數
def CombineParam(elem):
    if (elem.get('name') is not None) and (elem.get('value') is not None):
        return str(elem.get('name')) + '=' + str(elem.get('value'))


# 產生爬蟲目標url
def MakeURL(i_param, url_param):
    if i_param:
        url_param = url_param + '&' + i_param.replace('document.t59sb01_form.', '').replace('.value', ''). \
            replace(";openWindow(this.form ,'');", '').replace('\'', '').replace(';', '&')
        target_url = 'https://mops.twse.com.tw/mops/web/ajax_t59sb01?' + url_param
        return target_url


# 設定程式執行路徑
runProgramPath = 'C:\\Users\\User\\PycharmProjects\\spider\\'
os.chdir(runProgramPath)

# 確認當前目錄是否有資料儲存資料夾 若沒有則建立
if 'material_info' not in os.listdir():
    os.mkdir((runProgramPath + 'material_info'))

# # 產生近7個實際日期
# todayDate = datetime.datetime.now()
# dateList = []
# for i in range(1):
#     iDate = todayDate - datetime.timedelta(days=i)
#     dateList.append(iDate.strftime('%Y%m%d'))

# 設定爬蟲日期區間
# 起始日
file = os.listdir((runProgramPath + 'material_info'))
if len(file) > 0:
    downloadStartDate = max(file).replace('.csv', '')
    downloadStartDate = downloadStartDate[0:4] + '-' + downloadStartDate[4:6] + '-' + downloadStartDate[6:8]
else:
    downloadStartDate = '2015-01-01'
# 結束日
downloadEndDate = datetime.datetime.now()
# 產生日期序列
dateList = pd.date_range(start=downloadStartDate, end=downloadEndDate).strftime('%Y%m%d')

# 每次只爬200個交易日
dateList = dateList[0:200]
# 計步器: 爬50個交易日後休息2小時
downloadDayNums = 0

# 迴圈日期下載重大訊息資訊資料
for iDate in dateList:

    print('目前程式正在下載日期: ' + iDate + ' 上市櫃重大訊息資料')

    # 建立儲存表
    materialInfoData = pd.DataFrame()

    # 年月日
    iYear = str(int(iDate[0:4]) - 1911)
    iMonth = iDate[4:6]
    iDay = iDate[6:8]

    # 下載公司當日重大訊息資料
    url = 'https://mops.twse.com.tw/mops/web/ajax_t05st02?' \
          'encodeURIComponent=1&step=1&step00=0&firstin=1&off=1&' \
          'TYPEK=all&year=' + iYear + '&month=' + iMonth + '&day=' + iDay

    # 防呆機制
    checkSuccess = False
    tryNums = 0
    while not checkSuccess:
        soup, checkSuccess = CheckConnect(url)
        if not checkSuccess:   # 若爬取失敗 則暫停120秒
            if tryNums == 5:   # 若已重新爬取累計5次 則放棄此次程式執行
                break
            tryNums += 1
            print('本次下載失敗 程式暫停120秒')
            time.sleep(120)

    # 防呆機制: 若累積爬取資料失敗 則終止此次程式
    if tryNums == 5:
        print('下載失敗次數累積5次 結束程式')
        break

    # 防呆機制: 若頁面出現"查無[日期]之重大訊息資料" 則進行下一個迴圈
    if '查無' in str(soup):
        print('該日期無資料 進行下一個日期資料下載')
        continue

    # 整理資料
    rowDatas = soup.find_all('table')[2].find_all('tr')
    rows = list()
    for row in rowDatas:
        rows.append([elem.get('value') for elem in row.find_all('input')])
    rows = [elem[:-1] for elem in rows if elem]
    columnNames = ['name', 'code', 'announce_date', 'time', 'subject',
                   'number', 'rule', 'actual_date', 'content']
    df = pd.DataFrame(data=rows, columns=columnNames)

    # 儲存重大訊息資訊資料
    materialInfoData = pd.concat([materialInfoData, df])
    time.sleep(5)

    # 下載DR公司當日重大訊息
    print('目前程式正在下載日期: ' + iDate + ' DR公司當日重大訊息資料')

    # 由於DR公司和一般公司的重大訊息架構不一樣 需要額外處理
    # 整理基本資訊
    rowDatas = soup.find_all('table')[3].find_all('tr')
    simpleInfoRows = list()
    for row in rowDatas:
        simpleInfoRows.append([elem.getText().replace('\xa0', '') for elem in row.find_all('td')])
    simpleInfoRows = [elem for elem in simpleInfoRows if elem]

    # 整理詳細資料資訊
    # 整理詳細資料url網址的共用參數
    urlParamRaw = soup.find_all('form')[1]
    urlParam = list()
    for i in urlParamRaw:
        urlParam.append([CombineParam(elem) for elem in urlParamRaw.find_all('input')])
    urlParam = [elem for elem in urlParam[0] if elem]
    urlParam = '&'.join(urlParam)

    # 整理各家DR公司重訊詳細資料url
    rawUrl = soup.find_all('table')[3].find_all('tr')
    urlList = list()
    for i in rawUrl:
        urlList.append([MakeURL(elem.get('onclick'), urlParam) for elem in i.find_all('input')])
    urlList = [elem for elem in urlList if elem]

    # 執行迴圈爬蟲
    for idx, iUrl in enumerate(urlList):

        # 取得DR公司重訊資訊
        url = iUrl[0]

        # 防呆機制
        checkSuccess = False
        tryNums = 0
        while not checkSuccess:
            soup2, checkSuccess = CheckConnect(url)
            if not checkSuccess:  # 若爬取失敗 則暫停120秒
                if tryNums == 5:  # 若已重新爬取累計5次 則放棄此次程式執行
                    break
                tryNums += 1
                print('本次下載失敗 程式暫停120秒')
                time.sleep(120)

        # 防呆機制: 若累積爬取資料失敗 則終止此次程式
        if tryNums == 5:
            print('下載失敗次數累積5次 結束程式')
            break

        # 整理資料
        rowDatas = soup2.find_all('table')[1].find_all('tr')
        detailInfoRows = list()
        for row in rowDatas:
            detailInfoRows.append([elem.getText() for elem in row.find_all('td')])

        iRow = [[simpleInfoRows[idx][3],                   # 股票名稱(name)
                 simpleInfoRows[idx][2],                   # 股票代碼(code)
                 ConvertDate(simpleInfoRows[idx][0]),      # 公告日期(announce_date)
                 simpleInfoRows[idx][1].replace(':', ''),  # 公告時間(time)
                 simpleInfoRows[idx][4],                   # 主旨(subject)
                 detailInfoRows[1][0],                     # 公告序號(number)
                 '',                                       # 條款(rule): 內容有提供 但因每家寫的格式不一樣很難處理 故直接以缺值取代
                 ConvertDate(detailInfoRows[2][0]),        # 事實發生日(actual_date)
                 detailInfoRows[5][0]]]                    # 內容(content)

        # 儲存資料
        df = pd.DataFrame(data=iRow, columns=columnNames)
        materialInfoData = pd.concat([materialInfoData, df])
        time.sleep(5)

    # 將本日重大訊息資料以csv檔案儲存
    saveFilePath = runProgramPath + 'material_info\\' + iDate + '.csv'
    materialInfoData.to_csv(saveFilePath, index=False)

    # 計步器: 爬50個交易日後休息2小時
    downloadDayNums += 1
    if downloadDayNums % 50 == 0:
        print('目前已爬50個交易日 程式自動休息2小時!')
        time.sleep(60*60*2)

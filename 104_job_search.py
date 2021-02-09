# 104人力銀行爬蟲程式碼
# 2021/02/08 蘇彥庭
import time
import datetime
import requests
import re
import json
import pandas as pd
from bs4 import BeautifulSoup

# 確認是否有正常連線
def CheckConnect(url, headers):
    try:
        response = requests.get(url, headers=headers)
        checkSuccess = True
        return response, checkSuccess
    except Exception as e:
        print('下載失敗!')
        response = None
        checkSuccess = False
        return response, checkSuccess


# 爬蟲參數設定
# 搜尋關鍵詞
keyword = '數據分析'
# 搜尋最大頁數
maxPage = 100

# 迴圈搜尋結果頁數
outputDf = pd.DataFrame()
for page in range(1, maxPage+1):

    # 設定header
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                      '(KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36'
    }

    # 目標網址
    url = 'https://www.104.com.tw/jobs/search/?ro=0&isnew=30&keyword=' + keyword + \
          '&expansionType=area%2Cspec%2Ccom%2Cjob%2Cwf%2Cwktm&order=14&asc=0&s9=1&page=' + \
          str(page) + '&mode=s&jobsource=2018indexpoc'

    # 網址重要參數
    # keyword: 搜尋關鍵字
    # isnew: 更新日期 / 例如: 本日最新=0 二週內=14 一個月內=30
    # s9:上班時段 / 例如: 日班=1 晚班=2
    # page: 搜尋結果第N頁
    # 此處搜尋條件設定為: 最近一個月內 日班

    # 取得網頁資料
    # 防呆機制
    checkSuccess = False
    tryNums = 0
    while not checkSuccess:
        response, checkSuccess = CheckConnect(url, headers)
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

    # 確認是否已查詢到底
    if '搜尋條件無符合工作機會' in response.text:
        print('搜尋結果已到底 無工作職缺資訊可下載 爬蟲終止!')
        break

    # 轉為soup格式
    soup = BeautifulSoup(response.text, 'html.parser')

    # 取得搜尋返回結果
    jobList = soup.select('article.b-block--top-bord')
    # 取得職缺公布時間
    jobAnnounceDate = [elem.select('span.b-tit__date')[0].text.replace('\n', '').strip() for elem in jobList]
    # 取得職缺名稱
    jobTitles = [elem.select('a.js-job-link')[0].text for elem in jobList]
    # 取得職缺公司名稱
    jobCompanyName = [elem.select('a')[1].text.replace('\n', '').strip() for elem in jobList]
    # 取得職缺公司頁面資訊連結
    jobCompanyUrl = ['https:' + elem.select('a')[1]['href'] for elem in jobList]
    # 取得職缺公司所屬產業類別
    jobCompanyIndustry = [elem.select('li')[2].text for elem in jobList]
    # 取得待遇資訊
    jobSalary = [elem.select('div.job-list-tag.b-content')[0].select('span')[0].text for elem in jobList]

    # 整理其他工作資訊(工作地點, 年資要求, 學歷要求)
    jobOtherInfo = [elem.select('ul.b-list-inline.b-clearfix.job-list-intro.b-content')[0] for elem in jobList]
    # 取得工作地點
    jobLocation = [elem.select('li')[0].text for elem in jobOtherInfo]
    # 取得年資要求
    jobRqYear = [elem.select('li')[1].text for elem in jobOtherInfo]
    # 取得學歷要求
    jobRqEducation = [elem.select('li')[2].text for elem in jobOtherInfo]

    # 取得職缺網址資訊
    jobDetailUrl = ['https:' + elem.select('a')[0]['href'] for elem in jobList]

    # 迴圈職缺網址資訊取得更詳細資訊
    jobContent = list()
    jobCategory = list()
    jobRqDepartment = list()
    jobSpecialty = list()
    jobOthers = list()
    for i, iJobDetailUrl in enumerate(jobDetailUrl):

        print('目前正在爬取第' + str(page) + '頁連結，當前頁面連結下載進度: ' + str(i+1) + ' / ' + str(len(jobDetailUrl)))

        # 詳細資訊需透過額外的ajax爬取
        iUrl = 'https://www.104.com.tw/job/ajax/content/' + re.search('job/(.*)\?', iJobDetailUrl).group(1)

        # 設定header
        headers = {
            'Referer': iJobDetailUrl,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                          '(KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36'
        }

        # 取得網頁資料
        # 防呆機制
        checkSuccess = False
        tryNums = 0
        while not checkSuccess:
            response, checkSuccess = CheckConnect(iUrl, headers)
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

        # 取得網頁資料
        response = response.json()

        # 判斷是否有error: 職務不存在
        if response.get('error'):

            jobContent.append('')
            jobCategory.append('')
            jobRqDepartment.append('')
            jobSpecialty.append('')
            jobOthers.append('')

        else:

            # 取得工作內容
            jobContent.append(response['data']['jobDetail']['jobDescription'])
            # 取得職務類別
            jobCategory.append(','.join([elem['description'] for elem in response['data']['jobDetail']['jobCategory']]))
            # 取得科系要求
            jobRqDepartment.append(','.join(response['data']['condition']['major']))
            # 取得擅長工具
            jobSpecialty.append(','.join([elem['description'] for elem in response['data']['condition']['specialty']]))
            # 取得其他條件
            jobOthers.append(response['data']['condition']['other'])

        # 暫停秒數避免爬太快
        time.sleep(3)

    # 組合資訊成資料表並儲存
    iOutputDf = pd.DataFrame({'jobAnnounceDate': jobAnnounceDate,
                              'jobTitles': jobTitles,
                              'jobCompanyName': jobCompanyName,
                              'jobCompanyUrl': jobCompanyUrl,
                              'jobCompanyIndustry': jobCompanyIndustry,
                              'jobContent': jobContent,
                              'jobCategory': jobCategory,
                              'jobSalary': jobSalary,
                              'jobLocation': jobLocation,
                              'jobRqYear': jobRqYear,
                              'jobRqEducation': jobRqEducation,
                              'jobRqDepartment': jobRqDepartment,
                              'jobSpecialty': jobSpecialty,
                              'jobOthers': jobOthers,
                              'jobDetailUrl': jobDetailUrl})
    outputDf = pd.concat([outputDf, iOutputDf])

# 加入本次搜尋資訊
outputDf.insert(0, 'maxPage', maxPage, True)
outputDf.insert(0, 'keyword', keyword, True)
now = datetime.datetime.now()
outputDf.insert(0, 'searchTime', now.strftime('%Y-%m-%d %H:%M:%S'), True)

# 刪除jobAnnounceDate為空值之列(代表該筆資料屬於104廣告職缺 與搜尋職缺較不相關)
outputDf = outputDf[outputDf.jobAnnounceDate != '']

# 輸出csv檔案
fileName = now.strftime('%Y%m%d%H%M%S') + '104人力銀行_' + keyword + '_爬蟲搜尋結果.csv'
outputDf.to_csv('104人力銀行爬蟲搜尋結果.csv', encoding='utf-8-sig')

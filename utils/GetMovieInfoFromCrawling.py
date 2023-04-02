import re
import json
import requests
import pandas as pd
from itertools import chain
from bs4 import BeautifulSoup
from datetime import datetime, timedelta 

class GetMovieInfoFromCrawling():
    # refactoring : run_time 시에 template화 해야하는 코드
    date = str(datetime.now() - timedelta(1))[:10].replace('-', '')
    
    def __init__(self,):
        movie_API_key = ""
    
    def news_crawler(df):
        news_list = []
        movieCd_list = df['movieCd'].to_list()
        movie_list = df['movieNm'].to_list()
        keyword_list = [re.sub('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]',' ', movie) for movie in movie_list]
        
        df['openDt_c'] = pd.to_datetime(df['openDt'], format = '%Y%M%d')
        df['startDt'] = df['openDt_c']- timedelta(days=15)
        df['endDt'] = df['openDt_c'] + timedelta(days=15)
        opendt_list = [df['openDt'].astype('str')[i].replace('-','') for i in range(len(df))]
        startdate_list = [df['startDt'].astype('str')[i].split(' ')[0].replace('-','') + '000000' for i in range(len(df))]
        enddate_list = [df['endDt'].astype('str')[i].split(' ')[0].replace('-','') + '235959' for i in range(len(df))]

        for i in range(len(movieCd_list)):
            movieCd = str(movieCd_list[i])
            keyword = str(keyword_list[i])
            startdate = str(startdate_list[i])
            enddate = str(enddate_list[i])
            url = f"https://search.daum.net/search?nil_suggest=btn&w=news&DA=STC&q=영화+{keyword}&p=1&period=u&sd={startdate}&ed={enddate}"
            response = requests.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text,"html.parser")
                count = soup.select_one('#resultCntArea').get_text()
                count = count.split('/ ')[1]
                count = count.replace('약 ','')
                count = count.replace('건','')
                count = count.replace(',','')
                news_list.append([movieCd, count])
            
            else:
                print(response.status_code)
                
        news_df = pd.DataFrame(news_list, columns = ["movieCd", "뉴스 언급 건수"])
        news_df['뉴스 언급 건수'] = news_df['뉴스 언급 건수'].astype(int)
        return news_df

def star_crawler(df):
    star_list = []
    
    movieCd_list = df['movieCd'].to_list()
    movie_list = df['movieNm'].to_list()
    keyword_list = [re.sub('[-=+,#/\?:^$.@*\"※~&%ㆍ!』\\‘|\(\)\[\]\<\>`\'…》]',' ', movie) for movie in movie_list]
    
    df['openDt_c'] = pd.to_datetime(df['openDt'], format = '%Y%M%d')
    df['startDt'] = df['openDt_c']- timedelta(days=15)
    df['endDt'] = df['openDt_c'] + timedelta(days=15)
    
    for i in range(len(df)):
        movieCd = str(movieCd_list[i])
        keyword = str(keyword_list[i])
        url = f"https://movie.naver.com/movie/search/result.naver?query={keyword}&section=all&ie=utf8"
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text,"html.parser")
            star =  soup.select('#old_content > ul.search_list_1 > li > dl > dd.point > em.num')
            if(len(star)>0):
                star = star[0].text
                star_list.append([movieCd, star])
            else: 
                star_list.append([movieCd, 0])
                    
        
        else:
            print(response.status_code)
                
        star_df = pd.DataFrame(star_list, columns = ["movieCd", "평점"])
        star_df['평점'] = star_df['평점'].astype(float)
        return star_df

    def extract_value(trial):
        # 장르, 등급, 감독, 국적 추출
        genre = []
        audit = []
        director = []
        nation = []
        for i in range(len(trial)):
            try :
                genre.append(trial.genres[i][0]['genreNm'])
            except :
                genre.append('')
            try : 
                audit.append(trial.audits[i][0]['watchGradeNm'])
            except :
                audit.append('')
            try :
                director.append(trial.directors[i][0]['peopleNm'])
            except :
                director.append('')
            try:
                nation.append(trial.nations[i][0]['nationNm'])
            except :
                nation.append('') 

        trial['장르'] = genre
        trial['등급'] = audit
        trial['감독'] = director
        trial['국적'] = nation

        #배급사 가져오기
        distributor = []
        for i in range(len(trial)):
            row = trial['companys'][i]
            for j in range(len(row)):
                if row[j]['companyPartNm'] == '배급사':
                    distributor.append(row[j]['companyNm'])
                    break
        trial['배급사'] = distributor
        return(trial)
    

    def pre_nation(df):
        df.loc[(df['국적'] != '한국')*(df['국적']!='미국'), '국적'] = '한국미국제외'
        return df


    def upload_file_s3(bucket,filename, content):
        client = boto3.client('s3')
        try :
            client.put_object(Bucket=bucket, Key=filename, Body=content)
            return True
        except :
            return False
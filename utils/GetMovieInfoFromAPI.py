import re
import json
import requests
import pandas as pd
from itertools import chain
from bs4 import BeautifulSoup
from datetime import datetime, timedelta 

class GetMovieInfoFromAPI():
    # refactoring : run_time 시에 template화 해야하는 코드
    date = str(datetime.now() - timedelta(1))[:10].replace('-', '')
    
    def __init__(self,):
        movie_API_key = ""
    
    def get_Top10_movie_info_df() :
        date = str(datetime.now() - timedelta(1))[:10].replace('-', '')
        url = 'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key=5d450765c401dac01475905006e44d99&targetDt='+date
        res = requests.get(url); text= res.text
        movie_raw = json.loads(text)
        df = pd.DataFrame()
        
        name_list = []; open_date_list = []; movie_cd_list = []
        
        for movie_info in movie_raw['boxOfficeResult']['dailyBoxOfficeList']:
            name_list.append(movie_info['movieNm'])
            open_date_list.append(movie_info['openDt'])
            movie_cd_list.append(movie_info['movieCd']) 
        
        df['movieNm'] = name_list
        df['openDt'] = open_date_list
        df['movieCd'] = movie_cd_list      
        return df
    
    def movie_info(df):
        movie_url = lambda x : 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key=dddea6429a20616861ab4a73b4618dc3&movieCd=' + str(x)
        moviecd_list = df['movieCd']
        movie_jsons = (requests.get(movie_url(moviecd)).json() for moviecd in moviecd_list)
        movie_json = chain(movie_json['movieInfoResult']['movieInfo'] for movie_json in movie_jsons)
        df_info = pd.json_normalize(movie_json)
        return df_info
    
    def extract_TOP_10(self,):
        
        pass

    def extract_specific_data(self,):
        pass
    
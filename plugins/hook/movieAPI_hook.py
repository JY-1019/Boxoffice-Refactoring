import json
import requests
import pandas as pd
from itertools import chain
from datetime import datetime, timedelta 
from airflow.hooks.base_hook import BaseHook


# define the class inheriting from an existing hook class
class MovieAPIHook(BaseHook):

    def __init__(self, conn_key):
        super.__init__()
        self._conn_key = conn_key
    
    def get_movie_raw_data(self, ) :
        date = str(datetime.now() - timedelta(1))[:10].replace('-', '')
        url = f'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={self._conn_key}targetDt={date}'
        res = requests.get(url); text= res.text
        movie_raw = json.loads(text)
        return movie_raw

    def get_movie_info_df(self,) :
        df = self.Top10_movie_info_df()
        df_info = self.movie_info(df)
        return df_info

    def Top10_movie_info_df(self,) :
        df = pd.DataFrame()
        movie_raw = self.get_movie_raw_data()
        
        name_list = []; open_date_list = []; movie_cd_list = []
        
        for movie_info in movie_raw['boxOfficeResult']['dailyBoxOfficeList']:
            name_list.append(movie_info['movieNm'])
            open_date_list.append(movie_info['openDt'])
            movie_cd_list.append(movie_info['movieCd']) 
        
        df['movieNm'] = name_list
        df['openDt'] = open_date_list
        df['movieCd'] = movie_cd_list      
        return df
    
    def movie_info(self, df):
        movie_url = lambda x : f'http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={self._conn_key}&movieCd=' + str(x)
        moviecd_list = df['movieCd']
        movie_jsons = (requests.get(movie_url(moviecd)).json() for moviecd in moviecd_list)
        movie_json = chain(movie_json['movieInfoResult']['movieInfo'] for movie_json in movie_jsons)
        df_info = pd.json_normalize(movie_json)
        return df_info
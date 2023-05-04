import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import re
import boto3
import requests
import datetime
import pandas as pd
from utils import *
from bs4 import BeautifulSoup
from airflow.models import BaseOperator
from hook.movieAPI_hook import MovieAPIHook
from airflow.utils.decorators import apply_defaults


class GetMovieReview(BaseOperator):

    @apply_defaults
    def __init__(self,**kwargs):
        super.__init__(self, **kwargs)

    def execute(self, context) : 
        bucket = 'boxoffice-bucket'
        date = int(str(datetime.datetime.now() - datetime.timedelta(1))[:10].replace('-', ''))
        file_name = f'review-{date}'
        movie_api_key = get_API_key("MOVIE_API_KEY")         
        hook = MovieAPIHook(movie_api_key)
        result = hook.get_movie_raw_data()
        movie_titles = []
        for movie_info in result['boxOfficeResult']['dailyBoxOfficeList']:
            movie_titles.append(movie_info['movieNm'])
        
        review_urls = []
        for movie_title in movie_titles:
            review_urls.append(self.get_review_url(movie_title))
        
        result = pd.DataFrame(columns=['제목', '리뷰', '별점'])
        for i in range(10):
            movie_title = movie_titles[i]
            review_url = review_urls[i]
            result = pd.concat([result, self.crawl_review(movie_title, review_url, date)])
            
        result = result.to_csv()
        success = upload_file_s3(bucket, 'review/'+file_name + '.csv', result)

    @staticmethod
    def no_space(text):
        text1 = re.sub('&nbsp;|&nbsp;|\n|\t|\r', '', text)
        text2 = re.sub('\n\n', '', text1)
        return text2

    def get_review_url(self, movie_title):
        url = f'https://movie.naver.com/movie/search/result.naver?query={movie_title}&section=all&ie=utf8'
        res = requests.get(url)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            user_dic = {}
            idx = 1
            for href in soup.find("ul", class_="search_list_1").find_all("li"): 
                user_dic[idx] = int(href.dl.dt.a['href'][30:])
                idx += 1
        
            movie_code = user_dic[1]
            order_key = 'newest'  # 공감순: order=sympathyScore, 최신순: order=newest
            url = f'https://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?code={movie_code}&type=after&onlyActualPointYn=N&onlySpoilerPointYn=N&order={order_key}&page='
            return url
        
        print(f'Request Error: Code {res.status_code}')
        return ''


    def crawl_review(self, movie_title, base_url, target_date):
        print(f'Start to crawl {movie_title}.')
        
        res = requests.get(base_url)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text,'html.parser')
            try:
                total_review = int(soup.select('div.score_total > strong > em')[0].text.replace(',', ''))
                total_page = (total_review + 9) // 10
                total_reviews = []
                total_stars = []
                page = 0
                flag = False
                while (not flag) and (page <= total_page):
                    page += 1
                    
                    url = base_url + str(page)
                    res = requests.get(url)
                    if res.status_code == 200:
                        soup = BeautifulSoup(res.text, 'html.parser')
                        dates = soup.select('div.score_reple > dl > dt > em:nth-child(2)')
                        if int(dates[-1].text.split()[0].replace('.', '')) > target_date:
                            continue
                        
                        stars = soup.select('div.star_score > em')
                        reviews = soup.select('div.score_reple > p > span')
                        reviews = [*filter(lambda x: x.text not in ['관람객', '스포일러가 포함된 감상평입니다. 감상평 보기'], reviews)]
                        
                        for date, star, review in zip(dates, stars, reviews):
                            date = int(date.text.split()[0].replace('.', ''))
                            if date < target_date:
                                flag = True
                                break
                            
                            if date == target_date:
                                total_stars.append(int(star.text))
                                total_reviews.append(GetMovieReview.no_space(review.text))
                
                assert len(total_reviews) == len(total_stars)
                df = pd.DataFrame({"제목": movie_title, "리뷰": total_reviews, "별점": total_stars})
            
            except:
                print(f'<{movie_title}>의 리뷰가 아직 등록되지 않았습니다.')
                df = pd.DataFrame([[movie_title, '아직 리뷰가 등록되지 않았습니다', None]], columns=['제목', '리뷰', '별점'])
            
            return df
        
        print(f'Request Error: Code {res.status_code}')
        return pd.DataFrame(columns=['제목', '리뷰', '별점'])
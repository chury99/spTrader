import os
import sys
import pandas as pd
import json
import re

import pandas.errors
from tqdm import tqdm

import analyzer_알고리즘 as Logic


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Analyzer:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_analyzer"]}_{self.s_오늘}.log')

        # 폴더 정의
        import UT_폴더정보
        dic_폴더정보 = UT_폴더정보.dic_폴더정보
        self.folder_ohlcv = dic_폴더정보['데이터|ohlcv']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_감시대상 = dic_폴더정보['분석1종목|감시대상']
        self.folder_감시대상모델 = dic_폴더정보['분석1종목|모델_감시대상']
        # os.makedirs(self.folder_감시대상모델, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        self.li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                               if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명])

        # log 기록
        self.make_log(f'### 공통 분석 시작 ###')

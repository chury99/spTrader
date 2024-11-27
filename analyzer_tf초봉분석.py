import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import UT_차트maker as Chart
import analyzer_tf알고리즘 as Logic

# 그래프 한글 설정
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames,PyUnusedLocal
class Analyzer:
    def __init__(self, n_분석일수=None):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_analyzer"]}_{self.s_오늘}.log')

        # 폴더 정의
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_대상종목 = dic_폴더정보['이력|대상종목']
        self.folder_지표생성 = dic_폴더정보['tf초봉분석|10_지표생성']
        os.makedirs(self.folder_지표생성, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수
        self.li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                        if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 분석_지표생성(self, b_차트, n_초봉):
        """ trader 동작 시 생성된 대상종목 기준으로 초봉 데이터에 지표 추가 생성 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_대상종목'
        s_파일명_생성 = 'dic_지표생성'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_대상종목)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지표생성)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{s_일자}.pkl'))
            dic_종목코드2종목명 = df_대상종목.set_index('종목코드').to_dict()['종목명']
            dic_종목코드2추가시점 = df_대상종목.set_index('종목코드').to_dict()['추가시점']

            # 초봉 불러오기
            try:
                dic_초봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_{n_초봉}초봉_{s_일자}.pkl'))
            except FileNotFoundError:
                continue

            # 전체 시간 생성
            # li_전체시간 = list(pd.concat([dic_초봉[코드]['체결시간'] for 코드 in dic_초봉.keys()]).sort_values().unique())

            # 종목별 분석 진행
            dic_지표생성 = dict()
            for s_종목코드 in tqdm(df_대상종목['종목코드'], desc=f'지표생성-{n_초봉}초봉-{s_일자}'):
                # 기준정보 생성
                s_종목명 = dic_종목코드2종목명[s_종목코드]
                s_추가시점 = dic_종목코드2추가시점[s_종목코드]

                # 종목명 추가
                df_초봉 = dic_초봉[s_종목코드].sort_index()
                li_컬럼명 = list(df_초봉.columns)
                df_초봉['종목명'] = s_종목명
                df_초봉 = df_초봉.loc[:, ['종목코드', '종목명'] + li_컬럼명[1:]]

                # 추가정보 생성
                df_초봉 = df_초봉[df_초봉['체결시간'] >= s_추가시점].copy()
                df_초봉['체결강도'] = (df_초봉['매수량'] / df_초봉['매도량'] * 100).apply(lambda x:
                                                                                99999 if x == float('inf') else x)
                df_초봉['z_매수'] = df_초봉['매수량'].rolling(30).apply(lambda x: Logic.cal_z스코어(x))
                df_초봉['z_매도'] = df_초봉['매도량'].rolling(30).apply(lambda x: Logic.cal_z스코어(x))
                df_초봉['z_거래'] = df_초봉['거래량'].rolling(30).apply(lambda x: Logic.cal_z스코어(x))
                df_초봉['만원_매수'] = df_초봉['종가'] * df_초봉['매수량'] / 10000
                df_초봉['만원_매도'] = df_초봉['종가'] * df_초봉['매도량'] / 10000
                df_초봉['만원_거래'] = df_초봉['종가'] * df_초봉['거래량'] / 10000
                df_초봉['추가시점'] = s_추가시점

                # 미존재 데이터 처리
                for s_컬러명 in ['거래량', '매수량', '매도량', '체결강도', 'z_매수', 'z_매도', 'z_거래',
                              '만원_매수', '만원_매도', '만원_거래']:
                    df_초봉.loc[df_초봉['종가'].isnull(), s_컬러명] = None

                # dic 추가
                dic_지표생성[s_종목코드] = df_초봉

                # csv 저장
                s_폴더 = os.path.join(self.folder_지표생성, f'종목별_{s_일자}')
                os.makedirs(s_폴더, exist_ok=True)
                df_초봉.to_csv(os.path.join(s_폴더, f'df_지표생성_{s_일자}_{n_초봉}초봉_{s_종목코드}.csv'),
                              index=False, encoding='cp949')

            # dic 저장
            pd.to_pickle(dic_지표생성, os.path.join(self.folder_지표생성, f'dic_지표생성_{s_일자}_{n_초봉}초봉.pkl'))

            # log 기록
            self.make_log(f'지표생성 완료({s_일자}, {n_초봉}초봉, {len(dic_지표생성):,}개 종목)')

    ###################################################################################################################
    def make_log(self, s_text, li_loc=None):
        """ 입력 받은 s_text에 시간 붙여서 self.path_log에 저장 """
        # 정보 설정
        s_시각 = pd.Timestamp('now').strftime('%H:%M:%S')
        s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
        s_모듈 = sys._getframe(1).f_code.co_name

        # log 생성
        s_log = f'[{s_시각}] {s_파일} | {s_모듈} | {s_text}'

        # log 출력
        li_출력 = ['콘솔', '파일'] if li_loc is None else li_loc
        if '콘솔' in li_출력:
            print(s_log)
        if '파일' in li_출력:
            with open(self.path_log, mode='at', encoding='cp949') as file:
                file.write(f'{s_log}\n')


#######################################################################################################################
if __name__ == "__main__":
    a = Analyzer(n_분석일수=None)
    li_초봉 = [1, 2, 3, 5, 10]
    for n_초봉 in li_초봉:
        a.분석_지표생성(b_차트=True, n_초봉=n_초봉)

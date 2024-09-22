import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import UT_차트maker as Chart
import analyzer_sr알고리즘 as Logic
from analyzer_sr백테스팅 import Analyzer as Backtest


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
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
        self.folder_분석대상 = dic_폴더정보['데이터|분석대상']
        self.folder_일봉변동 = dic_폴더정보['sr종목선정|10_일봉변동']
        self.folder_지지저항 = dic_폴더정보['sr종목선정|20_지지저항']
        self.folder_매수신호 = dic_폴더정보['sr종목선정|30_매수신호']
        self.folder_매도신호 = dic_폴더정보['sr종목선정|40_매도신호']
        self.folder_종목선정 = dic_폴더정보['sr종목선정|50_종목선정']
        os.makedirs(self.folder_일봉변동, exist_ok=True)
        os.makedirs(self.folder_지지저항, exist_ok=True)
        os.makedirs(self.folder_매수신호, exist_ok=True)
        os.makedirs(self.folder_매도신호, exist_ok=True)
        os.makedirs(self.folder_종목선정, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수
        self.li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                        if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]

        # log 기록
        self.make_log(f'### 종목 선정 시작 ###')

    def 분석_일봉변동(self):
        """ 분석대상종목 기준으로 일봉 분석해서 거래량 변동 발생 종목 선정 후 pkl, csv 저장 \n
                    # 선정기준 : 최근 20일 z-score +3 초과 & 거래대금 100억 초과 되는 종목 """
        # 파일명 정의
        s_파일명_생성 = 'df_일봉변동'

        # 분석대상 일자 선정
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(days=self.n_보관기간_analyzer)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        li_일자_전체 = sorted([일자 for 일자 in self.li_전체일자 if 일자 > s_기준일자])
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_일봉변동)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 일봉 데이터 불러오기 (from 캐시변환, 20일분)
            s_금월 = s_일자[:6]
            s_전월 = (pd.Timestamp(s_일자) - pd.DateOffset(months=1)).strftime('%Y%m')
            dic_일봉_금월 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_금월}.pkl'))
            dic_일봉_전월 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_전월}.pkl'))

            # 종목별 거래량 변동 확인
            li_df_일봉변동 = list()
            for s_종목코드 in tqdm(dic_일봉_금월.keys(), desc=f'일봉 변동 분석|{s_일자}'):
                # 분석대상 종목 확인 (키움 조건식 연계) => dic_분석대상 쌓기 시작한 지 얼마 안돼서 과거 데이터 예외처리 적용
                try:
                    dic_조건검색 = pd.read_pickle(os.path.join(self.folder_분석대상, f'dic_조건검색_{s_일자}.pkl'))
                    df_분석대상종목 = dic_조건검색['분석대상종목']
                    if s_종목코드 not in df_분석대상종목['종목코드'].values:
                        continue
                except FileNotFoundError:
                    pass

                # 일봉 불러오기
                li_df_일봉 = [dic_일봉_금월[s_종목코드], dic_일봉_전월[s_종목코드] if s_종목코드 in dic_일봉_전월.keys()
                else pd.DataFrame()]
                df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자').reset_index(drop=True)
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]

                # 일봉변동 확인
                df_일봉변동_종목 = Logic.find_일봉변동_거래량(df_일봉=df_일봉, n_윈도우=20, n_z값=3)

                # 종목 결과 list 입력
                li_df_일봉변동.append(df_일봉변동_종목)

            # 일봉 변동 종목 df 생성
            df_일봉변동 = pd.concat(li_df_일봉변동, axis=0).reset_index(drop=True)

            # df 저장
            df_일봉변동.to_pickle(os.path.join(self.folder_일봉변동, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_일봉변동.to_csv(os.path.join(self.folder_일봉변동, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'일봉 변동 분석 완료({s_일자}, {len(df_일봉변동):,}종목)')

    def 분석_지지저항(self, b_차트):
        """ 일봉변동 분석 결과 선정된 종목 대상으로 3분봉 SRline 산출 후  pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_일봉변동'
        s_파일명_생성 = 'df_지지저항'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_일봉변동)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지지저항)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_일봉변동 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 일봉 불러오기 (1년치)
            li_대상월 = [re.findall(r'\d{6}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                      if 'dic_코드별_일봉_' in 파일명 and '.pkl' in 파일명]
            li_대상월 = sorted([대상월 for 대상월 in li_대상월 if 대상월 <= s_일자[:6]])[-13:]
            dic_일봉_대상월 = dict()
            for s_대상월 in li_대상월:
                dic_일봉_대상월[s_대상월] = pd.read_pickle(os.path.join(self.folder_캐시변환,
                                                                 f'dic_코드별_일봉_{s_대상월}.pkl'))

            # 3분봉 읽어오기 (10일치)
            li_대상일 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                      if 'dic_코드별_3분봉_' in 파일명 and '.pkl' in 파일명]
            li_대상일 = sorted([대상일 for 대상일 in li_대상일 if 대상일 <= s_일자])[-10:]
            dic_3분봉_대상일 = dict()
            for s_대상일 in li_대상일:
                dic_3분봉_대상일[s_대상일] = pd.read_pickle(os.path.join(self.folder_캐시변환,
                                                                 f'dic_코드별_3분봉_{s_대상일}.pkl'))

            # 종목별 지지저항 찾기
            li_df_지지저항 = list()
            for s_종목코드 in tqdm(df_일봉변동['종목코드'].values, desc=f'종목별 지지저항 분석|{s_일자}'):
                # 일봉 정리
                li_df_일봉 = [dic_일봉_대상월[s_대상월][s_종목코드] if s_종목코드 in dic_일봉_대상월[s_대상월].keys()
                             else pd.DataFrame() for s_대상월 in li_대상월]
                df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자')

                # 3분봉 정리
                li_df_3분봉 = [dic_3분봉_대상일[s_대상일][s_종목코드] if s_종목코드 in dic_3분봉_대상일[s_대상일].keys()
                             else pd.DataFrame() for s_대상일 in li_대상일]
                df_3분봉 = pd.concat(li_df_3분봉, axis=0).sort_values(['일자', '시간'])

                # 지지저항 값 생성 (거래량 기준 + 피크값 기준), 분봉 + 일봉
                li_df_지지저항_종목 = list()
                li_df_지지저항_종목.append(Logic.find_지지저항_거래량(df_ohlcv=df_3분봉, n_윈도우=20))
                li_df_지지저항_종목.append(Logic.find_지지저항_피크값(df_ohlcv=df_3분봉))
                li_df_지지저항_종목.append(Logic.find_지지저항_거래량(df_ohlcv=df_일봉, n_윈도우=20))
                li_df_지지저항_종목.append(Logic.find_지지저항_피크값(df_ohlcv=df_일봉))

                # 지지저항 값 통합
                df_지지저항_종목 = pd.concat(li_df_지지저항_종목, axis=0)
                df_지지저항_종목 = Logic.find_지지저항_라인통합(df_지지저항=df_지지저항_종목, n_퍼센트범위=5)
                li_df_지지저항.append(df_지지저항_종목)

                # 차트 생성 및 저장
                if b_차트:
                    # 차트 생성
                    fig = chart.make_차트(df_ohlcv=df_3분봉, n_봉수=128 * 2)
                    for n_지지저항 in df_지지저항_종목['고가'].values:
                        fig.axes[0].axhline(n_지지저항)

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_지지저항, '그래프', f'지지저항_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'지지저항_{s_종목코드}_{s_일자}.png'))

            # df_지지저항 생성
            df_지지저항 = pd.concat(li_df_지지저항, axis=0)

            # df 저장
            df_지지저항.to_pickle(os.path.join(self.folder_지지저항, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_지지저항.to_csv(os.path.join(self.folder_지지저항, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'지지 저항 분석 완료({s_일자}, {len(df_지지저항["종목코드"].unique()):,}종목)')

    def 분석_매수신호(self, b_차트):
        """ 산출된 지지저항 값 기준으로 매수신호 분석 후 결과 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_지지저항'
        s_파일명_생성 = 'df_매수신호'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지지저항)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수신호)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 날짜 확인
            s_일자_당일 = s_일자
            s_일자_전일 = max(일자 for 일자 in self.li_전체일자 if 일자 < s_일자_당일)

            # 종목선정, 지지저항 불러오기 (당일)
            try:
                df_일봉변동 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'df_일봉변동_{s_일자_당일}.pkl'))
                li_대상종목 = list(df_일봉변동['종목코드'].sort_values().unique())
                df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자_당일}.pkl'))
            except FileNotFoundError:
                continue

            # 3분봉 불러오기 (당일)
            dic_3분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_전일}.pkl'))
            dic_3분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_당일}.pkl'))

            # 종목별 확인
            li_df_매수신호 = list()
            li_df_매수신호_상세 = list()
            for s_종목코드 in tqdm(li_대상종목, desc=f'종목별 매수신호 분석|{s_일자}'):
                # 데이터 준비
                try:
                    df_3분봉_당일 = dic_3분봉_당일[s_종목코드]
                except KeyError:
                    continue
                df_3분봉_전일 = dic_3분봉_전일[s_종목코드] if s_종목코드 in dic_3분봉_전일.keys() else pd.DataFrame()
                df_3분봉 = pd.concat([df_3분봉_전일, df_3분봉_당일], axis=0).sort_values(['일자', '시간'])
                df_지지저항_종목 = df_지지저항[df_지지저항['종목코드'] == s_종목코드]

                # 매수신호 탐색
                ret_매수검증 = Backtest.make_매수매도_매수검증(df_3분봉=df_3분봉, df_지지저항_전일=df_지지저항_종목)
                df_매수신호_종목, df_매수신호_상세_종목 = ret_매수검증

                # li_df 추가
                li_df_매수신호.append(df_매수신호_종목)
                li_df_매수신호_상세.append(df_매수신호_상세_종목)

                # 차트 생성 및 저장
                if b_차트:
                    # 데이터 미존재 시 차트 skip
                    if len(df_매수신호_상세_종목) == 0:
                        continue

                    # 차트 생성
                    fig = Chart.make_차트(df_ohlcv=df_매수신호_상세_종목)
                    for n_지지저항 in df_매수신호_상세_종목['지지저항'].values[-1]:
                        fig.axes[0].axhline(n_지지저항)

                    # 매수신호 표시 (매수는 ^, 매도는 v)
                    df_매수 = df_매수신호_상세_종목[df_매수신호_상세_종목['매수신호']]
                    fig.axes[0].scatter(df_매수['일시'], df_매수['시가'], color='black', marker='^')

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_매수신호, '그래프', f'매수신호_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'매수신호_{s_종목코드}_{s_일자}.png'))

            # df 생성
            df_매수신호 = pd.concat(li_df_매수신호, axis=0)
            df_매수신호_상세 = pd.concat(li_df_매수신호_상세, axis=0)

            # df 저장
            df_매수신호.to_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매수신호.to_csv(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')
            df_매수신호_상세.to_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}_상세.pkl'))
            df_매수신호_상세.to_csv(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}_상세.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매수신호 분석 완료({s_일자}, {len(df_매수신호["종목코드"].unique()):,}종목)')

    def 분석_매도신호(self, b_차트):
        """ 매수신호 기준으로 매도신호 분석 후 결과 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_매수신호'
        s_파일명_생성 = 'df_매도신호'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수신호)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도신호)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 날짜 확인
            s_일자_당일 = s_일자
            s_일자_전일 = max(일자 for 일자 in self.li_전체일자 if 일자 < s_일자_당일)

            # 종목선정, 지지저항 불러오기 (당일)
            try:
                df_일봉변동 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'df_일봉변동_{s_일자_당일}.pkl'))
                li_대상종목 = list(df_일봉변동['종목코드'].sort_values().unique())
                df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자_당일}.pkl'))
            except FileNotFoundError:
                continue

            # 3분봉, 1분봉 불러오기 (당일)
            dic_1분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자_당일}.pkl'))
            dic_3분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_전일}.pkl'))
            dic_3분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_당일}.pkl'))

            # 매수신호 불러오기
            df_매수신호 = pd.read_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_기준}_{s_일자}.pkl'))
            df_매수신호_상세 = pd.read_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_기준}_{s_일자}_상세.pkl'))

            # 종목별 확인
            li_df_매도신호 = list()
            li_df_매도신호_상세 = list()
            for s_종목코드 in tqdm(df_매수신호['종목코드'].unique(), desc=f'종목별 매도신호 분석|{s_일자}'):
                # 데이터 준비
                try:
                    df_3분봉_당일 = dic_3분봉_당일[s_종목코드]
                except KeyError:
                    continue
                df_3분봉_전일 = dic_3분봉_전일[s_종목코드] if s_종목코드 in dic_3분봉_전일.keys() else pd.DataFrame()
                df_3분봉 = pd.concat([df_3분봉_전일, df_3분봉_당일], axis=0).sort_values(['일자', '시간'])
                df_1분봉 = dic_1분봉[s_종목코드]
                df_지지저항_종목 = df_지지저항[df_지지저항['종목코드'] == s_종목코드]
                df_매수신호_종목 = df_매수신호[df_매수신호['종목코드'] == s_종목코드]

                # 매도신호 탐색
                ret_매도검증 = Backtest.make_매수매도_매도검증(df_매수신호=df_매수신호_종목, df_1분봉=df_1분봉, df_3분봉=df_3분봉)
                df_매수매도_종목, df_매수매도_상세_종목 = ret_매도검증

                # 매도 전 매수 케이스 제거
                df_매수매도_종목 = Backtest.make_매수매도_중복거래제거(df_매수매도=df_매수매도_종목)

                # li_df 추가
                li_df_매도신호.append(df_매수매도_종목)
                li_df_매도신호_상세.append(df_매수매도_상세_종목)

                # 차트 생성 및 저장
                if b_차트:
                    # 데이터 미존재 시 차트 skip
                    if len(df_1분봉) == 0:
                        continue

                    # 차트 생성
                    fig = Chart.make_차트(df_ohlcv=df_1분봉)
                    for n_지지저항 in df_매수매도_상세_종목['지지저항'].values[-1]:
                        fig.axes[0].axhline(n_지지저항)

                    # 매수매도 표시 (매수는 ^, 매도는 v)
                    df_차트 = df_매수매도_종목.copy()
                    df_차트['매수일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매수시간']
                    df_차트['매도일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매도시간']
                    fig.axes[0].scatter(df_차트['매수일시'], df_차트['매수단가'], color='black', marker='^')
                    fig.axes[0].scatter(df_차트['매도일시'], df_차트['매도단가'], color='black', marker='v')

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_매도신호, '그래프', f'매도신호_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'매도신호_{s_종목코드}_{s_일자}.png'))

            # df 생성
            df_매도신호 = pd.concat(li_df_매도신호, axis=0)
            df_매도신호_상세 = pd.concat(li_df_매도신호_상세, axis=0)

            # 매도 전 매수 케이스 제거
            df_매도신호 = Backtest.make_매수매도_중복거래제거(df_매수매도=df_매도신호)

            # df 저장
            df_매도신호.to_pickle(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매도신호.to_csv(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')
            df_매도신호_상세.to_pickle(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}_상세.pkl'))
            df_매도신호_상세.to_csv(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}_상세.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매도신호 분석 완료({s_일자}, {len(df_매도신호):,}건)')

    def 분석_종목선정(self, b_차트):
        """ 매도신호 기준으로 적합성 검증 후 대상 종목 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_매도신호'
        s_파일명_생성 = 'df_종목선정'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도신호)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목선정)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and '상세' not in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 데이터 불러오기
            df_매도신호_전체 = pd.read_pickle(os.path.join(self.folder_매도신호, f'{s_파일명_기준}_{s_일자}.pkl'))
            dic_1분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자}.pkl'))
            df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자}.pkl'))

            # 종목별 확인
            li_df_종목선정 = list()
            for s_종목코드 in tqdm(df_매도신호_전체['종목코드'].unique(), desc=f'종목별 종목선정 분석|{s_일자}'):
                # 데이터 정리
                df_매도신호 = df_매도신호_전체[df_매도신호_전체['종목코드'] == s_종목코드].sort_values('매수시간')
                df_1분봉 = dic_1분봉[s_종목코드].copy().sort_values(['일자', '시간']).reset_index(drop=True)
                li_지지저항 = list(df_지지저항[df_지지저항['종목코드'] == s_종목코드]['고가'].values)

                # 평가 지표 산출
                n_전체매매 = len(df_매도신호)
                n_수익건수 = len(df_매도신호[df_매도신호['수익률(%)'] > 0.3])
                n_손실건수 = n_전체매매 - n_수익건수
                n_성공률_퍼센트 = n_수익건수 / n_전체매매 * 100

                # 종목 선정 (미충족 시 이후 skip)
                # if n_성공률_퍼센트 < 90:
                #     continue

                # 선정된 종목 추가
                li_df_종목선정.append(df_매도신호)

                # 차트 생성 및 저장
                if b_차트:
                    # 데이터 미존재 시 차트 skip
                    if len(df_1분봉) == 0:
                        continue

                    # 차트 생성
                    fig = Chart.make_차트(df_ohlcv=df_1분봉)
                    for n_지지저항 in li_지지저항:
                        fig.axes[0].axhline(n_지지저항)

                    # 매수매도 표시 (매수는 ^, 매도는 v)
                    df_차트 = df_매도신호.copy()
                    df_차트['매수일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매수시간']
                    df_차트['매도일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매도시간']
                    fig.axes[0].scatter(df_차트['매수일시'], df_차트['매수단가'], color='black', marker='^')
                    fig.axes[0].scatter(df_차트['매도일시'], df_차트['매도단가'], color='black', marker='v')

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_종목선정, '그래프', f'종목선정_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'종목선정_{s_종목코드}_{s_일자}.png'))

            # df_종목선정 생성
            if len(li_df_종목선정) > 0:
                df_종목선정 = pd.concat(li_df_종목선정, axis=0)
            else:
                s_최근일자 = max(re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도신호)
                             if s_파일명_기준 in 파일명 and '.pkl' in 파일명)
                df_종목선정 = pd.read_pickle(os.path.join(self.folder_종목선정, f'{s_파일명_생성}_{s_최근일자}.pkl'))
                df_종목선정 = df_종목선정[:0]

            # 누적 수익률 산출
            df_종목선정 = df_종목선정.loc[:, '일자': '수익률(%)']
            df_종목선정 = df_종목선정.sort_values('매수시간')
            df_종목선정['누적수익(%)'] = (1 + df_종목선정['수익률(%)'] / 100).cumprod() * 100

            # df 저장
            df_종목선정.to_pickle(os.path.join(self.folder_종목선정, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_종목선정.to_csv(os.path.join(self.folder_종목선정, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목선정 분석 완료({s_일자}, {len(df_종목선정["종목코드"].unique()):,}종목 선정)')

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

    a.분석_일봉변동()
    a.분석_지지저항(b_차트=False)
    a.분석_매수신호(b_차트=False)
    a.분석_매도신호(b_차트=False)
    a.분석_종목선정(b_차트=False)

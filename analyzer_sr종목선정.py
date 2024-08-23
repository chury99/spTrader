import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import analyzer_sr알고리즘 as Logic


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

        li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                           if 'dic_코드별_분봉_' in 파일명 and '.pkl' in 파일명])
        li_일자_전체 = [일자 for 일자 in li_일자_전체 if 일자 > s_기준일자]
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

    def 분석_지지저항(self):
        """ 일봉변동 분석 결과 선정된 종목 대상으로 3분봉 SRline 산출 후  pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_일봉변동'
        s_파일명_생성 = 'df_지지저항'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_일봉변동)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지지저항)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_일봉변동 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 3분봉 읽어오기 (5일치)
            li_대상일 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_3분봉_' in 파일명 and '.pkl' in 파일명]
            li_대상일 = sorted([대상일 for 대상일 in li_대상일 if 대상일 <= s_일자])
            li_대상일 = li_대상일[-5:]
            dic_3분봉_대상일 = dict()
            for s_대상일 in li_대상일:
                dic_3분봉_대상일[s_대상일] = pd.read_pickle(os.path.join(self.folder_캐시변환,
                                                                 f'dic_코드별_3분봉_{s_대상일}.pkl'))

            # 종목별 지지저항 찾기
            li_df_지지저항 = list()
            for s_종목코드 in tqdm(df_일봉변동['종목코드'].values, desc=f'종목별 지지저항 분석|{s_일자}'):
                # 3분봉 정리
                li_df_3분봉 = [dic_3분봉_대상일[s_대상일][s_종목코드] if s_종목코드 in dic_3분봉_대상일[s_대상일].keys()
                             else pd.DataFrame() for s_대상일 in li_대상일]
                df_3분봉 = pd.concat(li_df_3분봉, axis=0).sort_index()

                # 추가 데이터 생성
                import UT_차트maker as chart
                df_3분봉 = chart.find_전일종가(df_ohlcv=df_3분봉)
                df_3분봉 = chart.make_이동평균(df_ohlcv=df_3분봉)

                # 지지저항 값 생성 (거래량 기준 + 피크값 기준)
                df_지지저항_종목_거래량 = Logic.find_지지저항_거래량(df_ohlcv=df_3분봉, n_윈도우=120)
                df_지지저항_종목_피크값 = Logic.find_지지저항_피크값(df_ohlcv=df_3분봉, n_피크선명도=100)
                df_지지저항_종목 = pd.concat([df_지지저항_종목_거래량, df_지지저항_종목_피크값], axis=0)
                df_지지저항_종목 = Logic.find_지지저항_라인통합(df_지지저항=df_지지저항_종목, n_퍼센트범위=2)
                li_df_지지저항.append(df_지지저항_종목)

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

    def 분석_매수신호(self):
        """ 산출된 지지저항 값 기준으로 매수신호 분석 후 결과 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_지지저항'
        s_파일명_생성 = 'df_매수신호'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지지저항)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수신호)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 3분봉 읽어오기(2일치, 추세 확인 목적)
            li_대상일 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_3분봉_' in 파일명 and '.pkl' in 파일명]
            li_대상일 = sorted([대상일 for 대상일 in li_대상일 if 대상일 <= s_일자])
            li_대상일 = li_대상일[-2:]
            dic_3분봉_대상일 = dict()
            for s_대상일 in li_대상일:
                dic_3분봉_대상일[s_대상일] = pd.read_pickle(os.path.join(self.folder_캐시변환,
                                                                 f'dic_코드별_3분봉_{s_대상일}.pkl'))

            # 종목별 확인
            li_df_매수신호 = list()
            for s_종목코드 in tqdm(df_지지저항['종목코드'].unique(), desc=f'종목별 매수신호 분석|{s_일자}'):
                # 3분봉 정리 (tr조회 결과)
                li_df_3분봉 = [dic_3분봉_대상일[s_대상일][s_종목코드] if s_종목코드 in dic_3분봉_대상일[s_대상일].keys()
                             else pd.DataFrame() for s_대상일 in li_대상일]
                df_3분봉 = pd.concat(li_df_3분봉, axis=0).sort_index()

                # 추가 데이터 생성
                import UT_차트maker as chart
                df_3분봉 = chart.find_전일종가(df_ohlcv=df_3분봉)
                df_3분봉 = chart.make_이동평균(df_ohlcv=df_3분봉)

                # 지지저항 생성
                li_지지저항 = list(df_지지저항[df_지지저항['종목코드'] == s_종목코드]['고가'].values)

                # 매수신호 탐색
                li_df_매수신호_종목 = list()
                for s_시간 in df_3분봉[df_3분봉['일자'] == s_일자]['시간']:
                    # 입력용 분봉 잘라내기 (tr 동일하게 현재 분봉 포함)
                    dt_일자시간 = pd.Timestamp(f'{s_일자} {s_시간}')
                    df_3분봉_시간 = df_3분봉[df_3분봉.index <= dt_일자시간]

                    # 매수신호 생성
                    li_매수신호 = Logic.find_매수신호(df_ohlcv=df_3분봉_시간, li_지지저항=li_지지저항, dt_일자시간=dt_일자시간)

                    # 결과 정리
                    df_3분봉_매수신호 = df_3분봉_시간[-1:].copy()
                    df_3분봉_매수신호['매수신호'] = sum(li_매수신호) == len(li_매수신호)
                    for i in range(len(li_매수신호)):
                        df_3분봉_매수신호[f'매수{i + 1}'] = li_매수신호[i]
                    li_df_매수신호_종목.append(df_3분봉_매수신호)

                # 종목별 df 생성
                df_매수신호_종목 = pd.concat(li_df_매수신호_종목, axis=0)
                li_df_매수신호.append(df_매수신호_종목)

                # 차트 생성
                fig = chart.make_차트(df_ohlcv=df_매수신호_종목)
                for n_지지저항 in li_지지저항:
                    fig.axes[0].axhline(n_지지저항)

                # 매수신호 표시 (매수는 ^, 매도는 v)
                df_매수 = df_매수신호_종목[df_매수신호_종목['매수신호']]
                fig.axes[0].scatter(df_매수['일시'], df_매수['시가'], color='black', marker='^')

                # 차트 저장
                folder_그래프 = os.path.join(self.folder_매수신호, '그래프', f'매수신호_{s_일자}')
                os.makedirs(folder_그래프, exist_ok=True)
                fig.savefig(os.path.join(folder_그래프, f'매수신호_{s_종목코드}_{s_일자}.png'))

            # df 생성
            df_매수신호 = pd.concat(li_df_매수신호, axis=0).drop_duplicates()

            # df 저장
            df_매수신호.to_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매수신호.to_csv(os.path.join(self.folder_매수신호, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매수신호 분석 완료({s_일자}, {len(df_매수신호["종목코드"].unique()):,}종목)')

    def 분석_매도신호(self):
        """ 매수신호 기준으로 매도신호 분석 후 결과 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_매수신호'
        s_파일명_생성 = 'df_매도신호'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수신호)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도신호)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 데이터 불러오기
            df_매수신호_전체 = pd.read_pickle(os.path.join(self.folder_매수신호, f'{s_파일명_기준}_{s_일자}.pkl'))
            df_매수신호_전체_필터 = df_매수신호_전체[df_매수신호_전체['매수신호']]
            df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자}.pkl'))

            # 1분봉 읽어오기(당일)
            dic_1분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자}.pkl'))

            # 종목별 확인
            li_df_매도신호 = list()
            for s_종목코드 in tqdm(df_매수신호_전체_필터['종목코드'].unique(), desc=f'종목별 매도신호 분석|{s_일자}'):
                # 1분봉 정리
                df_1분봉 = dic_1분봉[s_종목코드].copy().sort_values(['일자', '시간']).reset_index(drop=True)

                # 데이터 정리
                df_매수신호 = df_매수신호_전체_필터[df_매수신호_전체_필터['종목코드'] == s_종목코드].sort_values('시간')
                li_지지저항 = list(df_지지저항[df_지지저항['종목코드'] == s_종목코드]['고가'].values)

                # 매수신호 탐색
                li_df_매도신호_종목 = list()
                s_매도시간 = '00:00:00'
                for s_매수시간 in df_매수신호['시간']:
                    # 매도 전 매수 금지
                    if s_매수시간 <= s_매도시간:
                        continue

                    # 기준정보 정의
                    n_매수단가 = df_매수신호[df_매수신호['시간'] == s_매수시간]['시가'].values[0]
                    n_지지선 = max(지지 for 지지 in li_지지저항 if 지지 < n_매수단가) if min(li_지지저항) < n_매수단가 else None
                    n_저항선 = min(저항 for 저항 in li_지지저항 if 저항 > n_매수단가) if max(li_지지저항) > n_매수단가 else None
                    dic_지지저항 = {'n_매수단가': n_매수단가, 'n_지지선': n_지지선, 'n_저항선': n_저항선}

                    # 1분봉 데이터 확인
                    for s_시간 in df_1분봉['시간']:
                        # 매수시간 이전 데이터 skip
                        if s_시간 < s_매수시간:
                            continue

                        # 기준 데이터 정의
                        df_1분봉_시점 = df_1분봉[df_1분봉['시간'] == s_시간]
                        n_고가 = df_1분봉_시점['고가'].values[0]
                        n_저가 = df_1분봉_시점['저가'].values[0]

                        # 매도신호 탐색
                        li_매도신호_고가, n_매도단가_고가 = Logic.find_매도신호(n_현재가=n_고가, dic_지지저항=dic_지지저항)
                        li_매도신호_저가, n_매도단가_저가 = Logic.find_매도신호(n_현재가=n_저가, dic_지지저항=dic_지지저항)
                        li_매도신호 = [(li_매도신호_고가[i] or li_매도신호_저가[i]) for i in range(len(li_매도신호_고가))]
                        n_매도단가 = n_매도단가_고가 or n_매도단가_저가

                        # 결과 확인
                        if sum(li_매도신호) > 0:
                            s_매도시간 = s_시간
                            df_매도신호_시점 = df_1분봉_시점.loc[:, ['일자', '종목코드', '종목명']]
                            df_매도신호_시점['매수시간'] = s_매수시간
                            df_매도신호_시점['매수단가'] = int(n_매수단가)
                            df_매도신호_시점['매도시간'] = s_매도시간
                            df_매도신호_시점['매도단가'] = int(n_매도단가)
                            df_매도신호_시점['수익률(%)'] = (df_매도신호_시점['매도단가'] / df_매도신호_시점['매수단가'] - 1) * 100
                            for i in range(len(li_매도신호)):
                                df_매도신호_시점[f'매도{i + 1}'] = li_매도신호[i]
                            li_df_매도신호_종목.append(df_매도신호_시점)
                            break

                # 종목별 df 생성
                try:
                    df_매도신호_종목 = pd.concat(li_df_매도신호_종목, axis=0)
                    li_df_매도신호.append(df_매도신호_종목)
                except ValueError:
                    continue

                # 차트 생성
                import UT_차트maker as chart
                fig = chart.make_차트(df_ohlcv=df_1분봉)
                for n_지지저항 in li_지지저항:
                    fig.axes[0].axhline(n_지지저항)

                # 매수매도 표시 (매수는 ^, 매도는 v)
                df_차트 = df_매도신호_종목.copy()
                df_차트['매수일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매수시간']
                df_차트['매도일시'] = df_차트['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매도시간']
                fig.axes[0].scatter(df_차트['매수일시'], df_차트['매수단가'], color='black', marker='^')
                fig.axes[0].scatter(df_차트['매도일시'], df_차트['매도단가'], color='black', marker='v')

                # 차트 저장
                folder_그래프 = os.path.join(self.folder_매도신호, '그래프', f'매도신호_{s_일자}')
                os.makedirs(folder_그래프, exist_ok=True)
                fig.savefig(os.path.join(folder_그래프, f'매도신호_{s_종목코드}_{s_일자}.png'))

            # df_매도신호 생성
            df_매도신호 = pd.concat(li_df_매도신호, axis=0)

            # df 저장
            df_매도신호.to_pickle(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매도신호.to_csv(os.path.join(self.folder_매도신호, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매도신호 분석 완료({s_일자}, {len(df_매도신호):,}건)')

    def 분석_종목선정(self):
        """ 매도신호 기준으로 적합성 검증 후 대상 종목 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_매도신호'
        s_파일명_생성 = 'df_종목선정'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도신호)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목선정)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
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
                n_수익률_퍼센트 = n_수익건수 / n_전체매매 * 100

                # 종목 선정 (미충족 시 이후 skip)
                if n_수익률_퍼센트 < 90:
                    continue

                # 선정된 종목 추가
                li_df_종목선정.append(df_매도신호)

                # 차트 생성
                import UT_차트maker as chart
                fig = chart.make_차트(df_ohlcv=df_1분봉)
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
    a = Analyzer()

    a.분석_일봉변동()
    a.분석_지지저항()
    a.분석_매수신호()
    a.분석_매도신호()
    a.분석_종목선정()

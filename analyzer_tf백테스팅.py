import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm
import multiprocessing as mp

import UT_차트maker as Chart
import analyzer_tf알고리즘_백테스팅 as Logic

# 그래프 한글 설정
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames,PyUnusedLocal,PyTypeChecker
class Analyzer:
    def __init__(self, b_멀티=False, s_시작일자=None, n_분석일수=None):
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
        self.folder_체결잔고 = dic_폴더정보['이력|체결잔고']
        self.folder_대상종목 = dic_폴더정보['이력|대상종목']
        self.folder_주문정보 = dic_폴더정보['이력|주문정보']
        self.folder_분봉확인 = dic_폴더정보['tf종목분석|20_분봉확인']
        self.folder_매수매도 = dic_폴더정보['tf백테스팅|10_매수매도']
        self.folder_결과정리 = dic_폴더정보['tf백테스팅|20_결과정리']
        self.folder_결과요약 = dic_폴더정보['tf백테스팅|30_결과요약']
        self.folder_수익요약 = dic_폴더정보['tf백테스팅|40_수익요약']
        self.folder_매매이력 = dic_폴더정보['tf백테스팅|50_매매이력']
        os.makedirs(self.folder_매수매도, exist_ok=True)
        os.makedirs(self.folder_결과정리, exist_ok=True)
        os.makedirs(self.folder_결과요약, exist_ok=True)
        os.makedirs(self.folder_수익요약, exist_ok=True)
        os.makedirs(self.folder_매매이력, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.s_시작일자 = s_시작일자
        self.n_분석일수 = n_분석일수
        self.b_멀티 = b_멀티
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                        if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]
        self.dic_매개변수 = dict()

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 검증_매수매도(self, n_초봉):
        """ 지표 추가된 초봉 데이터 기준 매수, 매도 검증 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_분봉확인'
        s_파일명_생성 = 'dic_매수매도'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_분봉확인)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명]
        li_일자_전체 = [일자 for 일자 in li_일자_전체 if 일자 >= self.s_시작일자] if self.s_시작일자 is not None else li_일자_전체
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]
        # li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 초봉 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_분봉확인, f'dic_분봉확인_{s_일자}_{n_초봉}초봉.pkl'))
            # dic_1초봉 = pd.read_pickle(os.path.join(self.folder_분봉확인, f'dic_분봉확인_{s_일자}_1초봉.pkl'))
            dic_1초봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_1초봉_{s_일자}.pkl'))

            # 종목별 분석 진행
            li_대상종목 = list(dic_초봉.keys())
            self.dic_매개변수['n_초봉'] = n_초봉
            self.dic_매개변수['s_일자'] = s_일자
            self.dic_매개변수['dic_초봉'] = dic_초봉
            self.dic_매개변수['dic_1초봉'] = dic_1초봉
            if self.b_멀티:
                with mp.Pool(processes=self.n_멀티코어수) as pool:
                    li_df_매수매도 = list(tqdm(pool.imap(self.검증_매수매도_종목별, li_대상종목),
                                           total=len(li_대상종목), desc=f'매수매도-{n_초봉}초봉-{s_일자}'))
                dic_매수매도 = dict(zip(li_대상종목, li_df_매수매도))
            else:
                dic_매수매도 = dict()
                for s_종목코드 in tqdm(li_대상종목, desc=f'매수매도-{n_초봉}초봉-{s_일자}'):
                    df_매수매도 = self.검증_매수매도_종목별(s_종목코드=s_종목코드)
                    dic_매수매도[s_종목코드] = df_매수매도

            # dic 저장
            pd.to_pickle(dic_매수매도, os.path.join(self.folder_매수매도, f'dic_매수매도_{s_일자}_{n_초봉}초봉.pkl'))

            # log 기록
            self.make_log(f'신호생성 완료({s_일자}, {n_초봉}초봉, {len(dic_매수매도):,}개 종목)')

    def 검증_매수매도_종목별(self, s_종목코드):
        """ 종목별 매수매도 정보 생성 후 df 리턴 """
        # 기준정보 정의
        n_초봉 = self.dic_매개변수['n_초봉']
        s_일자 = self.dic_매개변수['s_일자']
        dic_초봉 = self.dic_매개변수['dic_초봉']
        dic_1초봉 = self.dic_매개변수['dic_1초봉']

        # df 정의
        df_초봉 = dic_초봉[s_종목코드].sort_index().copy() if s_종목코드 in dic_초봉 else pd.DataFrame()
        df_1초봉 = dic_1초봉[s_종목코드].sort_index().copy() if s_종목코드 in dic_1초봉 else pd.DataFrame()

        # 종목별 매개변수 생성
        s_종목명 = df_초봉['종목명'].values[0] if '종목명' in df_초봉 else None
        s_선정사유 = df_초봉['선정사유'].values[0] if '선정사유' in df_초봉 else None
        dic_매개변수_종목 = self.dic_매개변수[s_종목코드] if s_종목코드 in self.dic_매개변수 else dict()
        dic_매개변수_종목['s_종목코드'] = s_종목코드
        dic_매개변수_종목['s_종목명'] = s_종목명
        dic_매개변수_종목['s_일자'] = s_일자
        dic_매개변수_종목['n_초봉'] = n_초봉
        dic_매개변수_종목['s_선정사유'] = s_선정사유

        # 신호 생성
        b_매수신호 = False
        b_매도신호 = False
        b_보유신호 = False

        # 시간별 검증
        dic_매수매도 = dict()
        for dt_시점 in df_1초봉.index:
            # 무효시간 필터링
            if not b_보유신호 and dt_시점.second % n_초봉 != 1 and n_초봉 != 1:
                continue

            # 초봉 데이터 준비
            df_초봉_시점 = df_초봉[df_초봉.index < dt_시점 - pd.Timedelta(seconds=n_초봉)]
            df_초봉_시점 = df_초봉_시점[-50:]
            if df_초봉_시점.empty:
                continue

            # 현재가 확인
            df_현재가 = df_1초봉[df_1초봉.index <= dt_시점]
            n_현재가 = df_현재가['시가'].values[-1]
            n_현재가 = df_현재가['종가'].dropna().values[-1] if pd.isna(n_현재가) and len(df_현재가['종가'].dropna()) > 0\
                                                        else n_현재가

            # 매수검증
            if not b_보유신호:
                # 매수신호 검증
                dic_매개변수_종목['df_초봉_매수봇'] = df_초봉_시점
                dic_매수신호 = Logic.make_매수신호(dic_매개변수=dic_매개변수_종목, dt_일자시간=dt_시점)
                b_매수신호 = dic_매수신호['b_매수신호_매수봇']
                li_매수신호 = dic_매수신호['li_매수신호_매수봇']

                # 매개변수 업데이트
                dic_매개변수_종목['s_탐색시간_매수봇'] = dt_시점.strftime('%H:%M:%S')
                dic_매개변수_종목['n_현재가_매수봇'] = n_현재가
                dic_매개변수_종목 = {**dic_매개변수_종목, **dic_매수신호}
                self.dic_매개변수[s_종목코드] = dic_매개변수_종목

                # 매수 정보 생성
                if b_매수신호:
                    s_매수시간 = dt_시점.strftime('%H:%M:%S')
                    dic_매개변수_종목['s_주문시간_매수봇'] = s_매수시간
                    dic_매개변수_종목['n_주문단가_매수봇'] = n_현재가
                    dic_매개변수_종목['n_주문수량_매수봇'] = 1
                    b_보유신호 = True

            # 매도검증
            if b_보유신호:
                dic_매개변수_종목['n_매수단가_매도봇'] = dic_매개변수_종목['n_주문단가_매수봇']
                dic_매개변수_종목['n_보유수량_매도봇'] = dic_매개변수_종목['n_주문수량_매수봇']
                dic_매개변수_종목['df_초봉_매도봇'] = df_초봉_시점
                dic_매개변수_종목['n_현재가_매도봇'] = n_현재가
                dic_매도신호 = Logic.make_매도신호(dic_매개변수=dic_매개변수_종목, dt_일자시간=dt_시점)
                b_매도신호 = dic_매도신호['b_매도신호_매도봇']
                li_매도신호 = dic_매도신호['li_매도신호_매도봇']
                li_신호종류 = dic_매도신호['li_신호종류_매도봇']

                # 매개변수 업데이트
                dic_매개변수_종목['s_탐색시간_매도봇'] = dt_시점.strftime('%H:%M:%S')
                dic_매개변수_종목 = {**dic_매개변수_종목, **dic_매도신호}
                self.dic_매개변수[s_종목코드] = dic_매개변수_종목

                # 매도 정보 생성
                if b_매도신호:
                    dic_매개변수_종목['s_주문시간_매도봇'] = dt_시점.strftime('%H:%M:%S')
                    dic_매개변수_종목['n_현재가_매도봇'] = n_현재가
                    dic_매개변수_종목['n_주문단가_매도봇'] = n_현재가
                    dic_매개변수_종목['n_주문수량_매도봇'] = 1
                    dic_매개변수_종목['s_매도사유_매도봇'] = [li_신호종류[i] for i in range(len(li_매도신호)) if li_매도신호[i]][0]

            # 결과 정리
            for s_컬럼명 in df_초봉_시점.columns:
                dic_매수매도.setdefault(s_컬럼명, list()).append(df_초봉_시점[s_컬럼명].values[-1])
            dic_매수매도.setdefault('매수신호', list()).append(b_매수신호)
            dic_매수매도.setdefault('매도신호', list()).append(b_매도신호)
            dic_매수매도.setdefault('보유신호', list()).append(b_보유신호)
            dic_매수매도.setdefault('현재시점', list()).append(dt_시점.strftime('%H:%M:%S'))
            for i, s_신호종류 in enumerate(dic_매개변수_종목['li_신호종류_매수봇']):
                dic_매수매도.setdefault(f'매수_{s_신호종류}', list()).append(dic_매개변수_종목['li_매수신호_매수봇'][i])
            dic_매수매도.setdefault('현재가', list()).append(n_현재가)
            for i, s_신호종류 in enumerate(['매도우세', '매수피크', '하락한계', '타임아웃']):
                dic_매수매도.setdefault(f'매도_{s_신호종류}', list()).append(dic_매개변수_종목['li_매도신호_수치_매도봇'][i]
                                                                                            if b_보유신호 else None)
            dic_매수매도.setdefault('피크_기준봉', list()).append(dic_매개변수_종목['n_매수량_기준봉_매도봇'] if b_보유신호 else None)
            dic_매수매도.setdefault('피크_매수량', list()).append(dic_매개변수_종목['n_매수량_매도봇'] if b_보유신호 else None)
            dic_매수매도.setdefault('피크_z매수량', list()).append(dic_매개변수_종목['n_z매수량_매도봇'] if b_보유신호 else None)
            dic_매수매도.setdefault('매수가', list()).append(dic_매개변수_종목['n_매수단가_매도봇'] if b_보유신호 else None)
            dic_매수매도.setdefault('매도가', list()).append(dic_매개변수_종목['n_주문단가_매도봇'] if b_매도신호 else None)
            dic_매수매도.setdefault('매수시간', list()).append(dic_매개변수_종목['s_주문시간_매수봇'] if b_보유신호 else None)
            dic_매수매도.setdefault('매도시간', list()).append(dic_매개변수_종목['s_주문시간_매도봇'] if b_매도신호 else None)
            dic_매수매도.setdefault('매도사유', list()).append(dic_매개변수_종목['s_매도사유_매도봇'] if b_매도신호 else None)
            dic_매수매도.setdefault('수익률%', list()).append(dic_매개변수_종목['n_수익률_매도봇'] if b_보유신호 else None)

            # 신호 초기화
            b_보유신호 = False if b_매도신호 else b_보유신호
            b_매수신호 = False
            b_매도신호 = False

        # df 생성
        df_매수매도 = pd.DataFrame(dic_매수매도)
        df_매수매도['dt일시'] = pd.to_datetime(s_일자 + ' ' + df_매수매도['체결시간'])
        df_매수매도 = df_매수매도.set_index('dt일시').sort_index()

        # csv 저장
        s_폴더 = os.path.join(f'{self.folder_매수매도}_종목별', f'매수매도_{s_일자}')
        os.makedirs(s_폴더, exist_ok=True)
        df_매수매도.to_csv(os.path.join(s_폴더, f'df_매수매도_{s_일자}_{n_초봉}초봉_{s_종목코드}_{s_종목명}.csv'),
                       index=False, encoding='cp949')

        return df_매수매도

    def 검증_결과정리(self, n_초봉):
        """ 매수 매도 신호 기준으로 결과 정리 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_매수매도'
        s_파일명_생성 = 'df_결과정리'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]
        # li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 초봉 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_매수매도, f'dic_매수매도_{s_일자}_{n_초봉}초봉.pkl'))

            # 종목별 매수매도 통합
            li_df_결과정리_전체 = list()
            for s_종목코드 in dic_초봉.keys():
                df_초봉 = dic_초봉[s_종목코드].sort_index().copy()
                li_df_결과정리_전체.append(df_초봉)
            if len(li_df_결과정리_전체) == 0:
                continue
            df_결과정리_전체 = pd.concat(li_df_결과정리_전체, axis=0)

            # 추가정보 생성
            df_결과정리_전체['수익률%'] = (df_결과정리_전체['매도가'] / df_결과정리_전체['매수가'] - 1) * 100 - 0.2
            df_결과정리_전체['보유초'] = (pd.to_datetime(df_결과정리_전체['매도시간'], format='%H:%M:%S')
                                    - pd.to_datetime(df_결과정리_전체['매수시간'], format='%H:%M:%S')).dt.total_seconds()

            # 매도 데이터 있는 항목만 골라내기
            df_결과정리 = df_결과정리_전체[df_결과정리_전체['매도시간'] > '00:00:00']

            # 파일 저장 (매수매도 only)
            df_결과정리.to_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl'))
            df_결과정리.to_csv(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.csv'),
                          index=False, encoding='cp949')

            # 파일 저장 (매수매도 only)
            df_결과정리_전체.to_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉_전체.pkl'))
            df_결과정리_전체.to_csv(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉_전체.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'결과정리 완료({s_일자}, {n_초봉}초봉, 수익률: {df_결과정리["수익률%"].sum():,.1f}%)')

    def 검증_결과요약(self, n_초봉):
        """ 결과정리 데이터 기준으로 일별 데이터 요약 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_결과정리'
        s_파일명_생성 = 'df_결과요약'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명 and '_전체' not in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]
        # li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전체 결과정리 파일 확인
            li_파일일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                       if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'_{n_초봉}초봉' in 파일명 and '_전체' not in 파일명]
            li_파일일자 = [파일일자 for 파일일자 in li_파일일자 if 파일일자 <= s_일자]

            # 파일별 결과 요약
            li_df_결과요약 = list()
            for s_파일일자 in li_파일일자:
                # 결과 파일 불러오기
                df_결과 = pd.read_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_파일일자}_{n_초봉}초봉.pkl'))
                dic_결과_선정사유 = dict()
                li_선정사유 = ['일봉변동', 'vi발동', '거래량급증']
                for s_선정사유 in li_선정사유:
                    dic_결과_선정사유[s_선정사유] = df_결과[df_결과['선정사유'] == s_선정사유]

                # 결과 요약
                df_일별요약 = pd.DataFrame({'일자': [s_파일일자]})
                df_일별요약['전체거래'] = int(len(df_결과))
                df_일별요약['수익거래'] = int(len(df_결과[df_결과['수익률%'] > 0]))
                df_일별요약['성공률%'] = (df_일별요약['수익거래'] / df_일별요약['전체거래']) * 100
                df_일별요약['수익률%'] = df_결과['수익률%'].sum()
                df_일별요약['평균수익률%'] = df_일별요약['수익률%'] / df_일별요약['전체거래']
                df_일별요약['평균보유초'] = df_결과['보유초'].mean()
                df_일별요약['종목수'] = len(df_결과['종목코드'].unique())
                df_일별요약['종목당거래'] = df_일별요약['전체거래'] / df_일별요약['종목수']
                for s_선정사유 in li_선정사유:
                    df_결과_선정사유 = dic_결과_선정사유[s_선정사유]
                    df_일별요약[f'{s_선정사유[:2]}|성공률%'] = (int(len(df_결과_선정사유[df_결과_선정사유['수익률%'] > 0]))
                                                / int(len(df_결과_선정사유))) * 100 if len(df_결과_선정사유) > 0 else None
                    df_일별요약[f'{s_선정사유[:2]}|수익률%'] = df_결과_선정사유['수익률%'].sum()
                li_df_결과요약.append(df_일별요약)

            # df 생성
            df_결과요약 = pd.concat(li_df_결과요약, axis=0).sort_values('일자', ascending=False)

            # 파일 저장
            df_결과요약.to_pickle(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{n_초봉}초봉.pkl'))
            df_결과요약.to_csv(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{n_초봉}초봉.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'결과요약 완료({s_일자}, {n_초봉}초봉, 누적 수익률: {df_결과요약["수익률%"].sum():,.1f}%)')

    def 검증_수익요약(self):
        """ 결과요약 데이터 기준 초봉별 수익률 정리 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_결과요약'
        s_파일명_생성 = 'df_수익요약'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = list(dict.fromkeys(li_일자_전체))    # 중복제거
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익요약)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]
        # li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 내 초봉 확인
            li_초봉 = [re.findall(r'\d+초봉', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                     if s_일자 in 파일명 and '.pkl' in 파일명]
            li_초봉 = sorted(li_초봉, key=lambda x: int(x.replace('초봉', '')))

            # 초봉별 수익요약 생성
            li_df_수익요약 = list()
            li_li_일자 = list()
            li_선정사유 = ['일봉변동', 'vi발동', '거래량급증']
            for s_초봉 in li_초봉:
                # 초 이름 정의
                s_초 = s_초봉.replace('초봉', '초')

                # 일별 데이터 설정 (20일)
                df_결과 = pd.read_pickle(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{s_초봉}.pkl'))
                df_요약_일별 = pd.DataFrame()
                df_요약_일별['일자'] = df_결과['일자']
                df_요약_일별[f'{s_초}|전체'] = df_결과['수익률%']
                for s_선정사유 in li_선정사유:
                    df_요약_일별[f'{s_초}|{s_선정사유[:2]}'] = df_결과[f'{s_선정사유[:2]}|수익률%']
                df_요약_일별 = df_요약_일별[:20]

                # 누적 데이터 생성 (20일)
                df_요약_누적 = pd.DataFrame()
                df_요약_누적['일자'] = ['20누적%']
                li_컬럼명 = [컬럼명 for 컬럼명 in df_요약_일별.columns if '일자' not in 컬럼명]
                for s_컬럼명 in li_컬럼명:
                    df_요약_누적[s_컬럼명] = df_요약_일별[s_컬럼명].sum()

                # 성능 데이터 생성 (10일 중 max, min 제외한 8일 and 수익률 0% 4개 이상 제외)
                df_요약_일별_10 = df_요약_일별[:10]
                df_요약_성능 = pd.DataFrame()
                df_요약_성능['일자'] = ['10성능%']
                li_컬럼명 = [컬럼명 for 컬럼명 in df_요약_일별_10.columns if '일자' not in 컬럼명]
                for s_컬럼명 in li_컬럼명:
                    sri_일별_10 = df_요약_일별_10[s_컬럼명]
                    df_요약_성능[s_컬럼명] = sri_일별_10.sum() - sri_일별_10.max() - sri_일별_10.min()\
                                            if (sri_일별_10 == 0).sum() < 4 else None

                # 초봉별 요약 데이터 생성
                df_요약 = pd.concat([df_요약_누적, df_요약_성능, df_요약_일별], axis=0)
                li_df_수익요약.append(df_요약.set_index('일자'))
                li_li_일자.append(df_요약['일자'].to_list())

            # 수익요약 데이터 생성
            # li_일자 = list(li_df_수익요약[0].index)
            li_일자 = max(li_li_일자, key=len)
            li_df_수익요약 = [df_수익요약.reset_index(drop=True) for df_수익요약 in li_df_수익요약]
            # df_수익요약 = pd.concat(li_df_수익요약, axis=1).reset_index()
            df_수익요약 = pd.concat(li_df_수익요약, axis=1)
            df_수익요약.insert(loc=0, column='일자', value=li_일자)
            # li_컬럼명 = list(df_수익요약.columns)
            # df_수익요약['일자'] = li_일자
            # df_수익요약 = df_수익요약.loc[:, ['일자'] + li_컬럼명]

            # 파일 저장
            df_수익요약.to_pickle(os.path.join(self.folder_수익요약, f'df_수익요약_{s_일자}.pkl'))
            df_수익요약.to_csv(os.path.join(self.folder_수익요약, f'df_수익요약_{s_일자}.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'수익요약 완료({s_일자})')

    def 검증_매매이력(self, b_카톡):
        """ 매매이력 데이터 정리 후 pkl, csv 저장 + 리포트 생성 """
        # 파일명 정의
        s_파일명_기준 = 'df_수익요약'
        s_파일명_생성 = 'df_매매이력'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익요약)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = list(dict.fromkeys(li_일자_전체))    # 중복제거
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매매이력)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료 and s_일자 >= self.s_시작일자]
        # li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 수익요약 데이터 불러오기
            df_수익요약 = pd.read_pickle(os.path.join(self.folder_수익요약, f'{s_파일명_기준}_{s_일자}.pkl'))
            dic_매매정보 = dict(s_일자=s_일자, df_수익요약=df_수익요약)
            # dic_매매정보['df_수익요약'] = df_수익요약

            # 실거래 적용 초봉 및 선정기준 확인
            df_대상종목_매매 = pd.read_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{s_일자}_매매.pkl')) \
                if f'df_대상종목_{s_일자}_매매.pkl' in os.listdir(self.folder_대상종목) else None
            n_초봉 = df_대상종목_매매['초봉'].values[-1] if df_대상종목_매매 is not None else 5
            s_선정사유 = df_대상종목_매매['선정사유'].values[-1] if df_대상종목_매매 is not None else 'vi발동'
            dic_매매정보['n_초봉'] = n_초봉
            dic_매매정보['s_선정사유'] = s_선정사유

            # 백테스팅 초봉 확인
            li_초봉_백테스팅 = [int(컬럼.split('초|')[0]) for 컬럼 in df_수익요약.columns if '초|' in 컬럼]
            li_초봉_백테스팅 = list(dict.fromkeys(li_초봉_백테스팅))

            # 거래정보 생성 - 실거래
            df_실거래 = self.검증_매매이력_실거래(dic_매매정보=dic_매매정보)
            df_백테스팅 = self.검증_매매이력_백테스팅(n_초봉=n_초봉, dic_매매정보=dic_매매정보)
            df_백테스팅_선정사유 = df_백테스팅[df_백테스팅['선정사유'] == s_선정사유]
            df_거래정보_실거래 = pd.concat([df_실거래, df_백테스팅_선정사유], axis=0).sort_values(['매수시간'])

            # 파일 저장 - 실거래
            df_거래정보_실거래.to_pickle(os.path.join(self.folder_매매이력, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_거래정보_실거래.to_csv(os.path.join(self.folder_매매이력, f'{s_파일명_생성}_{s_일자}.csv'),
                               index=False, encoding='cp949')

            # 거래정보 생성 및 저장 - 백테스팅
            dic_거래정보_백테스팅 = dict()
            for n_초봉_백테스팅 in li_초봉_백테스팅:
                df_거래정보_백테스팅 = (self.검증_매매이력_백테스팅(n_초봉=n_초봉_백테스팅, dic_매매정보=dic_매매정보)
                                        .sort_values(['선정사유', '매수시간']))
                dic_거래정보_백테스팅[n_초봉_백테스팅] = df_거래정보_백테스팅

                # 파일 저장 - 백테스팅
                s_파일명 = f'{s_파일명_생성}_{s_일자}_백테스팅_{n_초봉_백테스팅}초봉'
                df_거래정보_백테스팅.to_pickle(os.path.join(self.folder_매매이력, f'{s_파일명}.pkl'))
                df_거래정보_백테스팅.to_csv(os.path.join(self.folder_매매이력, f'{s_파일명}.csv'),
                                    index=False, encoding='cp949')
            # 리포트 생성 - 실거래
            dic_매매정보['folder_캐시변환'] = self.folder_캐시변환
            dic_매매정보['df_거래정보_실거래'] = df_거래정보_실거래
            fig = Chart.make_수익리포트(df_거래정보=df_거래정보_실거래, dic_매매정보=dic_매매정보, li_컬럼명_강조=None)

            # 리포트 파일 저장 - 실거래
            folder_리포트 = f'{self.folder_매매이력}_리포트'
            os.makedirs(folder_리포트, exist_ok=True)
            s_파일명_리포트 = f'백테스팅_리포트_{s_일자}.png'
            fig.savefig(os.path.join(folder_리포트, s_파일명_리포트), dpi=600)
            plt.close(fig)

            # 리포트 복사 to 서버 - 실거래
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/tf분석_백테스팅'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 리포트 생성 및 저장 - 백테스팅
            for n_초봉_백테스팅 in li_초봉_백테스팅:
                # 리포트 생성
                dic_매매정보['n_초봉'] = n_초봉_백테스팅
                fig = Chart.make_수익리포트(df_거래정보=dic_거래정보_백테스팅[n_초봉_백테스팅], dic_매매정보=dic_매매정보,
                                            li_컬럼명_강조=[f'{n_초봉_백테스팅}초|전체'])

                # 리포트 파일 저장
                s_파일명_리포트_백테스팅 = f'백테스팅_리포트_{s_일자}_백테스팅_{n_초봉_백테스팅}초봉.png'
                try:
                    fig.savefig(os.path.join(folder_리포트, s_파일명_리포트_백테스팅), dpi=200)
                except ValueError:
                    fig.savefig(os.path.join(folder_리포트, s_파일명_리포트_백테스팅), dpi=100)
                plt.close(fig)

                # 리포트 복사 to 서버
                w.to_ftp(s_파일명=s_파일명_리포트_백테스팅, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡 and s_일자 == li_일자_대상[-1]:
                import API_kakao
                k = API_kakao.KakaoAPI()
                # 실거래 리포트
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 백테스팅 완료',
                                        s_button_title=f'[tf분석] 백테스팅 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

                # 백테스팅 리포트 - 폴더
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 백테스팅 완료',
                                        s_button_title=f'[tf분석] 리포트 폴더 접속',
                                        s_url=f'http://goniee.com/{folder_서버}/')

            # log 기록
            self.make_log(f'매매이력 검증 완료({s_일자})')

    def 검증_매매이력_실거래(self, dic_매매정보):
        """ 실거래 정보 조회하여 df_거래정보 생성 후 리턴"""
        # 기준정보 정의
        s_일자 = dic_매매정보['s_일자']
        n_초봉 = dic_매매정보['n_초봉']
        s_선정사유 = dic_매매정보['s_선정사유']

        # 체결잔고 데이터 불러오기 - 실거래 확인
        df_체결잔고 = pd.read_csv(os.path.join(self.folder_체결잔고, f'체결잔고_{s_일자}.csv'), encoding='cp949') \
            if f'체결잔고_{s_일자}.csv' in os.listdir(self.folder_체결잔고) \
            else pd.DataFrame(dict(주문상태=list(), 계좌번호=list()))
        df_체결잔고_체결 = df_체결잔고[df_체결잔고['주문상태'] == '체결'].copy()
        df_체결잔고_체결 = df_체결잔고_체결[df_체결잔고_체결['계좌번호'] == 5292685210]
        if len(df_체결잔고_체결) == 0:
            return pd.DataFrame()

        # df_거래정보 생성
        li_컬럼명 = ['종목코드', '종목명', '시간', '주문구분', '체결가']
        df_거래정보 = df_체결잔고_체결.loc[:, li_컬럼명].copy()
        df_거래정보['종목코드'] = df_거래정보['종목코드'].apply(lambda x: x.replace('A', ''))
        df_거래정보['종목명'] = df_거래정보['종목명'].apply(lambda x: x.replace(' ', ''))

        # 매도사유 추가
        df_주문정보 = pd.read_pickle(os.path.join(self.folder_주문정보, f'주문정보_{s_일자}.pkl')) \
            if f'주문정보_{s_일자}.pkl' in os.listdir(self.folder_주문정보) \
            else pd.DataFrame(dict(일자=list(), 주문시간=list()))
        df_주문정보['dt일시'] = pd.to_datetime(df_주문정보['일자'] + ' ' + df_주문정보['주문시간'])
        df_주문정보 = df_주문정보.set_index('dt일시')
        df_주문정보_매도 = df_주문정보[df_주문정보['주문구분'] == '매도'] if len(df_주문정보) > 0 else None
        li_매도사유 = list()
        for i in range(len(df_거래정보)):
            s_종목코드 = df_거래정보['종목코드'].values[i]
            dt_거래시간 = pd.Timestamp(f'{s_일자} {df_거래정보["시간"].values[i]}')
            s_주문구분 = df_거래정보['주문구분'].values[i]
            if s_주문구분 == '매수' or df_주문정보_매도 is None:
                li_매도사유.append(None)
                continue
            if s_주문구분 == '매도':
                df_주문정보_매도_종목 = df_주문정보_매도[df_주문정보_매도['종목코드'] == s_종목코드]
                df_주문정보_매도_종목 = df_주문정보_매도_종목[df_주문정보_매도_종목.index <= dt_거래시간]
                df_주문정보_매도_종목 = df_주문정보_매도_종목[df_주문정보_매도_종목.index >= dt_거래시간 - pd.Timedelta(seconds=1)]
                s_매도사유 = df_주문정보_매도_종목['매도사유'].values[-1] if len(df_주문정보_매도_종목) > 0 else None
                li_매도사유.append(s_매도사유)
                continue
        df_거래정보['매도사유'] = li_매도사유

        # 양식 변경 - 매수매도 한번에
        df_거래정보 = df_거래정보.sort_values(['종목코드', '시간'])
        dic_거래정보_매수 = dict()
        dic_거래정보_매도 = dict()
        li_컬럼명 = ['종목코드', '종목명', '매수시간', '매도시간', '매수가', '매도가', '매도사유']
        for i in range(len(df_거래정보)):
            s_주문구분 = df_거래정보['주문구분'].values[i]
            if s_주문구분 == '매수':
                dic_거래정보_매수.setdefault('종목코드', list()).append(df_거래정보['종목코드'].values[i])
                dic_거래정보_매수.setdefault('종목명', list()).append(df_거래정보['종목명'].values[i])
                dic_거래정보_매수.setdefault('매수시간', list()).append(df_거래정보['시간'].values[i])
                dic_거래정보_매수.setdefault('매수가', list()).append(df_거래정보['체결가'].values[i])
            if s_주문구분 == '매도':
                dic_거래정보_매도.setdefault('매도시간', list()).append(df_거래정보['시간'].values[i])
                dic_거래정보_매도.setdefault('매도가', list()).append(df_거래정보['체결가'].values[i])
                dic_거래정보_매도.setdefault('매도사유', list()).append(df_거래정보['매도사유'].values[i])
        df_거래정보_매수 = pd.DataFrame(dic_거래정보_매수)
        df_거래정보_매도 = pd.DataFrame(dic_거래정보_매도)
        df_거래정보_양식변경 = pd.concat([df_거래정보_매수, df_거래정보_매도], axis=1).loc[:, li_컬럼명] \
            if len(df_거래정보_매수) == len(df_거래정보_매도) else pd.DataFrame()

        # 추가정보 생성
        df_거래정보_양식변경['초봉'] = n_초봉 if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경
        df_거래정보_양식변경['선정사유'] = s_선정사유 if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경
        df_거래정보_양식변경['거래구분'] = '실거래' if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경

        return df_거래정보_양식변경

    def 검증_매매이력_백테스팅(self, n_초봉, dic_매매정보):
        """ 백테스팅 정보 조회하여 df_거래정보 생성 후 리턴"""
        # 기준정보 정의
        s_일자 = dic_매매정보['s_일자']

        # 백테스팅 결과 불러오기
        df_백테결과 = pd.read_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl')) \
            if f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl' in os.listdir(self.folder_결과정리) else pd.DataFrame()

        # df_거래정보 생성
        df_거래정보 = pd.DataFrame()
        df_거래정보['종목코드'] = df_백테결과['종목코드'].values
        df_거래정보['종목명'] = df_백테결과['종목명'].values
        df_거래정보['매수시간'] = df_백테결과['매수시간'].values
        df_거래정보['매도시간'] = df_백테결과['매도시간'].values
        df_거래정보['매수가'] = df_백테결과['매수가'].values
        df_거래정보['매도가'] = df_백테결과['매도가'].values
        df_거래정보['매도사유'] = df_백테결과['매도사유'].values
        df_거래정보['초봉'] = n_초봉
        df_거래정보['선정사유'] = df_백테결과['선정사유'].values
        df_거래정보['거래구분'] = '백테스팅'

        return df_거래정보

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
    a = Analyzer(b_멀티=True, s_시작일자='20250623', n_분석일수=None)
    li_초봉 = [3, 5, 10, 12, 15, 20, 30]
    [a.검증_매수매도(n_초봉=n_초봉) for n_초봉 in li_초봉]
    [a.검증_결과정리(n_초봉=n_초봉) for n_초봉 in li_초봉]
    [a.검증_결과요약(n_초봉=n_초봉) for n_초봉 in li_초봉]
    a.검증_수익요약()
    a.검증_매매이력(b_카톡=True)

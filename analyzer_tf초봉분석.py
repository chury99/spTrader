import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm
import multiprocessing as mp

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
    def __init__(self, b_멀티=False, n_분석일수=None):
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
        self.folder_분봉확인 = dic_폴더정보['tf초봉분석|20_분봉확인']
        os.makedirs(self.folder_지표생성, exist_ok=True)
        os.makedirs(self.folder_분봉확인, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수
        self.b_멀티 = b_멀티
        self.n_멀티코어수 = mp.cpu_count() - 2
        self.n_분석일수 = n_분석일수
        self.li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                        if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 초봉분석 시작 ###')

    def 분석_지표생성(self, n_초봉):
        """ trader 동작 시 생성된 대상종목 기준으로 초봉 데이터에 지표 추가 생성 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_대상종목'
        s_파일명_생성 = 'dic_지표생성'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_대상종목)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and '매매' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지표생성)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{s_일자}_매매.pkl'))
            dic_종목코드2종목명 = df_대상종목.set_index('종목코드').to_dict()['종목명']
            dic_종목코드2추가시점 = df_대상종목.set_index('종목코드').to_dict()['추가시점']
            dic_종목코드2선정사유 = df_대상종목.set_index('종목코드').to_dict()['선정사유']

            # 초봉 불러오기
            try:
                dic_초봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_{n_초봉}초봉_{s_일자}.pkl'))
            except FileNotFoundError:
                continue

            # 종목별 분석 진행
            li_대상종목 = list(df_대상종목['종목코드'])
            self.dic_정보_지표생성 = dict(n_초봉=n_초봉, s_일자=s_일자, dic_초봉=dic_초봉, dic_종목코드2종목명=dic_종목코드2종목명,
                                    dic_종목코드2추가시점=dic_종목코드2추가시점, dic_종목코드2선정사유=dic_종목코드2선정사유)
            if self.b_멀티:
                with mp.Pool(processes=self.n_멀티코어수) as pool:
                    li_df_지표생성 = list(tqdm(pool.imap(self.종목별_지표생성, li_대상종목),
                                           total=len(li_대상종목), desc=f'지표생성-{n_초봉}초봉-{s_일자}'))
                dic_지표생성 = dict(zip(li_대상종목, li_df_지표생성))

                # 멀티 데이터 틀어짐 확인
                li_데이터확인 = [종목코드 == dic_지표생성[종목코드]['종목코드'].values[-1] for 종목코드 in li_대상종목]
                if sum(li_데이터확인) != len(li_데이터확인):
                    self.make_log(f'!!! 멀티 데이터 틀어짐 발생 !!! - {s_일자}, {n_초봉}초봉')
                    break
            else:
                dic_지표생성 = dict()
                for s_종목코드 in tqdm(li_대상종목, desc=f'지표생성-{n_초봉}초봉-{s_일자}'):
                    df_지표생성 = self.종목별_지표생성(s_종목코드=s_종목코드)
                    dic_지표생성[s_종목코드] = df_지표생성

            # dic 저장
            pd.to_pickle(dic_지표생성, os.path.join(self.folder_지표생성, f'dic_지표생성_{s_일자}_{n_초봉}초봉.pkl'))

            # log 기록
            self.make_log(f'지표생성 완료({s_일자}, {n_초봉}초봉, {len(dic_지표생성):,}개 종목)')

    def 종목별_지표생성(self, s_종목코드):
        """ 종목별 매수매도 정보 생성 후 df 리턴 """
        # 기준정보 정의
        n_초봉 = self.dic_정보_지표생성['n_초봉']
        s_일자 = self.dic_정보_지표생성['s_일자']
        dic_초봉 = self.dic_정보_지표생성['dic_초봉']
        s_종목명 = self.dic_정보_지표생성['dic_종목코드2종목명'][s_종목코드]
        s_추가시점 = self.dic_정보_지표생성['dic_종목코드2추가시점'][s_종목코드]
        s_선정사유 = self.dic_정보_지표생성['dic_종목코드2선정사유'][s_종목코드]

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
        df_초봉['선정사유'] = s_선정사유

        # 미존재 데이터 처리
        for s_컬러명 in ['거래량', '매수량', '매도량', '체결강도', 'z_매수', 'z_매도', 'z_거래',
                      '만원_매수', '만원_매도', '만원_거래']:
            df_초봉.loc[df_초봉['종가'].isnull(), s_컬러명] = None

        # csv 저장
        s_폴더 = os.path.join(self.folder_지표생성, f'종목별_{s_일자}')
        os.makedirs(s_폴더, exist_ok=True)
        df_초봉.to_csv(os.path.join(s_폴더, f'df_지표생성_{s_일자}_{n_초봉}초봉_{s_종목코드}.csv'),
                     index=False, encoding='cp949')

        return df_초봉

    def 분석_분봉확인(self, n_초봉):
        """ trader 동작 시 생성된 대상종목 기준으로 초봉 데이터에 지표 추가 생성 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_지표생성'
        s_파일명_생성 = 'dic_분봉확인'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지표생성)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_분봉확인)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 초봉 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_지표생성, f'dic_지표생성_{s_일자}_{n_초봉}초봉.pkl'))

            # 분봉 불러오기
            dic_분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자}.pkl'))

            # 종목별 분석 진행
            dic_분봉확인 = dict()
            for s_종목코드 in tqdm(dic_초봉.keys(), desc=f'분봉확인-{n_초봉}초봉-{s_일자}'):
                # 데이터 확인
                df_초봉 = dic_초봉[s_종목코드].copy()
                df_분봉 = dic_분봉[s_종목코드].copy()

                # 분봉 상승 확인
                df_분봉['고가%'] = (df_분봉['고가'] / df_분봉['종가'].shift(1) - 1) * 100
                df_분봉_상승 = df_분봉[df_분봉['고가%'] > 2].copy()

                # 확인 구간 생성
                df_분봉_상승['dt일시'] = pd.to_datetime(df_분봉_상승['일자'] + ' ' + df_분봉_상승['시간'])
                df_분봉_상승['dt시작'] = df_분봉_상승['dt일시'] - pd.Timedelta(seconds=10)
                df_분봉_상승['dt종료'] = df_분봉_상승['dt일시'] + pd.Timedelta(seconds=70)

                # 초봉에 구간 표기
                df_초봉['분봉상승'] = None
                for i in range(len(df_분봉_상승)):
                    dt_시작 = df_분봉_상승['dt시작'].values[i]
                    dt_종료 = df_분봉_상승['dt종료'].values[i]
                    df_초봉.loc[df_초봉.index >= dt_시작, '분봉상승'] = True
                    df_초봉.loc[df_초봉.index > dt_종료, '분봉상승'] = None

                # dic 추가
                dic_분봉확인[s_종목코드] = df_초봉

                # csv 저장
                s_폴더 = os.path.join(self.folder_분봉확인, f'종목별_{s_일자}')
                os.makedirs(s_폴더, exist_ok=True)
                df_초봉.to_csv(os.path.join(s_폴더, f'df_분봉확인_{s_일자}_{n_초봉}초봉_{s_종목코드}.csv'),
                              index=False, encoding='cp949')

            # dic 저장
            pd.to_pickle(dic_분봉확인, os.path.join(self.folder_분봉확인, f'dic_분봉확인_{s_일자}_{n_초봉}초봉.pkl'))

            # log 기록
            self.make_log(f'분봉확인 완료({s_일자}, {n_초봉}초봉, {len(dic_분봉확인):,}개 종목)')

    def 분석_일봉변동(self, b_차트):
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
            # 일봉 불러오기 (1년치)
            s_금월 = s_일자[:6]
            li_대상월 = [re.findall(r'\d{6}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                      if 'dic_코드별_일봉_' in 파일명 and '.pkl' in 파일명]
            li_대상월 = sorted([대상월 for 대상월 in li_대상월 if 대상월 <= s_금월])[-13:]
            dic_일봉_대상월 = dict()
            for s_대상월 in li_대상월:
                dic_일봉_대상월[s_대상월] = pd.read_pickle(os.path.join(self.folder_캐시변환,
                                                                f'dic_코드별_일봉_{s_대상월}.pkl'))

            # 종목별 거래량 변동 확인
            li_df_일봉변동 = list()
            for s_종목코드 in tqdm(dic_일봉_대상월[s_금월].keys(), desc=f'일봉 변동 분석|{s_일자}'):
                # 분석대상 종목 확인 (키움 조건식 연계) => dic_분석대상 쌓기 시작한 지 얼마 안돼서 과거 데이터 예외처리 적용
                try:
                    dic_조건검색 = pd.read_pickle(os.path.join(self.folder_분석대상, f'dic_조건검색_{s_일자}.pkl'))
                    df_분석대상종목 = dic_조건검색['분석대상종목']
                    if s_종목코드 not in df_분석대상종목['종목코드'].values:
                        continue
                except FileNotFoundError:
                    pass

                # 일봉 정리
                li_df_일봉 = [dic_일봉_대상월[대상월][s_종목코드] if s_종목코드 in dic_일봉_대상월[대상월].keys()
                            else pd.DataFrame() for 대상월 in li_대상월]
                df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자')
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]

                # 일봉변동 확인
                df_일봉변동_종목 = Logic.find_일봉변동_거래량(df_일봉=df_일봉, n_윈도우=60, n_z값=3)

                # 종목 결과 list 입력
                li_df_일봉변동.append(df_일봉변동_종목)

                # 차트 생성 및 저장
                if b_차트 and len(df_일봉변동_종목) > 0:
                    # 차트 생성
                    fig = Chart.make_차트(df_ohlcv=df_일봉, n_봉수=20 * 3)

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_일봉변동, '그래프', f'일봉변동_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'일봉변동_{s_종목코드}_{s_일자}.png'))

            # 일봉 변동 종목 df 생성
            df_일봉변동 = pd.concat(li_df_일봉변동, axis=0).reset_index(drop=True)

            # df 저장
            df_일봉변동.to_pickle(os.path.join(self.folder_일봉변동, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_일봉변동.to_csv(os.path.join(self.folder_일봉변동, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'일봉 변동 분석 완료({s_일자}, {len(df_일봉변동):,}종목)')

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
    a = Analyzer(b_멀티=True, n_분석일수=None)
    li_초봉 = [1, 2, 3, 5, 10]
    [a.분석_지표생성(n_초봉=n_초봉) for n_초봉 in li_초봉]
    [a.분석_분봉확인(n_초봉=n_초봉) for n_초봉 in li_초봉]

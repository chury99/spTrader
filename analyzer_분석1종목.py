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
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_변동성종목 = dic_폴더정보['분석1종목|10_변동성종목']
        self.folder_데이터셋 = dic_폴더정보['분석1종목|20_종목_데이터셋']
        self.folder_모델 = dic_폴더정보['분석1종목|30_종목_모델']
        self.folder_성능평가 = dic_폴더정보['분석1종목|40_종목_성능평가']
        self.folder_감시대상 = dic_폴더정보['분석1종목|50_종목_감시대상']
        self.folder_감시대상모델 = dic_폴더정보['분석1종목|60_종목_모델_감시대상']
        os.makedirs(self.folder_변동성종목, exist_ok=True)
        os.makedirs(self.folder_데이터셋, exist_ok=True)
        os.makedirs(self.folder_모델, exist_ok=True)
        os.makedirs(self.folder_성능평가, exist_ok=True)
        os.makedirs(self.folder_감시대상, exist_ok=True)
        os.makedirs(self.folder_감시대상모델, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        self.li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                               if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명])
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])

        # 모델 생성 케이스 생성
        li_대기봉수 = [1, 2, 3]
        li_학습일수 = [15, 30, 60]
        li_rf_트리 = [100, 200]
        li_rf_깊이 = [10, 20]

        self.li_케이스_전체 = [[n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이] for n_대기봉수 in li_대기봉수
                          for n_학습일수 in li_학습일수 for n_rf_트리 in li_rf_트리 for n_rf_깊이 in li_rf_깊이]

        # log 기록
        self.make_log(f'### 종목 분석 시작 ###')

    def 분석_변동성확인(self):
        """ 전체 종목 분석해서 변동성이 큰 종목 선정 후 pkl, csv 저장 \n
            # 선정기준 : 10분봉 기준 3%이상 상승이 하루에 2회 이상 존재 """
        # 파일명 정의
        s_파일명_생성 = 'df_변동성종목_당일'

        # 분석대상 일자 선정
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(days=self.n_보관기간_analyzer)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')
        li_일자_전체 = [일자 for 일자 in self.li_일자_전체 if 일자 > s_기준일자]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_변동성종목)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 10분봉 데이터 불러오기 (from 캐시변환)
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))

            # 종목별 분석 진행
            li_변동성종목 = list()
            for s_종목코드 in tqdm(dic_10분봉.keys(), desc=f'변동성종목선정|{s_일자}'):
                # 분석대상 종목에 포함 여부 확인
                if s_종목코드 not in self.li_종목_분석대상:
                    continue

                # 10분봉 39개 존재 여부 확인
                df_10분봉 = dic_10분봉[s_종목코드]
                if len(df_10분봉) < 39:
                    continue

                # 상승률 생성 (10분봉 기준)
                li_상승률 = list((df_10분봉['종가'] / df_10분봉['종가'].shift(1) - 1) * 100)
                li_상승률_처음값 = [(df_10분봉['종가'].values[0] / df_10분봉['전일종가'].values[0] - 1) * 100]
                df_10분봉['상승률(%)'] = li_상승률_처음값 + li_상승률[1:]

                # 3% 이상 상승 갯수 확인
                li_상승여부 = [1 if n_상승률 >= 3 else 0 for n_상승률 in df_10분봉['상승률(%)']]
                if sum(li_상승여부) >= 2:
                    s_종목명 = self.dic_코드2종목명[s_종목코드]
                    n_상승갯수 = sum(li_상승여부)
                    li_변동성종목.append([s_종목코드, s_종목명, n_상승갯수])

            # df 저장
            df_변동성종목 = pd.DataFrame(li_변동성종목, columns=['종목코드', '종목명', '상승갯수'])
            df_변동성종목 = df_변동성종목.sort_values('상승갯수', ascending=False).reset_index(drop=True)
            df_변동성종목.to_pickle(os.path.join(self.folder_변동성종목, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_변동성종목.to_csv(os.path.join(self.folder_변동성종목, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목선정 완료({s_일자}, {len(df_변동성종목):,}종목)')

    def 분석_데이터셋(self):
        """ 변동성 종목 대상 기준으로 모델 생성을 위한 데이터 정리 후 ary set을 dic 형태로 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_변동성종목_당일'
        s_파일명_생성 = 'dic_df_데이터셋'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_변동성종목)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_데이터셋)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상일자, 대상종목 확인
            li_대상일자 = [일자 for 일자 in self.li_일자_전체 if 일자 <= s_일자][-70:]
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_변동성종목, f'df_변동성종목_당일_{s_일자}.pkl'))
            li_대상종목 = list(df_대상종목['종목코드'])

            # 종목별 10분봉 데이터 불러오기 (from 캐시변환, 과거 70일치)
            dic_li_df_종목별 = dict()
            for s_대상일자 in tqdm(li_대상일자, desc=f'10분봉 읽어오기({s_일자})'):
                dic_10분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_대상일자}.pkl'))
                for s_종목코드 in li_대상종목:
                    try:
                        dic_li_df_종목별[s_종목코드].append(dic_10분봉[s_종목코드])
                    except KeyError:
                        dic_li_df_종목별[s_종목코드] = [dic_10분봉.get(s_종목코드, pd.DataFrame())]

            # 종목별 10분봉 데이터 합치기
            dic_df_10분봉 = dict()
            for s_종목코드 in dic_li_df_종목별.keys():
                dic_df_10분봉[s_종목코드] = pd.concat(dic_li_df_종목별[s_종목코드], axis=0).sort_index()

            # 분석용 데이터셋 생성
            dic_df_데이터셋 = dict()
            for s_종목코드 in tqdm(li_대상종목, desc=f'데이터셋 생성({s_일자})'):
                df_10분봉 = dic_df_10분봉[s_종목코드].dropna()
                df_데이터셋 = Logic.trd_make_추가데이터_종목모델_rf(df=df_10분봉)
                dic_df_데이터셋[s_종목코드] = df_데이터셋

            # 데이터셋 저장
            pd.to_pickle(dic_df_데이터셋, os.path.join(self.folder_데이터셋, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'데이터셋 준비 완료({s_일자})')

    def 분석_모델생성(self):
        """ 변동성 종목 대상 기준으로 종목별 모델 생성 후 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_df_데이터셋'
        s_파일명_생성 = 'dic_종목모델'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_데이터셋)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_모델)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_변동성종목, f'df_변동성종목_당일_{s_일자}.pkl'))
            li_대상종목 = list(df_대상종목['종목코드'])

            # 데이터셋 불러오기
            dic_df_데이터셋 = pd.read_pickle(os.path.join(self.folder_데이터셋, f'dic_df_데이터셋_{s_일자}.pkl'))

            # [ 당일 모델 생성 ] ########################################################################################

            # 종목별 모델 생성 (당일)
            dic_모델 = dict()
            for s_종목코드 in tqdm(li_대상종목, desc=f'종목모델 생성(당일-{s_일자})'):
                # 해당 데이터셋 설정
                df_데이터셋 = dic_df_데이터셋[s_종목코드]

                dic_모델_케이스 = dict()
                for li_케이스 in self.li_케이스_전체:
                    # 케이스 설정
                    n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = li_케이스

                    # 라벨 데이터 생성 (대기봉수 설정)
                    df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                    # 입력용 xy로 변경 (학습일수 설정)
                    dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=n_학습일수)
                    # 데이터셋 미존재 시 종료 (데이터량 부족)
                    if dic_데이터셋 is None:
                        continue
                    # 모델 생성 (rf 트리수, rf 깊이 설정)
                    obj_모델 = Logic.make_모델_rf(dic_데이터셋=dic_데이터셋, n_rf_트리=n_rf_트리, n_rf_깊이=n_rf_깊이)

                    # 케이스별 모델 저장
                    s_케이스 = '_'.join(str(n) for n in li_케이스)
                    dic_모델_케이스[s_케이스] = obj_모델

                # 모델 등록
                dic_모델[s_종목코드] = dic_모델_케이스

            # [ 전일 모델 생성 ] ########################################################################################

            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in self.li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 전일 모델 불러오기
            try:
                dic_모델_전일 = pd.read_pickle(os.path.join(self.folder_모델, f'{s_파일명_생성}_{s_일자_전일}.pkl'))
            except FileNotFoundError:
                dic_모델_전일 = dict()

            # 전일 모델에 미존재 하는 종목코드 찾기
            li_대상종목_전일 = [종목코드 for 종목코드 in li_대상종목 if 종목코드 not in dic_모델_전일.keys()]

            for s_종목코드 in tqdm(li_대상종목_전일, desc=f'종목모델 생성(전일-{s_일자_전일})'):
                # 해당 데이터셋 설정 (전일까지만)
                df_데이터셋 = dic_df_데이터셋[s_종목코드]
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] < s_일자]

                dic_모델_케이스_전일 = dict()
                for li_케이스 in self.li_케이스_전체:
                    # 케이스 설정
                    n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = li_케이스

                    # 라벨 데이터 생성 (대기봉수 설정)
                    df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                    # 입력용 xy로 변경 (학습일수 설정)
                    dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=n_학습일수)
                    # 데이터셋 미존재 시 종료 (데이터량 부족)
                    if dic_데이터셋 is None:
                        continue
                    # 모델 생성 (rf 트리수, rf 깊이 설정)
                    obj_모델 = Logic.make_모델_rf(dic_데이터셋=dic_데이터셋, n_rf_트리=n_rf_트리, n_rf_깊이=n_rf_깊이)

                    # 케이스별 모델 저장
                    s_케이스 = '_'.join(str(n) for n in li_케이스)
                    dic_모델_케이스_전일[s_케이스] = obj_모델

                # 모델 등록 (전일)
                dic_모델_전일[s_종목코드] = dic_모델_케이스_전일

            # 모델 저장 (전일 모델)
            pd.to_pickle(dic_모델_전일, os.path.join(self.folder_모델, f'{s_파일명_생성}_{s_일자_전일}.pkl'))
            self.make_log(f'종목모델 생성 완료(전일-{s_일자_전일}, {len(li_대상종목_전일):,}개 종목)')

            # 모델 저장 (당일 모델)
            pd.to_pickle(dic_모델, os.path.join(self.folder_모델, f'{s_파일명_생성}_{s_일자}.pkl'))
            self.make_log(f'종목모델 생성 완료(당일-{s_일자}, {len(li_대상종목):,}개 종목)')

    def 분석_성능평가(self):
        """ 전일 생성된 모델 기반으로 금일 데이터로 예측 결과 확인하여 평가결과 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_종목모델'
        s_파일명_생성 = 'dic_df_평가_성공여부'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_모델)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_성능평가)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 데이터셋 및 전일 모델 불러오기
            dic_df_데이터셋 = pd.read_pickle(os.path.join(self.folder_데이터셋, f'dic_df_데이터셋_{s_일자}.pkl'))
            dic_모델_전일 = pd.read_pickle(os.path.join(self.folder_모델, f'dic_종목모델_{s_일자_전일}.pkl'))
            li_대상종목 = list(dic_df_데이터셋.keys())

            # 종목별 성능 평가 진행
            dic_df_평가_케이스 = dict()
            dic_df_평가_성공여부 = dict()
            for s_종목코드 in tqdm(li_대상종목, desc=f'종목모델 성능 평가({s_일자})'):
                # 모델 설정
                try:
                    dic_모델_케이스_전일 = dic_모델_전일[s_종목코드]
                except KeyError:
                    continue

                # 데이터셋 설정
                df_데이터셋 = dic_df_데이터셋[s_종목코드]

                # 케이스별 모델 평가
                df_평가_케이스 = df_데이터셋.loc[:, '일자': '거래량'].copy()
                df_평가_케이스 = df_평가_케이스[df_평가_케이스['일자'] == s_일자]
                li_li_평가_성공여부 = list()
                for s_케이스 in dic_모델_케이스_전일.keys():
                    # 케이스 설정
                    li_케이스 = [int(조건) for 조건 in s_케이스.split('_')]
                    n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = li_케이스

                    # 모델 설정
                    obj_모델_전일 = dic_모델_케이스_전일[s_케이스]

                    # 데이터셋 설정 (평가용 데이터, 당일 데이터만)
                    df_데이터셋_케이스 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                    df_데이터셋_케이스 = df_데이터셋_케이스[df_데이터셋_케이스['일자'] == s_일자]
                    if len(df_데이터셋_케이스) == 0:
                        continue

                    # 입력용 ary 설정 (1일치 데이터)
                    dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋_케이스, n_학습일수=1)
                    ary_x_평가 = dic_데이터셋['ary_x_학습']
                    ary_y_정답 = dic_데이터셋['ary_y_학습']

                    # 상승확률, 정답 산출
                    s_col_확률 = f'상승확률(%)_{s_케이스}'
                    s_col_정답 = f'정답_{s_케이스}'
                    try:
                        df_평가_케이스[s_col_확률] = obj_모델_전일.predict_proba(ary_x_평가)[:, 1] * 100
                    except IndexError:
                        df_평가_케이스[s_col_확률] = 0
                    df_평가_케이스[s_col_정답] = ary_y_정답

                    # 케이스별 성능 평가
                    df_평가_성능 = df_평가_케이스.loc[:, [s_col_확률, s_col_정답]].copy()
                    df_평가_성능 = df_평가_성능.sort_values(s_col_확률, ascending=False).reset_index(drop=True)
                    df_평가_성능['누적_정답'] = df_평가_성능[s_col_정답].cumsum()
                    df_성공 = df_평가_성능[df_평가_성능['누적_정답'] == df_평가_성능.index + 1]

                    n_예측성공 = len(df_성공) if len(df_성공) > 0 else 0
                    n_확률스펙 = df_성공[s_col_확률].min() if len(df_성공) > 0 else None
                    s_종목명 = df_평가_케이스['종목명'].values[0] if len(df_평가_케이스) > 0 else None

                    li_평가_성공여부 = [s_종목코드, s_종목명, s_일자, n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이,
                                  n_예측성공, n_확률스펙]
                    li_li_평가_성공여부.append(li_평가_성공여부)

                # 케이스별 평가 결과 입력
                dic_df_평가_케이스[s_종목코드] = df_평가_케이스

                # 성능 평가 결과 입력
                li_컬럼명 = ['종목코드', '종목명', '일자', '대기봉수', '학습일수', 'rf_트리', 'rf_깊이', '예측성공', '확률스펙']
                df_평가_성공여부 = pd.DataFrame(li_li_평가_성공여부, columns=li_컬럼명)
                dic_df_평가_성공여부[s_종목코드] = df_평가_성공여부

            # 평가 결과 저장
            pd.to_pickle(dic_df_평가_케이스, os.path.join(self.folder_성능평가, f'dic_df_평가_케이스_{s_일자}.pkl'))
            pd.to_pickle(dic_df_평가_성공여부, os.path.join(self.folder_성능평가, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'성능평가 완료({s_일자}, {len(dic_df_평가_성공여부):,}개 종목)')

    def 선정_감시대상(self):
        """ 모델평가 결과를 바탕으로 trader에서 실시간 감시할 종목 선정 후 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_df_평가_성공여부'
        s_파일명_생성 = 'df_감시대상'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_성능평가)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 평가 결과 불러오기
            dic_df_평가_성공여부 = pd.read_pickle(os.path.join(self.folder_성능평가, f'dic_df_평가_성공여부_{s_일자}.pkl'))

            # 적합한 종목 및 스펙 선정
            df_감시대상 = pd.DataFrame()
            for s_종목코드 in tqdm(dic_df_평가_성공여부.keys(), desc=f'감시대상 선정({s_일자})'):
                df_평가 = dic_df_평가_성공여부[s_종목코드]
                df_추가 = df_평가[df_평가['확률스펙'] > 50]
                if len(df_추가) == 0:
                    continue
                df_추가 = df_추가.sort_values(['예측성공', '대기봉수', '확률스펙'], ascending=[False, True, False])
                df_추가 = df_추가.reset_index(drop=True).loc[:0, :]

                df_감시대상 = pd.concat([df_감시대상, df_추가], axis=0)

            if len(df_감시대상) > 0:
                df_감시대상 = df_감시대상.sort_values(['예측성공', '대기봉수', '확률스펙'], ascending=[False, True, False])
            df_감시대상 = df_감시대상.reset_index(drop=True)

            # 평가 결과 저장
            df_감시대상.to_pickle(os.path.join(self.folder_감시대상, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_감시대상.to_csv(os.path.join(self.folder_감시대상, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'감시대상 선정 완료({s_일자}, {len(df_감시대상):,}개 종목)')

    def 모델_감시대상(self):
        """ 생성된 모델 중 감시대상에 해당하는 종목, 케이스만 골라서 별도 파일로 저장 (이후 동작 시 속도 향상 목적) """
        # 파일명 정의
        s_파일명_기준 = 'df_감시대상'
        s_파일명_생성 = 'dic_종목모델_감시대상'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상모델)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 감시대상 종목 불러오기
            df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_일자}.pkl'))

            dic_모델_감시대상 = dict()
            if len(df_감시대상) > 0:
                # 케이스 처리
                li_li_케이스 = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': 'rf_깊이'].values]
                df_감시대상['케이스'] = ['_'.join(str(n) for n in li) for li in li_li_케이스]
                dic_종목코드2케이스 = df_감시대상.set_index('종목코드').to_dict()['케이스']

                # 당일 모델 불러오기
                dic_모델 = pd.read_pickle(os.path.join(self.folder_모델, f'dic_종목모델_{s_일자}.pkl'))

                # 사용할 모델만 골라내기
                for s_종목코드 in tqdm(df_감시대상['종목코드'], desc=f'감시대상 모델 선정({s_일자})'):
                    s_케이스 = dic_종목코드2케이스[s_종목코드]
                    obj_모델 = dic_모델[s_종목코드][s_케이스]
                    dic_모델_감시대상[s_종목코드] = {s_케이스: obj_모델}

            # 모델 저장
            pd.to_pickle(dic_모델_감시대상, os.path.join(self.folder_감시대상모델, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'감시대상 모델 별도 저장 완료({s_일자}, {len(dic_모델_감시대상):,}개 종목)')

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

    a.분석_변동성확인()
    a.분석_데이터셋()
    a.분석_모델생성()
    a.분석_성능평가()
    a.선정_감시대상()
    a.모델_감시대상()

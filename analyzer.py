import os
import sys
import pandas as pd
import json

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
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_ohlcv = os.path.join(folder_데이터, 'ohlcv')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')
        folder_분석 = os.path.join(folder_work, '분석')
        self.folder_변동성종목 = os.path.join(folder_분석, '10_변동성종목')
        self.folder_데이터셋 = os.path.join(folder_분석, '20_데이터셋')
        self.folder_모델 = os.path.join(folder_분석, '30_모델')
        self.folder_성능평가 = os.path.join(folder_분석, '40_성능평가')
        self.folder_감시대상 = os.path.join(folder_분석, '감시대상')
        os.makedirs(self.folder_변동성종목, exist_ok=True)
        os.makedirs(self.folder_데이터셋, exist_ok=True)
        os.makedirs(self.folder_모델, exist_ok=True)
        os.makedirs(self.folder_성능평가, exist_ok=True)
        os.makedirs(self.folder_감시대상, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        self.li_일자_전체 = sorted([파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_캐시변환)
                                if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명])

        # log 기록
        self.make_log(f'### 종목 분석 시작 ###')

    def 분석_변동성확인(self):
        """ 전체 종목 분석해서 변동성이 큰 종목 선정 후 pkl, csv 저장 \n
            # 선정기준 : 10분봉 기준 3%이상 상승이 하루에 2회 이상 존재 """
        # 분석대상 일자 선정
        li_일자_전체 = self.li_일자_전체[-180:]
        li_일자_완료 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_변동성종목)
                    if 'df_변동성종목_당일_' in 파일명 and '.pkl' in 파일명]
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
            df_변동성종목.to_pickle(os.path.join(self.folder_변동성종목, f'df_변동성종목_당일_{s_일자}.pkl'))
            df_변동성종목.to_csv(os.path.join(self.folder_변동성종목, f'변동성종목_당일_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목선정 완료({s_일자}, {len(df_변동성종목):,}종목)')

    def 분석_데이터셋(self, s_모델):
        """ 변동성 종목 대상 기준으로 모델 생성을 위한 데이터 정리 후 ary set을 dic 형태로 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_변동성종목)
                    if 'df_변동성종목_당일_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [파일명.split('_')[4].replace('.pkl', '') for 파일명 in os.listdir(self.folder_데이터셋)
                    if f'dic_df_데이터셋_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상일자, 대상종목 확인
            li_대상일자 = [일자 for 일자 in self.li_일자_전체 if 일자 <= s_일자][-61:]
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_변동성종목, f'df_변동성종목_당일_{s_일자}.pkl'))
            li_대상종목 = list(df_대상종목['종목코드'])

            # 종목별 10분봉 데이터 불러오기 (from 캐시변환, 과거 61일치)
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
            # pd.to_pickle(dic_df_10분봉, os.path.join(self.folder_데이터셋, '임시_dic_df_10분봉.pkl'))   ### 테스트용 임시 코드
            # dic_df_10분봉 = pd.read_pickle(os.path.join(self.folder_데이터셋, '임시_dic_df_10분봉.pkl'))   ### 테스트용 임시 코드

            # 분석용 데이터셋 생성
            dic_df_데이터셋 = dict()
            for s_종목코드 in tqdm(li_대상종목, desc=f'데이터셋 생성({s_일자})'):
                df_10분봉 = dic_df_10분봉[s_종목코드].dropna()
                # df_데이터셋 = None
                # if s_모델 == 'rf':
                #     df_데이터셋 = Logic.make_추가데이터_rf(df=df_10분봉)
                df_데이터셋 = Logic.make_추가데이터_rf(df=df_10분봉) if s_모델 == 'rf'\
                    else Logic.make_추가데이터_lstm(df=df_10분봉) if s_모델 == 'lstm'\
                    else None
                dic_df_데이터셋[s_종목코드] = df_데이터셋

            # 데이터셋 저장
            pd.to_pickle(dic_df_데이터셋, os.path.join(self.folder_데이터셋, f'dic_df_데이터셋_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'데이터셋 준비 완료({s_일자}, {s_모델})')

    def 분석_모델생성(self, s_모델):
        """ 변동성 종목 대상 기준으로 종목별 모델 생성 후 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [파일명.split('_')[4].replace('.pkl', '') for 파일명 in os.listdir(self.folder_데이터셋)
                    if f'dic_df_데이터셋_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_모델)
                    if f'dic_모델_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 대상종목 불러오기
            df_대상종목 = pd.read_pickle(os.path.join(self.folder_변동성종목, f'df_변동성종목_당일_{s_일자}.pkl'))
            li_대상종목 = list(df_대상종목['종목코드'])

            # 데이터셋 불러오기
            dic_df_데이터셋 = pd.read_pickle(os.path.join(self.folder_데이터셋, f'dic_df_데이터셋_{s_모델}_{s_일자}.pkl'))

            # 종목별 모델 생성 (당일)
            dic_모델 = dict()
            for s_종목코드 in tqdm(li_대상종목, desc=f'{s_모델} 모델 생성({s_일자})'):
                # 해당 데이터셋 설정
                df_데이터셋 = dic_df_데이터셋[s_종목코드]

                # 모델 생성
                obj_모델 = None

                if s_모델 == 'rf':
                    # 라벨 데이터 생성 (대기봉수 설정)
                    df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋)
                    # 입력용 xy로 변경 (학습일수 설정)
                    dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋)
                    # 데이터셋 미존재 시 종료 (데이터량 부족)
                    if dic_데이터셋 is None:
                        continue
                    # 모델 생성 (rf 트리수, rf 깊이 설정)
                    obj_모델 = Logic.make_모델_rf(dic_데이터셋=dic_데이터셋)

                # 모델 등록
                dic_모델[s_종목코드] = obj_모델

            # 모델 저장
            pd.to_pickle(dic_모델, os.path.join(self.folder_모델, f'dic_모델_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'모델 생성 완료({s_일자}, {len(li_대상종목):,}개 종목, {s_모델})')

            # [ 전일 모델 생성 ] ########################################################################################

            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 전일 모델 불러오기
            dic_모델_전일 = pd.read_pickle(os.path.join(self.folder_모델, f'dic_모델_{s_모델}_{s_일자_전일}.pkl'))

            # 전일 모델에 미존재 하는 종목코드 찾기
            li_대상종목_전일 = [종목코드 for 종목코드 in li_대상종목 if 종목코드 not in dic_모델_전일.keys()]

            for s_종목코드 in tqdm(li_대상종목_전일, desc=f'{s_모델} 모델 생성(전일-{s_일자_전일})'):
                # 해당 데이터셋 설정 (전일까지만)
                df_데이터셋 = dic_df_데이터셋[s_종목코드]
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] < s_일자]

                # 모델 생성
                obj_모델 = None

                if s_모델 == 'rf':
                    # 라벨 데이터 생성 (대기봉수 설정)
                    df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋)
                    # 입력용 xy로 변경 (학습일수 설정)
                    dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋)
                    # 데이터셋 미존재 시 종료 (데이터량 부족)
                    if dic_데이터셋 is None:
                        continue
                    # 모델 생성 (rf 트리수, rf 깊이 설정)
                    obj_모델 = Logic.make_모델_rf(dic_데이터셋=dic_데이터셋)

                # 모델 등록 (전일)
                dic_모델_전일[s_종목코드] = obj_모델

            # 모델 저장 (전일)
            pd.to_pickle(dic_모델_전일, os.path.join(self.folder_모델, f'dic_모델_{s_모델}_{s_일자_전일}.pkl'))

            # log 기록
            self.make_log(f'모델 생성 완료(전일-{s_일자_전일}, {len(li_대상종목_전일):,}개 종목, {s_모델})')

    def 분석_성능평가(self, s_모델):
        """ 전일 생성된 모델 기반으로 금일 데이터로 예측 결과 확인하여 평가결과 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_모델)
                    if f'dic_모델_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_성능평가)
                    if f'df_성능평가_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = min([일자_전체 for 일자_전체 in li_일자_전체 if 일자_전체 < s_일자])
            except ValueError:
                continue

            # 데이터셋 및 전일 모델 불러오기
            dic_df_데이터셋 = pd.read_pickle(os.path.join(self.folder_데이터셋, f'dic_df_데이터셋_{s_모델}_{s_일자}.pkl'))
            dic_모델_전일 = pd.read_pickle(os.path.join(self.folder_모델, f'dic_모델_{s_모델}_{s_일자_전일}.pkl'))
            li_대상종목 = list(dic_df_데이터셋.keys())

            # 종목별 성능 평가 진행
            dic_df_평가상세 = dict()
            li_li_결과 = list()
            for s_종목코드 in tqdm(li_대상종목, desc=f'{s_모델} 성능 평가({s_일자})'):
                # 모델 설정
                try:
                    obj_모델_전일 = dic_모델_전일[s_종목코드]
                except KeyError:
                    continue

                # 데이터셋 설정 (평가용 데이터, 당일 데이터만)
                df_데이터셋 = dic_df_데이터셋[s_종목코드]
                df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋)
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] == s_일자]
                if len(df_데이터셋) == 0:
                    continue

                # 입력용 ary 설정
                dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=1)
                ary_x_평가 = dic_데이터셋['ary_x_학습']
                ary_y_정답 = dic_데이터셋['ary_y_학습']

                # 모델 평가
                df_평가상세 = df_데이터셋.loc[:, '일자': '거래량'].copy()
                try:
                    df_평가상세['상승확률(%)'] = obj_모델_전일.predict_proba(ary_x_평가)[:, 1] * 100
                except IndexError:
                    df_평가상세['상승확률(%)'] = 0
                df_평가상세['예측'] = (df_평가상세['상승확률(%)'] > 60).astype(int)
                df_평가상세['정답'] = ary_y_정답

                dic_df_평가상세[s_종목코드] = df_평가상세

                # 상세 결과 저장
                folder_평가상세 = os.path.join(self.folder_성능평가, f'평가상세_{s_일자}')
                os.makedirs(folder_평가상세, exist_ok=True)
                df_평가상세.to_csv(os.path.join(folder_평가상세, f'평가상세_{s_종목코드}_{s_모델}_{s_일자}.csv'),
                               index=False, encoding='cp949')

                # 결과 정리
                df_결과 = df_평가상세[df_평가상세['예측'] == 1]
                n_상승예측 = len(df_결과)
                n_예측성공 = df_결과['정답'].sum()
                n_예측실패 = n_상승예측 - n_예측성공

                li_결과 = [s_종목코드, n_상승예측, n_예측성공, n_예측실패]
                li_li_결과.append(li_결과)

            # 결과 df로 정리
            df_성능평가 = pd.DataFrame(li_li_결과, columns=['종목코드', '상승예측', '예측성공', '예측실패'])

            # 결과 저장
            pd.to_pickle(dic_df_평가상세, os.path.join(self.folder_성능평가, f'dic_df_평가상세_{s_모델}_{s_일자}.pkl'))
            df_성능평가.to_pickle(os.path.join(self.folder_성능평가, f'df_성능평가_{s_모델}_{s_일자}.pkl'))
            df_성능평가.to_csv(os.path.join(self.folder_성능평가, f'성능평가_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'성능평가 완료(전일 모델, 금일 데이터_{s_일자}, {s_모델})')

    def 선정_감시대상(self):
        """ 모델평가 결과를 바탕으로 trader에서 실시간 감시할 종목 선정 후 저장 """
        pass

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
    a.분석_데이터셋(s_모델='rf')
    a.분석_모델생성(s_모델='rf')
    a.분석_성능평가(s_모델='rf')

import os
import sys
import pandas as pd
import json
import re

import matplotlib.pyplot as plt
from tqdm import tqdm

import analyzer_알고리즘 as Logic

# 그래프 한글 설정
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


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
        self.folder_감시대상 = dic_폴더정보['분석1종목|50_종목_감시대상']
        self.folder_종목모델 = dic_폴더정보['분석1종목|60_종목_모델_감시대상']
        self.folder_종목준비 = dic_폴더정보['백테스팅|10_종목준비']
        self.folder_종목데이터셋 = dic_폴더정보['백테스팅|20_데이터셋_종목']
        self.folder_종목매수검증 = dic_폴더정보['백테스팅|30_매수검증_종목']
        self.folder_공통데이터셋 = dic_폴더정보['백테스팅|40_데이터셋_공통']
        self.folder_공통매수검증 = dic_폴더정보['백테스팅|50_매수검증_공통']
        self.folder_매도검증 = dic_폴더정보['백테스팅|60_매도검증']
        self.folder_수익검증 = dic_폴더정보['백테스팅|70_수익검증']
        os.makedirs(self.folder_종목준비, exist_ok=True)
        os.makedirs(self.folder_종목데이터셋, exist_ok=True)
        os.makedirs(self.folder_종목매수검증, exist_ok=True)
        # os.makedirs(self.folder_공통데이터셋, exist_ok=True)
        # os.makedirs(self.folder_공통매수검증, exist_ok=True)

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 백테스팅_종목준비(self, s_모델):
        """ 감시대상 종목 불러와서 종목조건, 분봉, 10분봉 데이터 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 감시대상 불러오기 (전일 기준 - 전일 생성한 데이터 불러와서 당일 감시)
            df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_모델}_{s_일자_전일}.pkl'))
            df_종목조건 = df_감시대상.copy()
            li_li_종목조건 = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': '확률스펙'].values]
            df_감시대상['li_조건'] = [[int(li_종목조건[i]) if i < 5 else li_종목조건[i] for i in range(len(li_종목조건))]
                                for li_종목조건 in li_li_종목조건]
            try:
                dic_종목조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
            except KeyError:
                dic_종목조건 = dict()

            # 분봉 데이터 불러오기 (당일 기준 - 매수/매도 검증 시 사용)
            dic_분봉 = dict()
            dic_분봉_전체 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자}.pkl'))
            for s_종목코드 in dic_종목조건.keys():
                df_분봉 = dic_분봉_전체[s_종목코드]
                df_분봉['일자시간'] = df_분봉['일자'] + ' ' + df_분봉['시간']
                df_분봉['일자시간'] = pd.to_datetime(df_분봉['일자시간'], format='%Y%m%d %H:%M:%S')
                df_분봉 = df_분봉.set_index(keys='일자시간').sort_index(ascending=True)
                dic_분봉[s_종목코드] = df_분봉

            # 10분봉 데이터 불러오기 (당일 + 전일 기준 - 실제는 10분봉 tr 조회 후 리턴값 사용)
            dic_10분봉 = dict()
            dic_10분봉_전체_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_10분봉_전체_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))
            for s_종목코드 in dic_종목조건.keys():
                df_10분봉_당일 = dic_10분봉_전체_당일[s_종목코드]
                df_10분봉_전일 = dic_10분봉_전체_전일[s_종목코드]
                df_10분봉 = pd.concat([df_10분봉_전일, df_10분봉_당일], axis=0).sort_index()
                dic_10분봉[s_종목코드] = df_10분봉

            # 결과 저장
            pd.to_pickle(dic_종목조건, os.path.join(self.folder_종목준비, f'dic_종목조건_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(df_종목조건, os.path.join(self.folder_종목준비, f'df_종목조건_{s_모델}_{s_일자}.pkl'))
            df_종목조건.to_csv(os.path.join(self.folder_종목준비, f'df_종목조건_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')
            pd.to_pickle(dic_분봉, os.path.join(self.folder_종목준비, f'dic_분봉_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(dic_10분봉, os.path.join(self.folder_종목준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'종목준비 완료({s_일자}, {len(dic_10분봉):,}개 종목, {s_모델})')

    def 백테스팅_데이터셋_종목(self, s_모델):
        """ 10분봉 데이터 기준으로 모델 입력용 추가 데이터 생성 후 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목데이터셋)
                    if f'dic_데이터셋_종목_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 데이터 파일 불러오기
            dic_종목조건 = pd.read_pickle(os.path.join(self.folder_종목준비, f'dic_종목조건_{s_모델}_{s_일자}.pkl'))
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_종목준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))

            # 종목모델 데이터셋 생성
            dic_데이터셋_종목 = dict()
            for s_종목코드 in dic_종목조건.keys():
                # 데이터셋 생성
                df_10분봉 = dic_10분봉[s_종목코드]
                df_데이터셋 = Logic.make_추가데이터_rf(df=df_10분봉)
                dic_데이터셋_종목[s_종목코드] = df_데이터셋

            # 결과 저장
            pd.to_pickle(dic_데이터셋_종목, os.path.join(self.folder_종목데이터셋, f'dic_데이터셋_종목_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'공통 데이터셋 준비 완료({s_일자}, {s_모델})')

    def 백테스팅_매수검증_종목(self, s_모델):
        """ 10분 단위로 종목모델 검증 후 결과 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목데이터셋)
                    if f'dic_데이터셋_종목_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목매수검증)
                    if f'df_매수검증_공통_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 데이터 파일 불러오기
            dic_종목조건 = pd.read_pickle(os.path.join(self.folder_종목준비, f'dic_종목조건_{s_모델}_{s_일자}.pkl'))
            dic_데이터셋 = pd.read_pickle(os.path.join(self.folder_종목데이터셋, f'dic_데이터셋_종목_{s_모델}_{s_일자}.pkl'))
            dic_종목모델 = pd.read_pickle(os.path.join(self.folder_종목모델, f'dic_모델_감시대상_{s_모델}_{s_일자_전일}.pkl'))

            # 종목별 케이스 정보 생성
            dic_케이스 = dict()
            for s_종목코드 in dic_종목조건.keys():
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이, n_예측성공, n_확률스펙 = dic_종목조건[s_종목코드]
                dic_케이스[s_종목코드] = f'{n_대기봉수}_{n_학습일수}_{n_rf_트리}_{n_rf_깊이}'

            # 기준시간 생성
            try:
                s_코드 = max(dic_데이터셋, key=lambda x: len(dic_데이터셋[x]))
                li_시간 = list(dic_데이터셋[s_코드]['시간'].unique())
            except ValueError:
                li_시간 = []

            # 매수검증 진행
            li_df_매수검증 = []
            for s_시간 in tqdm(li_시간, desc=f'매수검증 - 종목모델 ({s_일자})'):
                for s_종목코드 in dic_종목조건.keys():
                    # 데이터 불러오기
                    df_데이터셋 = dic_데이터셋[s_종목코드]
                    s_케이스 = dic_케이스[s_종목코드]
                    obj_종목모델 = dic_종목모델[s_종목코드][s_케이스]

                    # 필요 데이터 골라내기 (tr 조회해서 가공한 df와 동일)
                    dt_시점 = pd.Timestamp(f'{s_일자} {s_시간}')
                    df_데이터셋_tr = df_데이터셋[df_데이터셋.index < dt_시점]
                    ary_x = Logic.make_1개검증용x_rf(df=df_데이터셋_tr)

                    # 상승확률 산출
                    n_상승확률 = obj_종목모델.predict_proba(ary_x)[:, 1][0] * 100

                    # 데이터 입력
                    df_검증 = df_데이터셋.loc[:, : '거래량ma120']
                    df_검증 = df_검증[df_검증.index == dt_시점].copy()
                    df_검증['종목확률(%)'] = n_상승확률
                    li_df_매수검증.append(df_검증)

            # df 정리 (데이터 없을 시 df에 None 출력)
            if len(li_df_매수검증) == 0:
                li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_종목매수검증)
                          if f'df_매수검증_종목_{s_모델}_' in 파일명 and '.pkl' in 파일명]
                s_참고파일 = max(li_파일명)
                df_매수검증 = pd.read_pickle(os.path.join(self.folder_종목매수검증, s_참고파일))
                df_매수검증 = df_매수검증.drop(df_매수검증.index)
                df_매수검증.loc[0] = None
            else:
                df_매수검증 = pd.concat(li_df_매수검증, axis=0).drop_duplicates()

            # 결과 저장
            pd.to_pickle(df_매수검증, os.path.join(self.folder_종목매수검증, f'df_매수검증_종목_{s_모델}_{s_일자}.pkl'))
            df_매수검증.to_csv(os.path.join(self.folder_종목매수검증, f'df_매수검증_종목_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목모델 매수검증 완료({s_일자}, {s_모델})')

    def 백테스팅_데이터셋_공통(self, s_모델):
        """ 공통모델 결과를 바탕으로 종목모델 검증 후 결과 저장 """
        # 분석2공통에서 진행한 데이터셋과 동일하게 만들 것
        # 공통모델 중 ok 종목만 대상
        # 모델 조건 기준 판정
        # 시간별 판정 결과 df 저장
        pass

    def 백테스팅_매수검증_공통(self, s_모델):
        pass

    def 백테스팅_매도검증(self, s_모델):
        """ 1분봉 기준 매도 조건 검증 후 결과 저장 """
        # 공통/종목모델 모두 ok인 항목 대상
        # 1분봉 기준 h가 +3% 이상이면 +2.5%
        # 1분봉 기준 l이 -3% 이하이면 -3.5%
        # 결과는 df로 저장 (매수시점, 매도시점 표기)
        pass

    def 백테스팅_수익검증(self, s_모델):
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

    a.백테스팅_종목준비(s_모델='rf')
    a.백테스팅_데이터셋_종목(s_모델='rf')
    a.백테스팅_매수검증_종목(s_모델='rf')
    a.백테스팅_데이터셋_공통(s_모델='rf')
    a.백테스팅_매수검증_공통(s_모델='rf')
    a.백테스팅_매도검증(s_모델='rf')
    a.백테스팅_수익검증(s_모델='rf')

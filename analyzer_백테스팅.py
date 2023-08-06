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
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')
        folder_분석 = os.path.join(folder_work, '분석')
        self.folder_모델 = os.path.join(folder_분석, '30_모델')
        self.folder_감시대상 = os.path.join(folder_분석, '감시대상')
        folder_백테스팅 = os.path.join(folder_work, '백테스팅')
        self.folder_상승예측 = os.path.join(folder_백테스팅, '10_상승예측')
        self.folder_수익검증 = os.path.join(folder_백테스팅, '20_수익검증')
        os.makedirs(self.folder_상승예측, exist_ok=True)
        os.makedirs(self.folder_수익검증, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        self.li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                                if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명])

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 백테스팅_상승예측(self, s_모델):
        """ 감시대상 종목 불러와서 10분봉 기준 상승여부 예측 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
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
            df_감시대상['li_조건'] = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': '확률스펙'].values]
            try:
                dic_조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
                li_감시대상 = list(df_감시대상['종목코드'].values)
            except KeyError:
                dic_조건 = dict()
                li_감시대상 = list()

            # 모델 불러오기 (전일 기준 - 전일 생성한 모델 불러와서 당일 적용)
            dic_모델 = pd.read_pickle(os.path.join(self.folder_모델, f'dic_모델_{s_모델}_{s_일자_전일}.pkl'))

            # 10분봉 불러오기 (당일 기준)
            dic_df_10분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_df_10분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))

            # 종목별 상승 예측값 생성
            dic_df_상승예측 = dict()
            for s_종목코드 in tqdm(li_감시대상, desc=f'{s_모델} 상승 예측값 생성({s_일자})'):
                # 조건 설정
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이, n_예측성공, n_확률스펙 = dic_조건[s_종목코드]
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = int(n_대기봉수), int(n_학습일수), int(n_rf_트리), int(n_rf_깊이)
                s_케이스 = f'{n_대기봉수}_{n_학습일수}_{n_rf_트리}_{n_rf_깊이}'

                # 모델 설정 (데이터 부족 등 이유로 모델 미생성된 종목은 예외처리)
                try:
                    obj_모델 = dic_모델[s_종목코드][s_케이스]
                except KeyError:
                    continue

                # 데이터셋 준비 (추가 데이터 생성, 라벨 생성, 당일만 잘라내기, 입력용 ary로 변환)
                df_10분봉 = pd.concat([dic_df_10분봉_전일[s_종목코드], dic_df_10분봉_당일[s_종목코드]], axis=0).sort_index()
                df_데이터셋 = Logic.make_추가데이터_rf(df=df_10분봉)
                df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] == s_일자]
                if len(df_데이터셋) == 0:
                    continue
                dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=1)
                ary_x_평가 = dic_데이터셋['ary_x_학습']
                ary_y_정답 = dic_데이터셋['ary_y_학습']

                # 상승확률 및 상승예측 생성 (10분봉 데이터를 당일 데이터만 잘라낸 후, 상승확률 및 상승예측 추가)
                df_10분봉 = df_10분봉[df_10분봉['일자'] == s_일자]
                try:
                    df_10분봉['상승확률(%)'] = obj_모델.predict_proba(ary_x_평가)[:, 1] * 100
                except IndexError:
                    df_10분봉['상승확률(%)'] = 0
                df_10분봉['상승예측'] = (df_10분봉['상승확률(%)'] > n_확률스펙) * 1
                df_10분봉['정답'] = ary_y_정답

                # dic_df에 입력
                dic_df_상승예측[s_종목코드] = df_10분봉

            # 상승예측 결과 하나의 df로 합치기
            li_df = [pd.DataFrame()] + [dic_df_상승예측[종목코드] for 종목코드 in dic_df_상승예측.keys()]
            df_상승예측 = pd.concat(li_df, axis=0)

            # 결과 저장
            df_상승예측.to_pickle(os.path.join(self.folder_상승예측, f'df_상승예측_{s_모델}_{s_일자}.pkl'))
            df_상승예측.to_csv(os.path.join(self.folder_상승예측, f'상승예측_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목별 상승예측 완료({s_일자}, {len(dic_df_상승예측):,}개 종목, {s_모델})')

    def 백테스팅_수익검증(self, s_모델, n_최소성공이력, n_최소확률스펙):
        """ 상승여부 예측한 결과를 바탕으로 종목선정 조건에 따른 결과 확인 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익검증)
                    if f'df_수익검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:

            pass

            # 결과 저장
            df_상승예측.to_pickle(os.path.join(self.folder_상승예측, f'df_수익검증_{s_모델}_{s_일자}.pkl'))
            df_상승예측.to_csv(os.path.join(self.folder_상승예측, f'수익검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'기준별 수익검증 완료({s_일자}, {n:,}개 예측 {n:,}개 성공, {s_모델})')

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

    a.백테스팅_상승예측(s_모델='rf')
    # a.백테스팅_수익검증(s_모델='rf', n_최소성공이력=1, n_최소확률스펙=50)

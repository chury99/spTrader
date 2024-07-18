import os
import sys
import pandas as pd
import json
import re

from scipy import stats
from tqdm import tqdm
import pandas.errors

import analyzer_rf알고리즘 as Logic


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
        self.folder_일봉분석 = dic_폴더정보['sr종목선정|10_일봉분석']
        self.folder_지지저항 = dic_폴더정보['sr종목선정|20_지지저항']
        os.makedirs(self.folder_일봉분석, exist_ok=True)
        os.makedirs(self.folder_지지저항, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])

        # log 기록
        self.make_log(f'### 종목 선정 시작 ###')

    def 분석_일봉분석(self):
        """ 분석대상종목 기준으로 일봉 분석해서 거래량 변동 발생 종목 선정 후 pkl, csv 저장 \n
                    # 선정기준 : 최근 20일 z-score +3 초과 & 거래대금 100억 초과 되는 종목 """
        # 파일명 정의
        s_파일명_생성 = 'df_거래량변동종목'

        # 분석대상 일자 선정
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(days=self.n_보관기간_analyzer)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                           if 'dic_코드별_분봉_' in 파일명 and '.pkl' in 파일명])
        li_일자_전체 = [일자 for 일자 in li_일자_전체 if 일자 > s_기준일자]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_일봉분석)
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
            li_df_거래량변동종목 = list()
            for s_종목코드 in tqdm(dic_일봉_금월.keys(), desc=f'일봉 거래량 분석|{s_일자}'):
                # 분석대상 종목 확인 (키움 조건식 연계)
                if s_종목코드 not in self.li_종목_분석대상:
                    continue

                # 일봉 불러오기
                li_df_일봉 = [dic_일봉_금월[s_종목코드], dic_일봉_전월[s_종목코드]
                            if s_종목코드 in dic_일봉_전월.keys() else dic_일봉_금월[s_종목코드]]
                df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자').reset_index(drop=True)
                df_일봉 = df_일봉[df_일봉['일자'] <= s_일자]

                # 20일 데이터만 골라내기
                if len(df_일봉) >= 20:
                    df_일봉 = df_일봉[-20:]
                else:
                    continue

                # 변동 확인 (z-score +3 초과, 거래대금 100억 초과)
                df_일봉['z값_거래량'] = stats.zscore(df_일봉['거래량'])
                if df_일봉['z값_거래량'].values[-1] > 3 and df_일봉['거래대금(백만)'].values[-1] > 10000:
                    li_df_거래량변동종목.append(df_일봉[-1:])

            # 거래량 변동 종목 df 생성
            df_거래량변동종목 = pd.concat(li_df_거래량변동종목, axis=0)

            # df 저장
            df_거래량변동종목.to_pickle(os.path.join(self.folder_일봉분석, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_거래량변동종목.to_csv(os.path.join(self.folder_일봉분석, f'{s_파일명_생성}_{s_일자}.csv'),
                              index=False, encoding='cp949')

            # log 기록
            self.make_log(f'일봉 거래량 분석 완료({s_일자}, {len(df_거래량변동종목):,}종목)')

    def 분석_지지저항(self):
        """ 일봉분석 결과 선정된 종목 대상으로 3분봉 SRline 산출 후  pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_거래량변동종목'
        s_파일명_생성 = 'df_지지저항'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_일봉분석)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_지지저항)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:




            # df 저장
            df_지지저항.to_pickle(os.path.join(self.folder_지지저항, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_지지저항.to_csv(os.path.join(self.folder_지지저항, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'지지/저항 분석 완료({s_일자}, {len(df_지지저항):,}종목)')

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

    a.분석_일봉분석()
    a.분석_지지저항()

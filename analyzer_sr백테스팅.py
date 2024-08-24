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
        self.folder_지지저항 = dic_폴더정보['sr종목선정|20_지지저항']
        self.folder_종목선정 = dic_폴더정보['sr종목선정|50_종목선정']
        self.folder_매수매도 = dic_폴더정보['sr백테스팅|10_매수매도']
        os.makedirs(self.folder_매수매도, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수

        # log 기록
        self.make_log(f'### 종목 선정 시작 ###')

    def 검증_매수매도(self):
        """ 전일 종목선정 데이터 기준으로 당일 매수매도 분석하여 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_종목선정'
        s_파일명_생성 = 'df_매수매도'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목선정)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 날짜 확인
            li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                       if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]
            s_일자_당일 = s_일자
            s_일자_전일 = max(일자 for 일자 in li_전체일자 if 일자 < s_일자_당일)

            # 종목선정, 지지저항 불러오기 (전일)
            try:
                df_종목선정 = pd.read_pickle(os.path.join(self.folder_종목선정, f'{s_파일명_기준}_{s_일자_전일}.pkl'))
                df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자_전일}.pkl'))
                li_대상종목 = list(df_종목선정['종목코드'].sort_values().unique())
            except FileNotFoundError:
                continue

            # 3분봉, 1분봉 불러오기 (당일)
            dic_1분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자_당일}.pkl'))
            dic_3분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_전일}.pkl'))
            dic_3분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_당일}.pkl'))
            li_시간_1분봉 = list(max(dic_1분봉.values(), key=len)['시간'].unique())
            li_시간_3분봉 = list(max(dic_3분봉_당일.values(), key=len)['시간'].unique())

            # 매수매도 검증
            li_df_매수매도 = list()
            for s_종목코드 in tqdm(li_대상종목, desc=f'매수매도 검증|{s_일자}'):
                # 데이터 준비
                try:
                    df_3분봉_당일 = dic_3분봉_당일[s_종목코드]
                except KeyError:
                    continue
                df_3분봉 = pd.concat([dic_3분봉_전일[s_종목코드], df_3분봉_당일], axis=0).sort_values(['일자', '시간'])
                df_3분봉['일자시간'] = pd.to_datetime(df_3분봉['일자'] + ' ' + df_3분봉['시간'], format='%Y%m%d %H:%M:%S')
                df_3분봉 = df_3분봉.set_index('일자시간').sort_index()

                df_1분봉 = dic_1분봉[s_종목코드]

                li_지지저항 = list(df_지지저항[df_지지저항['종목코드'] == s_종목코드]['고가'].values)

                # 매수신호 탐색
                li_df_매수신호_종목 = list()
                for s_시간 in df_3분봉_당일['시간']:
                    # 매수신호 생성
                    dt_일자시간 = pd.Timestamp(f'{s_일자} {s_시간}')
                    df_3분봉_시점 = df_3분봉[df_3분봉.index <= dt_일자시간]
                    li_매수신호 = Logic.find_매수신호(df_ohlcv=df_3분봉_시점, li_지지저항=li_지지저항, dt_일자시간=dt_일자시간)

                    # 매수신호 확인
                    if sum(li_매수신호) == len(li_매수신호):
                        df_매수신호_시점 = df_3분봉_시점.loc[:, ['일자', '종목코드', '종목명']].copy()[-1:]
                        df_매수신호_시점['매수시간'] = s_시간
                        df_매수신호_시점['매수단가'] = int(df_3분봉_시점['시가'].values[-1])
                        li_df_매수신호_종목.append(df_매수신호_시점)

                df_매수신호_종목 = pd.concat(li_df_매수신호_종목, axis=0) if len(li_df_매수신호_종목) > 0 else pd.DataFrame()

                # 매도신호 탐색
                li_df_매수매도_종목 = list()
                li_매수시간 = df_매수신호_종목['매수시간'] if len(df_매수신호_종목) > 0 else list()
                s_매도시간 = '00:00:00'
                for s_매수시간 in li_매수시간:
                    # 매도 전 매수 금지
                    if s_매수시간 <= s_매도시간:
                        continue

                    # 기준정보 정의
                    n_매수단가 = df_매수신호_종목[df_매수신호_종목['매수시간'] == s_매수시간]['매수단가'].values[0]
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

                        # 매도신호 생성
                        li_매도신호_고가, n_매도단가_고가 = Logic.find_매도신호(n_현재가=n_고가, dic_지지저항=dic_지지저항)
                        li_매도신호_저가, n_매도단가_저가 = Logic.find_매도신호(n_현재가=n_저가, dic_지지저항=dic_지지저항)
                        li_매도신호 = [(li_매도신호_고가[i] or li_매도신호_저가[i]) for i in range(len(li_매도신호_고가))]
                        n_매도단가 = n_매도단가_고가 or n_매도단가_저가

                        # 매도신호 확인
                        if sum(li_매도신호) > 0:
                            df_매수매도_시점 = df_매수신호_종목[df_매수신호_종목['매수시간'] == s_매수시간].copy()
                            df_매수매도_시점['매도시간'] = s_시간
                            df_매수매도_시점['매도단가'] = int(n_매도단가)
                            df_매수매도_시점['수익률(%)'] = (df_매수매도_시점['매도단가'] / df_매수매도_시점['매수단가'] - 1) * 100
                            li_df_매수매도_종목.append(df_매수매도_시점)
                            break

                df_매수매도_종목 = pd.concat(li_df_매수매도_종목, axis=0) if len(li_df_매수매도_종목) > 0 else pd.DataFrame()
                li_df_매수매도.append(df_매수매도_종목)

            # df_매수매도 생성
            df_매수매도 = pd.concat(li_df_매수매도, axis=0) if len(li_df_매수매도) > 0 else pd.DataFrame()
            if len(df_매수매도) == 0:
                s_파일명 = max(파일 for 파일 in os.listdir(self.folder_매수매도) if s_파일명_생성 in 파일 and '.pkl' in 파일)
                df_매수매도 = pd.read_pickle(os.path.join(self.folder_매수매도, s_파일명))[:0]

            # 매도 전 매수 케이스 제거
            df_매수매도 = df_매수매도.reset_index(drop=True)
            df_매수매도['검증'] = True
            while True:
                df_매수매도 = df_매수매도[df_매수매도['검증']]
                df_매수매도['검증'] = (df_매수매도['매수시간'] > df_매수매도['매도시간'].shift(1)) | (df_매수매도.index == 0)
                if sum(df_매수매도['검증']) == len(df_매수매도):
                    break

            # 누적 수익률 산출
            df_매수매도 = df_매수매도.loc[:, '일자': '수익률(%)']
            df_매수매도 = df_매수매도.sort_values('매수시간')
            df_매수매도['누적수익(%)'] = (1 + df_매수매도['수익률(%)'] / 100).cumprod() * 100

            # df 저장
            df_매수매도.to_pickle(os.path.join(self.folder_매수매도, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매수매도.to_csv(os.path.join(self.folder_매수매도, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매수매도 검증 완료({s_일자}, {len(df_매수매도):,}건 매매)')

    def 검증_결과정리(self):
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
    a = Analyzer(n_분석일수=None)

    a.검증_매수매도()
    a.검증_결과정리()

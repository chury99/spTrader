import os
import sys
from PyQt5.QtWidgets import *
import pandas as pd
import json
import time
import sqlite3


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Collector:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'sp_collector_{self.s_오늘}.log')
        self.n_딜레이 = 0.2

        # 폴더 정의
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_ohlcv = os.path.join(folder_데이터, 'ohlcv')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        os.makedirs(self.folder_ohlcv, exist_ok=True)
        os.makedirs(self.folder_캐시변환, exist_ok=True)
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')

        # 키움 api 연결
        import API_kiwoom
        self.api = API_kiwoom.KiwoomAPI()
        self.api.comm_connect()

        # log 기록
        self.make_log(f'### ohlcv 데이터 수집 시작 ({self.api.s_접속서버}) ###')

    def 진행확인(self):
        """ 전체 진행할 항목 중 얼만큼 진행되었는지 확인 후 잔여 항목 선정 (전체종목 pkl, 일봉/분봉 ohlcv 임시저장 pkl 활용) """
        # 전체 항목 확인 (df_전체종목.pkl 확인)
        df_전체종목 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_전체종목.pkl'))
        li_종목코드_전체 = list(df_전체종목['종목코드'].values)

        # 제외 항목 확인 (데이터 길이 0 인 종목코드)
        try:
            li_종목코드_제외_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'li_종목코드_제외_일봉.pkl'))
        except FileNotFoundError:
            li_종목코드_제외_일봉 = list()

        try:
            li_종목코드_제외_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'li_종목코드_제외_분봉.pkl'))
        except FileNotFoundError:
            li_종목코드_제외_분봉 = list()

        # 완료 항목 확인 (df_ohlcv_일봉_임시.pkl, df_ohlcv_분봉_임시.pkl 확인)
        try:
            df_ohlcv_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_ohlcv_일봉_임시.pkl'))
        except FileNotFoundError:
            df_ohlcv_일봉 = None

        try:
            df_ohlcv_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_ohlcv_분봉_임시.pkl'))
        except FileNotFoundError:
            df_ohlcv_분봉 = None

        li_종목코드_완료_일봉 = list(df_ohlcv_일봉['종목코드'].unique()) if df_ohlcv_일봉 is not None else list()
        li_종목코드_완료_분봉 = list(df_ohlcv_분봉['종목코드'].unique()) if df_ohlcv_분봉 is not None else list()

        # 잔여 항목 확인
        li_종목코드_잔여_일봉 = [s_종목코드 for s_종목코드 in li_종목코드_전체 if s_종목코드 not in li_종목코드_제외_일봉]
        li_종목코드_잔여_일봉 = [s_종목코드 for s_종목코드 in li_종목코드_잔여_일봉 if s_종목코드 not in li_종목코드_완료_일봉]

        li_종목코드_잔여_분봉 = [s_종목코드 for s_종목코드 in li_종목코드_전체 if s_종목코드 not in li_종목코드_제외_분봉]
        li_종목코드_잔여_분봉 = [s_종목코드 for s_종목코드 in li_종목코드_잔여_분봉 if s_종목코드 not in li_종목코드_완료_분봉]

        # self 변수 정의
        self.dic_종목코드2종목명 = df_전체종목.set_index('종목코드').to_dict()['종목명']
        self.n_전체항목_일봉 = len(li_종목코드_전체)
        self.n_전체항목_분봉 = len(li_종목코드_전체)
        self.n_완료항목_일봉 = len(li_종목코드_완료_일봉) + len(li_종목코드_제외_일봉)
        self.n_완료항목_분봉 = len(li_종목코드_완료_분봉) + len(li_종목코드_제외_분봉)
        self.li_종목코드_제외_일봉 = li_종목코드_제외_일봉
        self.li_종목코드_제외_분봉 = li_종목코드_제외_분봉
        self.li_종목코드_잔여_일봉 = li_종목코드_잔여_일봉
        self.li_종목코드_잔여_분봉 = li_종목코드_잔여_분봉
        self.li_종목코드_완료_일봉 = li_종목코드_완료_일봉
        self.li_종목코드_완료_분봉 = li_종목코드_완료_분봉

        # log 기록
        self.make_log(f'진행현황 확인 (일봉 {self.n_완료항목_일봉:,}/{self.n_전체항목_일봉:,} , '
                      f'분봉 {self.n_완료항목_분봉:,}/{self.n_전체항목_분봉:,})')

    def 일자확인(self):
        """ db 파일 조회하여 수집해야 할 일봉/분봉 일자 선정 """
        # 파일명 확인
        li_파일명_일봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_일봉_' in 파일명 and '.db' in 파일명]
        li_파일명_분봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_분봉_' in 파일명 and '.db' in 파일명]
        s_최종파일_일봉 = max(li_파일명_일봉)
        s_최종파일_분봉 = max(li_파일명_분봉)

        # 일봉 일자 확인
        con_일봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_최종파일_일봉))
        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con_일봉)
        s_테이블명_최종 = df_테이블명['name'].values.max()
        df_일봉 = pd.read_sql(f'SELECT * FROM {s_테이블명_최종}', con=con_일봉)
        s_최종일자_일봉 = df_일봉['일자'].max()

        # 분봉 일자 확인
        con_분봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_최종파일_분봉))
        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con_분봉)
        s_테이블명_최종 = df_테이블명['name'].values.max()
        s_최종일자_분봉 = s_테이블명_최종.split('_')[2]

        # self 변수 정의
        self.s_최종일자_일봉 = s_최종일자_일봉
        self.s_최종일자_분봉 = s_최종일자_분봉

        # log 기록
        self.make_log(f'DB에 저장된 마지막 데이터 확인 (일봉 {self.s_최종일자_일봉}, 분봉 {self.s_최종일자_분봉})')

    def 수집_일봉(self):
        """ 종목별 일봉 데이터 받아서 pkl 형식으로 임시 저장 """
        # 임시 pkl 불러오기
        s_파일명_임시pkl = 'df_ohlcv_일봉_임시.pkl'
        try:
            df_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, s_파일명_임시pkl))
        except FileNotFoundError:
            df_일봉 = pd.DataFrame()

        for n_순번, s_종목코드 in enumerate(self.li_종목코드_잔여_일봉):
            # 일봉 조회
            df_일봉_추가 = self.api.get_tr_일봉조회(s_종목코드=s_종목코드, s_기준일자_부터=self.s_최종일자_일봉)
            time.sleep(self.n_딜레이)

            # 해당 일자 골라내기
            df_일봉_추가 = df_일봉_추가[df_일봉_추가['일자'] > self.s_최종일자_일봉]

            # 데이터 없는 종목코드 별도 저장
            if len(df_일봉_추가) == 0:
                self.li_종목코드_제외_일봉.append(s_종목코드)
                pd.to_pickle(self.li_종목코드_제외_일봉, os.path.join(self.folder_정보수집, 'li_종목코드_제외_일봉.pkl'))

            # df 합쳐서 저장
            df_일봉 = pd.concat([df_일봉, df_일봉_추가], axis=0)
            df_일봉.to_pickle(os.path.join(self.folder_정보수집, s_파일명_임시pkl))

            # log 기록
            n_전체 = self.n_전체항목_일봉
            n_완료 = self.n_완료항목_일봉 + n_순번 + 1
            n_진행률 = n_완료 / n_전체 * 100
            s_종목명 = self.dic_종목코드2종목명[s_종목코드]
            self.make_log(f'{n_진행률:0.2f}% 수집 완료 ({n_완료}/{n_전체}) -- {s_종목명}')

        # log 기록
        n_전체 = self.n_전체항목_일봉
        n_제외 = len(self.li_종목코드_제외_일봉)
        n_수집 = len(df_일봉['종목코드'].unique())
        self.make_log(f'### 일봉 수집 완료 - 전체 {n_전체:,}종목, 제외 {n_제외:,}종목, 수집 {n_수집:,}종목 ###')

    def 수집_분봉(self):
        """ 종목별 분봉 데이터 받아서 pkl 형식으로 임시 저장 """
        # 임시 pkl 불러오기
        s_파일명_임시pkl = 'df_ohlcv_분봉_임시.pkl'
        try:
            df_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, s_파일명_임시pkl))
        except FileNotFoundError:
            df_분봉 = pd.DataFrame()

        for n_순번, s_종목코드 in enumerate(self.li_종목코드_잔여_분봉):
            # 분봉 조회
            df_분봉_추가 = self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=1, s_기준일자_부터=self.s_최종일자_분봉)
            time.sleep(self.n_딜레이)

            # 해당 일자 골라내기
            df_분봉_추가 = df_분봉_추가[df_분봉_추가['일자'] > self.s_최종일자_분봉]

            # 데이터 없는 종목코드 별도 저장
            if len(df_분봉_추가) == 0:
                self.li_종목코드_제외_분봉.append(s_종목코드)
                pd.to_pickle(self.li_종목코드_제외_분봉, os.path.join(self.folder_정보수집, 'li_종목코드_제외_분봉.pkl'))

            # df 합쳐서 저장
            df_분봉 = pd.concat([df_분봉, df_분봉_추가], axis=0)
            df_분봉.to_pickle(os.path.join(self.folder_정보수집, s_파일명_임시pkl))

            # log 기록
            n_전체 = self.n_전체항목_분봉
            n_완료 = self.n_완료항목_분봉 + n_순번 + 1
            n_진행률 = n_완료 / n_전체 * 100
            s_종목명 = self.dic_종목코드2종목명[s_종목코드]
            self.make_log(f'{n_진행률:0.2f}% 수집 완료 ({n_완료}/{n_전체}) -- {s_종목명}')

        # log 기록
        n_전체 = self.n_전체항목_분봉
        n_제외 = len(self.li_종목코드_제외_분봉)
        n_수집 = len(df_분봉['종목코드'].unique())
        self.make_log(f'### 분봉 수집 완료 - 전체 {n_전체:,}종목, 제외 {n_제외:,}종목, 수집 {n_수집:,}종목 ###')

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
    app = QApplication(sys.argv)
    c = Collector()

    c.진행확인()
    c.일자확인()
    # c.수집_일봉()
    c.수집_분봉()

import os
import sys
from PyQt5.QtWidgets import *
import pandas as pd
import json


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Collector:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_collector"]}_{self.s_오늘}.log')
        self.n_딜레이 = 0.2

        # 폴더 정의
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')
        os.makedirs(self.folder_정보수집, exist_ok=True)

        # 키움 api 연결
        import API_kiwoom
        self.api = API_kiwoom.KiwoomAPI()
        self.api.comm_connect()

        # log 기록
        self.make_log(f'### 데이터 다운로드 시작 ({self.api.s_접속서버}) ###')

    def get_전체종목(self):
        """ 전체 종목코드 받아서 pkl, csv 저장 """
        # 전체종목코드 받아오기
        li_코스피 = self.api.get_전체종목코드(s_주식시장='코스피')
        li_코스닥 = self.api.get_전체종목코드(s_주식시장='코스닥')

        # 정보 추가
        li_전체종목 = list()
        for s_종목코드 in li_코스피:
            s_종목명 = self.api.get_코드별종목명(s_종목코드)
            s_시장구분 = '코스피'
            n_상장주식수 = self.api.get_코드별상장주식수(s_종목코드)
            s_상장일 = self.api.get_코드별상장일(s_종목코드)
            s_투자유의종목 = self.api.get_투자유의종목(s_종목코드)

            li_전체종목.append([s_종목코드, s_종목명, s_시장구분, n_상장주식수, s_상장일, s_투자유의종목])

        for s_종목코드 in li_코스닥:
            s_종목명 = self.api.get_코드별종목명(s_종목코드)
            s_시장구분 = '코스닥'
            n_상장주식수 = self.api.get_코드별상장주식수(s_종목코드)
            s_상장일 = self.api.get_코드별상장일(s_종목코드)
            s_투자유의종목 = self.api.get_투자유의종목(s_종목코드)

            li_전체종목.append([s_종목코드, s_종목명, s_시장구분, n_상장주식수, s_상장일, s_투자유의종목])

        # df 변환
        li_컬럼명 = ['종목코드', '종목명', '시장구분', '상장주식수', '상장일', '투자유의종목']
        df_전체종목 = pd.DataFrame(li_전체종목, columns=li_컬럼명)

        # 파일 저장
        df_전체종목.to_pickle(os.path.join(self.folder_정보수집, 'df_전체종목.pkl'))
        df_전체종목.to_csv(os.path.join(self.folder_정보수집, '전체종목.csv'), index=False, encoding='cp949')

        # log 기록
        self.make_log(f'전체종목 저장 완료 - 총 {len(df_전체종목):,}종목 (코스피 {len(li_코스피):,}, 코스닥 {len(li_코스닥):,})')

    def get_조건검색(self):
        """ 조건검색 항목별 종목코드 받아서 pkl, csv 저장 """
        # 전체 조건검색 받아오기
        dic_조건검색 = self.api.get_조건검색_전체()

        # 전체 종목 받아오기
        df_전체종목 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_전체종목.pkl'))

        # 항목별 데이터 처리
        dic_조건검색_df = dict()
        for s_항목명 in dic_조건검색.keys():
            li_종목코드 = dic_조건검색[s_항목명]

            # 정보 추가
            li_df_정보 = [df_전체종목[df_전체종목['종목코드'] == s_종목코드] for s_종목코드 in li_종목코드]
            try:
                df_정보 = pd.concat(li_df_정보, axis=0).drop_duplicates().reset_index(drop=True)
            except ValueError:
                df_정보 = pd.DataFrame([], columns=df_전체종목.columns)

            # dic에 저장
            dic_조건검색_df[s_항목명] = df_정보

            # csv 파일 저장
            df_정보.to_csv(os.path.join(self.folder_정보수집, f'조건검색_{s_항목명}.csv'), index=False, encoding='cp949')

            # log 기록
            self.make_log(f'조건검색 저장 - [{s_항목명}] {len(df_정보):,}개 종목')

        # pkl 파일 저장
        pd.to_pickle(dic_조건검색_df, os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))

        # log 기록
        self.make_log(f'전체 조건검색 종목 저장 완료 - 총 {len(dic_조건검색_df)}개 항목')

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

    # c.get_전체종목()
    c.get_조건검색()

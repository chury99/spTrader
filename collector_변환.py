import os
import sys
import pandas as pd
import json
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
        self.path_log = os.path.join(dic_config['folder_log'], f'sp_ohlcv_{self.s_오늘}.log')

        # 폴더 정의
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_ohlcv = os.path.join(folder_데이터, 'ohlcv')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')

        # log 기록
        self.make_log(f'### 데이터 변환 시작 ###')

    def db저장_일봉(self):
        """ pkl 형식으로 임시 저장된 일봉 파일 읽어와서 db 파일 저장 """
        # pkl 읽어오기
        df_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_ohlcv_일봉_임시.pkl'))

        # 일별로 분리
        gr_일봉 = df_일봉.groupby('일자')
        for s_일자, df_일봉_신규 in gr_일봉:
            # db 파일 불러오기
            s_년도 = s_일자[:4]
            s_년월 = s_일자[:6]
            s_파일명 = f'ohlcv_일봉_{s_년도}.db'
            s_테이블명 = f'ohlcv_일봉_{s_년월}'
            con_일봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))
            df_일봉_기존 = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con_일봉)

            # df 합쳐서 저장
            df_일봉 = pd.concat([df_일봉_기존, df_일봉_신규], axis=0)
            df_일봉 = df_일봉.drop_duplicates().sort_values(['일자', '종목코드'], ascending=True)
            df_일봉.to_sql(s_테이블명, con=con_일봉, index=False, if_exists='replace')

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def db저장_분봉(self):
        """ pkl 형식으로 임시 저장된 분봉 파일 읽어와서 db 파일 저장 """
        # pkl 읽어오기
        df_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_ohlcv_분봉_임시.pkl'))

        # 일별로 분리
        gr_분봉 = df_분봉.groupby('일자')
        for s_일자, df_분봉 in gr_분봉:
            # db 파일 연결
            s_년도 = s_일자[:4]
            s_월 = s_일자[4:6]
            s_파일명 = f'ohlcv_분봉_{s_년도}_{s_월}.db'
            s_테이블명 = f'ohlcv_분봉_{s_일자}'
            con_분봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))

            # df 저장
            df_분봉 = df_분봉.drop_duplicates().sort_values(['종목코드', '시간'], ascending=[True, False])
            df_분봉.to_sql(s_테이블명, con=con_분봉, index=False, if_exists='replace')

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 캐시저장_일봉(self):
        """ db 파일 불러와서 종목별 분류 후 pkl 파일 저장 (일봉) """
        # 대상 구간 확인

        # db 파일 불러오기

        # 종목별 분류

        # pkl 저장

        pass

    def 캐시저장_분봉(self):
        """ db 파일 불러와서 종목별 분류 후 pkl 파일 저장 (분봉) """
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
    c = Collector()

    # c.db저장_일봉()
    c.db저장_분봉()
    c.캐시저장_일봉()
    c.캐시저장_분봉()
    pass

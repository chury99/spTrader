import os
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import pandas as pd
import json
import time


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
        self.n_딜레이 = 0.2

        # 폴더 정의
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_ohlcv = os.path.join(folder_데이터, 'ohlcv')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        os.makedirs(self.folder_ohlcv, exist_ok=True)
        os.makedirs(self.folder_캐시변환, exist_ok=True)

        # 키움 api 연결
        import API_kiwoom
        self.api = API_kiwoom.KiwoomAPI()

        # log 기록
        self.make_log(f'### Collector 구동 시작 ###')

    def 접속확인(self):


        pass

    ###################################################################################################################
    def make_log(self, s_text, li_loc=None):
        """ 입력 받은 s_text 에 시간 붙여서 self.path_log 에 저장 """
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

    c.api.comm_connect()
    c.접속확인()
    pass

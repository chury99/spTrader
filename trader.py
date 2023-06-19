import os
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
from PyQt5 import uic
import pandas as pd
import json
import time

# UI 파일 연결
form_class = uic.loadUiType(os.path.join(os.getcwd(), 'trader_ui.ui'))[0]


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Trader(QMainWindow, form_class):
    def __init__(self):
        # 이전 클래스 상속
        super().__init__()

        # UI 읽어오기
        self.setupUi(self)

        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 폴더 정의
        folder_work = dic_config['folder_work']
        self.folder_run = os.path.join(folder_work, 'run')
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        s_로그이름 = dic_config['로그이름_trader']
        self.path_log = os.path.join(dic_config['folder_log'], f'{s_로그이름}_{self.s_오늘}.log')
        self.path_log_주문 = os.path.join(dic_config['folder_log'], f'{s_로그이름}_주문_{self.s_오늘}.log')
        self.path_모니터링 = os.path.join(self.folder_run, '모니터링_trader.pkl')
        self.n_딜레이 = 0.2

        # 키움 api 연결
        import API_kiwoom
        self.api = API_kiwoom.KiwoomAPI()
        self.api.comm_connect()

        # log 기록
        self.make_log(f'### Short Punch Trader 시작 ({self.api.s_접속서버}) ###')

        # 초기 설정
        self.계좌번호 = self.set_ui설정()
        self.flag_종목보유 = self.set_flag설정()
        self.set_대상종목설정()

        # 타이머 기반 동작 설정 (메인봇 연결)
        self.타이머 = QTimer(self)
        self.타이머.start(1 * 1000)    # 1/1000초 단위로 숫자 입력
        self.타이머.timeout.connect(self.run_메인봇)







    ###################################################################################################################
    def set_ui설정(self):
        """ ui 내 정보 표시 및 버튼 동작 설정 """
        pass

    def run_메인봇(self):
        """ 설정된 주기에 따라 매수봇, 매도봇 구동 """
        pass

    def run_매수봇(self):
        """ 매수 조건 확인하여 조건 만족 시 매수 주문 실행 """
        pass

    def run_매도봇(self):
        """ 매도 조건 확인하여 조건 만족 시 매도 주문 실행 """
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
    app = QApplication(sys.argv)
    t = Trader()
    t.show()
    app.exec_()

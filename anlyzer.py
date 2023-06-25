import os
import sys
import pandas as pd
import json

import pandas.errors
from tqdm import tqdm


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
        self.folder_감시대상 = os.path.join(folder_분석, '감시대상')
        self.folder_모델_lstm = os.path.join(folder_분석, '모델_lstm')
        self.folder_모델_rf = os.path.join(folder_분석, '모델_rf')
        os.makedirs(self.folder_감시대상, exist_ok=True)
        os.makedirs(self.folder_모델_lstm, exist_ok=True)
        os.makedirs(self.folder_모델_rf, exist_ok=True)

        # log 기록
        self.make_log(f'### 종목 분석 시작 ###')

    def 분석_감시대상(self):
        """ 전체 종목 분석해서 감시대상 종목 선정 후 pkl, csv 저장 """

        pass

    def 분석_lstm(self):
        """ 감시대상 종목 기준으로 lstm 분석해서 종목별 모델 생성 """
        pass

    def 분석_rf(self):
        """ 감시대상 종목 기준으로 random forest 분석해서 종목별 모델 생성 """
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

    a.분석_감시대상()

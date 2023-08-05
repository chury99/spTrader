import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Rotator:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_rotator"]}_{self.s_오늘}.log')

        # 폴더 정의
        folder_work = dic_config['folder_work']
        self.dic_folder = dict()
        self.dic_folder['log'] = dic_config['folder_log']

        folder_분석 = os.path.join(folder_work, '분석')
        folder_백테스팅 = os.path.join(folder_work, '백테스팅')
        self.dic_folder['analyzer'] = {'변동성종목': os.path.join(folder_분석, '10_변동성종목'),
                                       '데이터셋': os.path.join(folder_분석, '20_데이터셋'),
                                       '모델': os.path.join(folder_분석, '30_모델'),
                                       '성능평가': os.path.join(folder_분석, '40_성능평가'),
                                       '감시대상': os.path.join(folder_분석, '감시대상'),
                                       '상승예측': os.path.join(folder_백테스팅, '10_상승예측'),
                                       '수익검증': os.path.join(folder_백테스팅, '20_수익검증')}

        folder_데이터 = os.path.join(folder_work, '데이터')
        self.dic_folder['collector'] = {'ohlcv': os.path.join(folder_데이터, 'ohlcv'),
                                        '정보수집': os.path.join(folder_데이터, '정보수집'),
                                        '캐시변환': os.path.join(folder_데이터, '캐시변환')}

        folder_run = os.path.join(folder_work, 'run')
        folder_이력 = os.path.join(folder_work, '이력')
        self.dic_folder['trader'] = {'run': folder_run,
                                     '메세지': os.path.join(folder_이력, '메세지'),
                                     '실시간': os.path.join(folder_이력, '실시간'),
                                     '체결잔고': os.path.join(folder_이력, '체결잔고')}

        # 변수 설정
        self.n_보관기간_log = int(dic_config['파일보관기간(개월)_log'])
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(개월)_analyzer'])
        self.n_보관기간_collector = int(dic_config['파일보관기간(개월)_collector'])
        self.n_보관기간_trader = int(dic_config['파일보관기간(개월)_trader'])

        # log 기록
        self.make_log(f'### 파일 보관기간 관리 시작 ###')

    def 파일관리_log(self):
        """ 로그파일 확인하여 보관기간 지난 파일 삭제 """
        # 기준 일자 정의
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(months=self.n_보관기간_log)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')
        s_기준일자 = '20210505'

        # 삭제대상 파일 찾기
        s_폴더 = self.dic_folder['log']
        li_파일_전체 = os.listdir(s_폴더)
        li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
        li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]

        # 대상 파일 삭제
        for s_파일 in li_파일_삭제대상:
            os.system(f'del {os.path.join(s_폴더, s_파일)}')

        # log 기록
        self.make_log(f' 파일 삭제 완료({self.n_보관기간_log}개월 경과, {s_기준일자} 기준, {len(li_파일_삭제대상)}개 파일)')

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
    r = Rotator()

    r.파일관리_log()
    r.파일관리_analyzer()
    r.파일관리_collector()
    r.파일관리_trader()

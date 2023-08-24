import os
import sys
import pandas as pd
import json
import subprocess
import time


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class LauncherAnalyzer:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_analyzer"]}_{self.s_오늘}.log')
        self.path_파이썬32 = dic_config['path_파이썬32']
        self.path_파이썬64 = dic_config['path_파이썬64']

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        import API_kakao
        self.k = API_kakao.KakaoAPI()
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 구동 시작 ###')

    def analyzer_분석(self):
        """ 10분봉 데이터를 분석하여 모델 생성 및 감시대상 선정 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'analyzer_분석.py')

        # log 기록
        self.make_log(f'10분봉 데이터 분석 실행')

        # 프로세스 실행
        ret = subprocess.run([self.path_파이썬64, path_실행], shell=True)
        s_실행결과 = '성공' if ret.returncode == 0 else '실패'

        # 실패 시 카카오 메세지 송부
        if s_실행결과 == '실패':
            s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 중 오류 발생 - {sys._getframe(0).f_code.co_name} !!!'
            self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)

        # log 기록
        self.make_log(f'10분봉 데이터 분석 완료 - {s_실행결과}')

    def analyzer_백테스팅(self):
        """ 분석 결과 생성된 모델을 신규 데이터에 적용하여 수익 확인 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'analyzer_백테스팅.py')

        # log 기록
        self.make_log(f'분석 모델 백테스팅 실행')

        # 프로세스 실행
        ret = subprocess.run([self.path_파이썬64, path_실행], shell=True)
        s_실행결과 = '성공' if ret.returncode == 0 else '실패'

        # 실패 시 카카오 메세지 송부
        if s_실행결과 == '실패':
            s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 중 오류 발생 - {sys._getframe(0).f_code.co_name} !!!'
            self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)

        # log 기록
        self.make_log(f'분석 모델 백테스팅 완료 - {s_실행결과}')

    def UT_파일rotator(self):
        """ 생성된 파일들의 시점 확인하여 보관기간 지난 파일 삭제 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'UT_파일rotator.py')

        # log 기록
        self.make_log(f'파일 Rotation 실행')

        # 프로세스 실행
        ret = subprocess.run([self.path_파이썬64, path_실행], shell=True)
        s_실행결과 = '성공' if ret.returncode == 0 else '실패'

        # 실패 시 카카오 메세지 송부
        if s_실행결과 == '실패':
            s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 중 오류 발생 - {sys._getframe(0).f_code.co_name} !!!'
            self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)

        # log 기록
        self.make_log(f'파일 Rotation 완료 - {s_실행결과}')

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
    l = LauncherAnalyzer()

    l.analyzer_분석()
    l.analyzer_백테스팅()
    l.UT_파일rotator()

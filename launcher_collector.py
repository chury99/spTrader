import os
import sys
import pandas as pd
import json
import subprocess
import time


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class LauncherCollector:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_collector"]}_{self.s_오늘}.log')
        self.path_파이썬32 = dic_config['path_파이썬32']
        self.path_파이썬64 = dic_config['path_파이썬64']

        # 폴더 정의
        import UT_폴더정보
        dic_폴더정보 = UT_폴더정보.dic_폴더정보
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']

        # 카카오 API 연결
        sys.path.append(dic_config['folder_kakao'])
        import API_kakao
        self.k = API_kakao.KakaoAPI()
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 구동 시작 ###')

    def collector_다운(self):
        """ 키움 서버에서 필요 정보 다운로드 후 저장 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'collector_다운.py')

        # log 기록
        self.make_log(f'서버 데이터 다운로드 실행')

        # 프로세스 실행
        ret = subprocess.run([self.path_파이썬32, path_실행], shell=True)
        s_실행결과 = '성공' if ret.returncode == 0 else '실패'

        # 실패 시 카카오 메세지 송부
        if s_실행결과 == '실패':
            s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 중 오류 발생 - {sys._getframe(0).f_code.co_name} !!!'
            self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)

        # log 기록
        self.make_log(f'데이터 다운로드 완료 - {s_실행결과}')

    def collector_수집(self):
        """ ohlcv 수집 시 임시 pkl 파일 감시하여 응답 없을 시 종료 후 재구동 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'collector_수집.py')
        path_모니터링_일봉 = os.path.join(self.folder_정보수집, 'df_ohlcv_일봉_임시.pkl')
        path_모니터링_분봉 = os.path.join(self.folder_정보수집, 'df_ohlcv_분봉_임시.pkl')
        path_제외종목_일봉 = os.path.join(self.folder_정보수집, 'li_종목코드_제외_일봉.pkl')
        path_제외종목_분봉 = os.path.join(self.folder_정보수집, 'li_종목코드_제외_분봉.pkl')

        # ohlcv 임시 파일 삭제 (데이터 혼입 방지)
        if os.path.exists(path_모니터링_일봉):
            os.system(f'del {path_모니터링_일봉}')
        if os.path.exists(path_모니터링_분봉):
            os.system(f'del {path_모니터링_분봉}')
        if os.path.exists(path_제외종목_일봉):
            os.system(f'del {path_제외종목_일봉}')
        if os.path.exists(path_제외종목_분봉):
            os.system(f'del {path_제외종목_분봉}')

        # 프로세스 실행
        프로세스 = subprocess.Popen([self.path_파이썬32, path_실행], shell=True)
        s_pid = 프로세스.pid
        time.sleep(30)

        # 모니터링 진행
        dt_시작시각 = pd.Timestamp('now')
        while True:
            # 현재시각 확인
            dt_현재시각 = pd.Timestamp('now')

            # 정상종료 되었으면 종료
            ret = 프로세스.poll()
            s_실행상태 = '정상종료' if ret == 0 else '실행중' if ret is None else '비정상종료'
            if s_실행상태 == '정상종료':
                # log 기록
                self.make_log('서버접속 정상 종료')
                break

            # 기준시간 경과 시 강제 종료 (카카오 메세지 송부)
            n_기준시간 = 2
            if dt_현재시각 > dt_시작시각 + pd.Timedelta(hours=n_기준시간):
                s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 후 {n_기준시간}시간 경과 - {sys._getframe(0).f_code.co_name} !!!'
                self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)
                # log 기록
                self.make_log(f'강제종료 - 모듈 실행 후 {n_기준시간}시간 경과')
                break

            # 모니터링 파일 확인
            try:
                n_수정시각_일봉 = os.path.getmtime(path_모니터링_일봉)
            except FileNotFoundError:
                n_수정시각_일봉 = 0

            try:
                n_수정시각_분봉 = os.path.getmtime(path_모니터링_분봉)
            except FileNotFoundError:
                n_수정시각_분봉 = 0

            n_수정시각 = max(n_수정시각_일봉, n_수정시각_분봉)
            dt_수정시각 = pd.Timestamp(time.ctime(n_수정시각))

            # 시간 지연 시 재구동
            n_지연시간_초 = 3
            if dt_현재시각 - dt_수정시각 > pd.Timedelta(seconds=n_지연시간_초):
                # log 기록
                self.make_log(f'서버응답 지연({n_지연시간_초}초) - 강제종료 요청')

                # 시간 지연 시 종료 요청
                ret = subprocess.run(f'taskkill /f /t /pid {s_pid}', shell=True)
                s_종료요청 = '성공' if ret.returncode == 0 else '실패'
                time.sleep(1)

                # 프로세스 재실행 (종료요청 성공 시)
                if s_종료요청 == '성공':
                    # log 기록
                    self.make_log(f'서버 재접속 요청')
                    프로세스 = subprocess.Popen([self.path_파이썬32, path_실행], shell=True)
                    s_pid = 프로세스.pid
                    time.sleep(30)
                else:
                    # log 기록
                    self.make_log(f'강제종료 실패')
            else:
                time.sleep(1)

    def collector_변환(self):
        """ 수집된 ohlcv 임시 파일을 정리하여 db파일, 캐시파일 생성 """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'collector_변환.py')

        # log 기록
        self.make_log(f'데이터 변환 실행')

        # 프로세스 실행
        ret = subprocess.run([self.path_파이썬64, path_실행], shell=True)
        s_실행결과 = '성공' if ret.returncode == 0 else '실패'

        # 실패 시 카카오 메세지 송부
        if s_실행결과 == '실패':
            s_메세지 = f'!!! [{self.s_파일}] 모듈 실행 중 오류 발생 - {sys._getframe(0).f_code.co_name} !!!'
            self.k.send_message(s_user='알림봇', s_friend='여봉이', s_text=s_메세지)

        # log 기록
        self.make_log(f'데이터 변환 완료 - {s_실행결과}')

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
    l = LauncherCollector()

    l.collector_다운()
    l.collector_수집()
    l.collector_변환()

import os
import sys
import pandas as pd
import json
import subprocess
import time


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class LauncherTrader:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_trader"]}_{self.s_오늘}.log')
        self.path_파이썬32 = dic_config['path_파이썬32']
        self.path_파이썬64 = dic_config['path_파이썬64']

        # 에러 출력 설정
        sys.stderr = open(os.path.join(dic_config['folder_log'],
                                       f'{dic_config["로그이름_trader"]}_에러_{self.s_오늘}.log'), 'a')

        # 폴더 정의
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_run = dic_폴더정보['run']

        # 재구동 대기시간 정의
        self.n_재구동대기시간_초 = int(dic_config['재구동_대기시간(초)'])

        # 구동시간 확인
        self.dt_시작시각 = pd.Timestamp(dic_config['시작시각'])
        self.dt_종료시각 = pd.Timestamp(dic_config['종료시각'])

        # log 기록
        self.make_log(f'### 구동 시작 ###')

    def trader(self):
        """ mnt 파일 감시하여 응답 없을 시 종료 후 재구동 (시작시각, 종료시각 사이에만 구동) """
        # 파일 지정
        path_실행 = os.path.join(os.getcwd(), 'trader.py')
        path_모니터링 = os.path.join(self.folder_run, '모니터링_trader.pkl')

        # 프로세스 실행
        트레이더 = subprocess.Popen([self.path_파이썬32, path_실행], shell=True, stderr=subprocess.PIPE)
        s_pid = 트레이더.pid
        time.sleep(30)

        # 모니터링 진행
        while True:
            # 현재시각 확인
            dt_현재시각 = pd.Timestamp('now')

            # 정상종료 되었으면 종료 (에러 발생 시 stderr 기록)
            ret = 트레이더.poll()
            s_실행상태 = '정상종료' if ret == 0 else '실행중' if ret is None else '비정상종료'

            if s_실행상태 == '정상종료':
                self.make_log('서버접속 정상 종료')
                break

            if s_실행상태 == '비정상종료':
                stdout_트레이더, stderr_트레이더 = 트레이더.communicate()
                stderr_트레이더 = stderr_트레이더.decode('cp949')
                if stderr_트레이더 == '':
                    self.make_log(f'trader 에러 발생 -> 에러코드 미수신)')
                else:
                    self.make_log(f'trader 에러 발생\n'
                                  f'{stderr_트레이더}')
                break

            # 종료시각 경과 시 종료
            if dt_현재시각 > self.dt_종료시각:
                self.make_log(f'실행시간 종료 (종료시각 {self.dt_종료시각.strftime("%H:%M:%S")}) - 서버접속 종료 요청')

                ret = subprocess.run(f'taskkill /f /t /pid {s_pid}', shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                s_요청결과 = '성공' if ret.returncode == 0 else '실패'

                if s_요청결과 == '성공':
                    self.make_log(f'요청 결과 - {s_요청결과}')
                    break
                else:
                    self.make_log(f'요청 결과 - {s_요청결과} - {ret.args}\n'
                                  f'\t stdout - {ret.stdout}\n'
                                  f'\t stderr - {ret.stderr}')

            # 시간 지연 시 재구동 (모니터링 파일 확인)
            try:
                n_수정시각 = os.path.getmtime(path_모니터링)
            except FileNotFoundError:
                n_수정시각 = 0
            dt_수정시각 = pd.Timestamp(time.ctime(n_수정시각))

            if dt_현재시각 - dt_수정시각 > pd.Timedelta(seconds=self.n_재구동대기시간_초):
                self.make_log(f'서버응답 지연({self.n_재구동대기시간_초}초) - 강제종료 요청')

                ret = subprocess.run(f'taskkill /f /t /pid {s_pid}', shell=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                s_요청결과 = '성공' if ret.returncode == 0 else '실패'

                if s_요청결과 == '성공':
                    self.make_log(f'요청 결과 - {s_요청결과}')
                else:
                    self.make_log(f'요청 결과 - {s_요청결과} - {ret.args}\n'
                                  f'\t stdout - {ret.stdout}\n'
                                  f'\t stderr - {ret.stderr}')

                # 프로세스 재실행
                time.sleep(1)
                self.make_log(f'서버 재접속 요청 (에러 메세지 없음)')
                트레이더 = subprocess.Popen([self.path_파이썬32, path_실행], shell=True, stderr=subprocess.PIPE)
                s_pid = 트레이더.pid
                time.sleep(30)

            else:
                time.sleep(1)

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
    l = LauncherTrader()

    l.trader()

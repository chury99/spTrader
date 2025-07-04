import os
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5 import uic
import pandas as pd
import json
import time
import re

import analyzer_sr알고리즘 as Logic_sr
import analyzer_tf알고리즘 as Logic
import UT_차트maker as Chart

# UI 파일 연결
form_class = uic.loadUiType(os.path.join(os.getcwd(), 'trader_ui.ui'))[0]


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames,PyUnboundLocalVariable
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
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_run = dic_폴더정보['run']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_체결잔고 = dic_폴더정보['이력|체결잔고']
        self.folder_신호탐색 = dic_폴더정보['이력|신호탐색']
        self.folder_주문정보 = dic_폴더정보['이력|주문정보']
        self.folder_대상종목 = dic_폴더정보['이력|대상종목']
        self.folder_초봉정보 = dic_폴더정보['이력|초봉정보']
        self.folder_매개변수 = dic_폴더정보['이력|매개변수']
        self.folder_일봉변동 = dic_폴더정보['sr종목선정|10_일봉변동']
        self.folder_수익요약 = dic_폴더정보['tf백테스팅|40_수익요약']
        os.makedirs(self.folder_신호탐색, exist_ok=True)
        os.makedirs(self.folder_주문정보, exist_ok=True)
        os.makedirs(self.folder_대상종목, exist_ok=True)
        os.makedirs(self.folder_초봉정보, exist_ok=True)
        os.makedirs(self.folder_매개변수, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        s_로그이름 = dic_config['로그이름_trader']
        self.path_log = os.path.join(dic_config['folder_log'], f'{s_로그이름}_{self.s_오늘}.log')
        self.path_log_신호 = os.path.join(dic_config['folder_log'], f'{s_로그이름}_신호_{self.s_오늘}.log')
        self.path_log_주문 = os.path.join(dic_config['folder_log'], f'{s_로그이름}_주문_{self.s_오늘}.log')
        self.path_모니터링 = os.path.join(self.folder_run, '모니터링_trader.pkl')
        self.path_매개변수 = os.path.join(self.folder_매개변수, f'dic_매개변수_{self.s_오늘}.pkl')
        self.dic_매개변수 = pd.read_pickle(self.path_매개변수) if os.path.exists(self.path_매개변수) else dict()
        self.n_딜레이 = 0.2

        # 키움 api 연결
        import API_kiwoom
        self.api = API_kiwoom.KiwoomAPI()
        self.api.comm_connect()

        # 정보 설정
        self.s_접속서버 = self.api.s_접속서버
        li_계좌번호 = self.api.get_로그인정보('계좌목록').split(';')
        self.s_계좌번호 = li_계좌번호[1] if self.s_접속서버 == '실서버' else li_계좌번호[0]
        self.s_시작시각 = dic_config['시작시각']
        self.s_종료시각 = dic_config['종료시각']
        self.s_자본금 = dic_config['자본금']
        self.n_모니터링파일생성주기 = int(int(dic_config['재구동_대기시간(초)']) / 2)
        self.s_전일 = self.get_전일날짜()
        self.li_호가단위 = self.make_호가단위()

        # log 기록
        self.make_log(f'### Short Punch Trader 시작 ({self.s_접속서버}) ###')

        # 대상 정보 설정
        self.n_초봉, self.li_매매대상 = self.get_대상선정(n_초봉=5, li_매매대상=['일봉변동'])
        # self.n_초봉, self.li_매매대상 = self.get_대상선정()
        self.dic_대상종목, self.df_대상종목_매매, self.dic_코드2종목명 = self.get_대상종목(li_매매대상=self.li_매매대상)

        # 초기 설정
        self.setui_초기설정()
        self.setui_예수금()
        self.setui_거래이력()

        self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
        self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

        # 타이머 기반 동작 설정 (메인봇 연결)
        self.타이머 = QTimer(self)
        self.타이머.start(1 * 1000)  # 1/1000초 단위로 숫자 입력
        self.타이머.timeout.connect(self.run_mainbot)

    ###################################################################################################################

    def run_메인봇(self):
        """ 1초 단위 실행 및 설정된 주기에 따라 매수봇, 매도봇 구동 """
        # ui 동작상태 업데이트
        self.lb_run_mainbot.setText('[ 메인봇 ] 동작중')

        # 현재 시각 확인
        dt_현재 = pd.Timestamp('now')
        s_현재 = dt_현재.strftime('%H:%M:%S')
        n_현재_분 = int(dt_현재.strftime('%M'))
        n_현재_초 = int(dt_현재.strftime('%S'))

        # 1초 단위 업데이트
        self.setui_실시간()

        # 대상종목 업데이트 (10분 주기) - 조건식 1분주기 이내 조회시 미응답
        li_초반탐색시간 = ['09:01:30', '09:03:00', '09:04:30', '09:06:00', '09:08:00']
        if (s_현재 in li_초반탐색시간) or (n_현재_분 % 10 == 0 and n_현재_초 == 0):
            self.dic_대상종목, self.df_대상종목_매매, self.dic_코드2종목명 = self.get_대상종목(li_매매대상=self.li_매매대상)

        # 매수봇 호출 (초봉 주기)
        if not self.flag_종목보유:
            if n_현재_초 % self.n_초봉 == 1 and s_현재 < '15:10:00':
                self.run_매수봇()

        # 매도봇 호출 (1초 주기)
        if self.flag_종목보유:
            if n_현재_초 % 1 == 0:
                self.run_매도봇()

        # 모니터링 파일 생성 (재구동 대기시간 절반 주기)
        if n_현재_초 % self.n_모니터링파일생성주기 == 0:
            self.update_모니터링파일()

        # 초기 설정 (3분 주기)
        if n_현재_분 % 3 == 0 and n_현재_초 == 30:
            self.setui_예수금()
            self.setui_거래이력()

            self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
            self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

        # 파일 변환
        if dt_현재 > pd.Timestamp('15:19:00') and n_현재_초 == 10:
            self.convert_이력파일()
            pd.to_pickle(self.dic_매개변수, self.path_매개변수)

    def run_매수봇(self):
        """ 매수 조건 확인하여 조건 만족 시 매수 주문 실행 """
        # icloud 내 로그 업데이트 위한 신호 생성
        b_로그 = pd.Timestamp('now').second != 1 and self.n_초봉 not in [1, 2, 3]

        # ui 상태 업데이트 및 log 기록
        self.lb_run_buybot.setText('[ 매수봇 ] 동작중')
        if b_로그:
            li_매매대상 = [대상[:2] for 대상 in self.li_매매대상]
            self.make_log(f'매수신호 탐색 - {self.n_초봉}초{li_매매대상}')
            self.make_log_신호(f'##### 매수신호 탐색 #####\n'
                             f'  ===== z값검증 | 금액검증 | 강도검증 ===== {self.n_초봉}초{li_매매대상}')

        # 대상 종목별 매수신호 탐색
        dic_코드2선정사유 = self.df_대상종목_매매.set_index('종목코드').to_dict()['선정사유']
        for s_종목코드 in self.df_대상종목_매매['종목코드']:
            # 기초정보 정의
            s_종목명 = self.dic_코드2종목명[s_종목코드]
            s_선정사유 = dic_코드2선정사유[s_종목코드]

            # 종목별 매개변수 불러오기
            dic_매개변수_종목 = self.dic_매개변수[s_종목코드] if s_종목코드 in self.dic_매개변수 else dict()
            dic_매개변수_종목['s_종목코드'] = s_종목코드
            dic_매개변수_종목['s_종목명'] = s_종목명
            dic_매개변수_종목['s_일자'] = self.s_오늘
            dic_매개변수_종목['n_초봉'] = self.n_초봉
            dic_매개변수_종목['s_선정사유'] = s_선정사유

            # 초봉 데이터 생성
            n_봉수 = 30
            n_기준시간 = self.n_초봉 * (n_봉수 + 3)
            s_기준시간 = (pd.Timestamp('now') - pd.Timedelta(seconds=n_기준시간)).strftime('%H:%M:%S')
            li_체결정보 = [li_체결 for li_체결 in self.api.dic_실시간_체결[s_종목코드] if li_체결[1] > s_기준시간] \
                            if s_종목코드 in self.api.dic_실시간_체결.keys() else list()
            df_초봉 = Logic.make_초봉데이터_trader(li_체결정보=li_체결정보,
                                            s_오늘=self.s_오늘, n_초봉=self.n_초봉, s_종목코드=s_종목코드)
            n_현재가 = df_초봉['종가'].dropna().values[-1] if len(df_초봉['종가'].dropna()) > 0 else None
            if len(df_초봉) == 0:
                continue

            # 마지막 데이터 잘라내기
            dt_마지막 = pd.Timestamp('now') - pd.Timedelta(seconds=self.n_초봉)
            df_초봉 = df_초봉[df_초봉.index <= dt_마지막]

            # 매수신호 탐색
            dic_매개변수_종목['df_초봉_매수봇'] = df_초봉
            dic_매수신호 = Logic.make_매수신호(dic_매개변수=dic_매개변수_종목)
            b_매수신호 = dic_매수신호['b_매수신호_매수봇']
            li_매수신호 = dic_매수신호['li_매수신호_매수봇']

            # 매개변수 업데이트
            dic_매개변수_종목['s_탐색시간_매수봇'] = pd.Timestamp('now').strftime('%H:%M:%S')
            dic_매개변수_종목['n_현재가_매수봇'] = n_현재가
            dic_매개변수_종목 = {**dic_매개변수_종목, **dic_매수신호}
            self.dic_매개변수[s_종목코드] = dic_매개변수_종목

            # log 기록
            li_신호종류 = dic_매개변수_종목['li_신호종류_매수봇']
            s_매수신호 = 'ok' if b_매수신호 else 'NG'
            n_반대신호 = len(li_매수신호) - sum(li_매수신호) if b_매수신호 == False else ''
            li_s_매수신호 = ['ok' if b_신호 else 'NG' for b_신호 in li_매수신호]
            s_li_매수신호 = ', '.join([f'{li_신호종류[i]}_{li_s_매수신호[i]}' for i in range(len(li_신호종류))])
            if b_로그:
                self.make_log_신호(f'{s_종목명}({s_종목코드})-{s_선정사유[:2]}\n'
                                 f'  #{s_매수신호}-{n_반대신호}# {s_li_매수신호}-초봉{len(df_초봉)}봉')

            # 매수 주문 (매수신호 모두 True 조건)
            if b_매수신호:
                # 매수 주문 요청
                n_현재가 = self.api.dic_실시간_현재가[s_종목코드] if s_종목코드 in self.api.dic_실시간_현재가.keys() else None
                n_주문단가 = self.find_주문단가(n_현재가=n_현재가, n_호가보정=+3) if n_현재가 is not None else None
                n_주문수량 = int(self.n_주문가능금액 / n_주문단가) if n_현재가 is not None else None
                self.api.send_주문(s_계좌번호=self.s_계좌번호, s_주문유형='매수', s_종목코드=s_종목코드,
                                 n_주문수량=n_주문수량, n_주문단가=n_주문단가, s_거래구분='지정가IOC')
                self.make_log_주문(f'++++ 매수 주문 ++++\n'
                                 f'\t{s_종목명}({s_종목코드}) - {s_선정사유} - 현재가 {n_현재가:,}\n'
                                 f'\t예수금 {self.n_주문가능금액:,}원'
                                 f' | 주문 {n_주문단가 * n_주문수량:,}원({n_주문단가:,}원*{n_주문수량:,}주)\n')

                # 주문정보 파일 업데이트
                dic_매개변수_종목['s_주문시간_매수봇'] = pd.Timestamp('now').strftime('%H:%M:%S')
                dic_매개변수_종목['n_현재가_매수봇'] = n_현재가
                dic_매개변수_종목['n_주문단가_매수봇'] = n_주문단가
                dic_매개변수_종목['n_주문수량_매수봇'] = n_주문수량
                self.update_주문정보파일(s_매수매도='매수', dic_매개변수=dic_매개변수_종목)

                # 계좌정보 업데이트
                self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
                self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

                # 초봉 저장
                li_빈칸 = [None] * (len(df_초봉) - 1)
                df_초봉['체결강도'] = li_빈칸 + [dic_매개변수_종목['n_체결강도_매수봇']]
                df_초봉['z_매수'] = li_빈칸 + [dic_매개변수_종목['n_z매수량_매수봇']]
                df_초봉['z_매도'] = li_빈칸 + [dic_매개변수_종목['n_z매도량_매수봇']]
                df_초봉['만원_매수'] = li_빈칸 + [dic_매개변수_종목['n_매수금액_매수봇']]
                df_초봉['선정사유'] = li_빈칸 + [dic_매개변수_종목['s_선정사유']]
                df_초봉['매수신호'] = li_빈칸 + [b_매수신호]
                for i, b_매수신호 in enumerate(li_매수신호):
                    s_신호종류 = li_신호종류[i]
                    df_초봉[f'{i}_{s_신호종류}'] = li_빈칸 + [b_매수신호]
                df_초봉['현재가'] = li_빈칸 + [n_현재가]
                s_현재시각 = pd.Timestamp("now").strftime("%H%M%S")
                s_파일명 = f'{self.n_초봉}초봉_{self.s_오늘}_{s_종목코드}_{s_종목명}_{s_현재시각}_매수'
                df_초봉.to_pickle(os.path.join(self.folder_초봉정보, f'{s_파일명}.pkl'))
                df_초봉.to_csv(os.path.join(self.folder_초봉정보, f'{s_파일명}.csv'), index=False, encoding='cp949')

                # 매개변수 업데이트 및 저장
                self.dic_매개변수[s_종목코드] = dic_매개변수_종목
                pd.to_pickle(self.dic_매개변수, self.path_매개변수)

                # 매수 탐색 종료 (매수 주문 시)
                break

            # 매수탐색 파일 업데이트
            self.update_신호탐색파일(s_매수매도='매수', dic_매개변수=dic_매개변수_종목)

        # ui 상태 업데이트
        self.lb_run_buybot.setText('[ 매수봇 ] 동작 대기')

    def run_매도봇(self):
        """ 매도 조건 확인하여 조건 만족 시 매도 주문 실행 """
        # ui 상태 업데이트 및 log 기록
        self.lb_run_sellbot.setText('[ 매도봇 ] 동작중')

        # 보유종목 정보 확인
        s_종목코드 = self.df_계좌잔고_종목별['종목코드'].values[0]
        s_종목명 = self.df_계좌잔고_종목별['종목명'].values[0]
        n_매수단가 = int(self.df_계좌잔고_종목별['매입가'].values[0])
        n_보유수량 = int(self.df_계좌잔고_종목별['보유수량'].values[0])

        # 종목별 매개변수 불러오기
        dic_매개변수_종목 = self.dic_매개변수[s_종목코드] if s_종목코드 in self.dic_매개변수 else dict()
        dic_매개변수_종목['s_종목코드'] = s_종목코드
        dic_매개변수_종목['s_종목명'] = s_종목명
        dic_매개변수_종목['n_매수단가_매도봇'] = n_매수단가
        dic_매개변수_종목['n_보유수량_매도봇'] = n_보유수량

        # 초봉 데이터 생성
        li_체결정보 = self.api.dic_실시간_체결[s_종목코드] if s_종목코드 in self.api.dic_실시간_체결.keys() else list()
        df_초봉 = Logic.make_초봉데이터_trader(li_체결정보=li_체결정보,
                                        s_오늘=self.s_오늘, n_초봉=self.n_초봉, s_종목코드=s_종목코드)

        # 마지막 데이터 잘라내기
        dt_마지막 = pd.Timestamp('now') - pd.Timedelta(seconds=self.n_초봉)
        df_초봉 = df_초봉[df_초봉.index <= dt_마지막]

        # 현재가 확인 (실처리 미존재 시 tr 요청 => 현재가 확인 + 실시간 등록)
        try:
            n_현재가 = self.api.dic_실시간_현재가[s_종목코드]
        except (AttributeError, KeyError):
            df_분봉 = self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=1)
            self.api.dic_실시간_현재가 = dict() if not hasattr(self.api, 'dic_실시간_현재가') else self.api.dic_실시간_현재가
            self.api.dic_실시간_현재가[s_종목코드] = df_분봉['종가'].values[0]
            n_현재가 = self.api.dic_실시간_현재가[s_종목코드]

        # 매도신호 탐색
        dic_매개변수_종목['df_초봉_매도봇'] = df_초봉
        dic_매개변수_종목['n_현재가_매도봇'] = n_현재가
        dic_매도신호 = Logic.make_매도신호(dic_매개변수=dic_매개변수_종목)
        b_매도신호 = dic_매도신호['b_매도신호_매도봇']
        li_매도신호 = dic_매도신호['li_매도신호_매도봇']

        # 매개변수 업데이트
        dic_매개변수_종목['s_탐색시간_매도봇'] = pd.Timestamp('now').strftime('%H:%M:%S')
        dic_매개변수_종목 = {**dic_매개변수_종목, **dic_매도신호}
        self.dic_매개변수[s_종목코드] = dic_매개변수_종목

        # log 기록
        n_수익률 = dic_매개변수_종목['n_수익률_매도봇']
        self.make_log_신호(f'{s_종목명}({s_종목코드})-초봉{len(df_초봉)}봉\n'
                         f'  [매수 {n_매수단가:,}원 | 현재 {n_현재가:,}원 | 수익 {n_수익률:.2f}%]-매도{b_매도신호}')

        # 매도 주문 (매도신호 중 1개 이상 True 조건)
        if b_매도신호:
            # 매도 주문 요청
            n_주문단가 = self.find_주문단가(n_현재가=n_현재가, n_호가보정=-2)
            n_주문수량 = int(n_보유수량)
            self.api.send_주문(s_계좌번호=self.s_계좌번호, s_주문유형='매도', s_종목코드=s_종목코드,
                             n_주문수량=n_주문수량, n_주문단가=n_주문단가, s_거래구분='지정가IOC')

            # log 기록
            li_신호종류 = dic_매개변수_종목['li_신호종류_매도봇']
            li_매도신호_수치 = dic_매개변수_종목['li_매도신호_수치_매도봇']
            li_s_매도신호 = [f'_{s_수치}' if b_신호 else '' for b_신호, s_수치 in zip(li_매도신호, li_매도신호_수치)]
            s_매도신호 = ', '.join([f'{li_신호종류[i]}{li_s_매도신호[i]}' for i in range(len(li_신호종류))])
            self.make_log_주문(f'---- 매도 주문 ----\n'
                             f'\t{s_종목명}({s_종목코드}) - 현재가 {n_현재가:,}\n'
                             f'\t단가 {n_주문단가:,}원 | 수량 {n_주문수량:,}주 | 금액 {n_주문단가 * n_주문수량:,}원\n'
                             f'\t[{s_매도신호}]\n')

            # 주문정보 파일 업데이트
            dic_매개변수_종목['s_주문시간_매도봇'] = pd.Timestamp('now').strftime('%H:%M:%S')
            dic_매개변수_종목['n_현재가_매도봇'] = n_현재가
            dic_매개변수_종목['n_주문단가_매도봇'] = n_주문단가
            dic_매개변수_종목['n_주문수량_매도봇'] = n_주문수량
            self.update_주문정보파일(s_매수매도='매도', dic_매개변수=dic_매개변수_종목)

            # 계좌정보 업데이트
            self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
            self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

            # 초봉 저장
            s_현재시각 = pd.Timestamp("now").strftime("%H%M%S")
            s_파일명 = f'{self.n_초봉}초봉_{self.s_오늘}_{s_종목코드}_{s_종목명}_{s_현재시각}_매도'
            df_초봉.to_pickle(os.path.join(self.folder_초봉정보, f'{s_파일명}.pkl'))
            df_초봉.to_csv(os.path.join(self.folder_초봉정보, f'{s_파일명}.csv'), index=False, encoding='cp949')

            # 매개변수 업데이트 및 저장
            dic_매개변수_종목['n_주문단가_매도봇'] = None
            dic_매개변수_종목['n_주문수량_매도봇'] = None
            self.dic_매개변수[s_종목코드] = dic_매개변수_종목
            pd.to_pickle(self.dic_매개변수, self.path_매개변수)

        # 이력 파일 업데이트
        self.update_신호탐색파일(s_매수매도='매도', dic_매개변수=dic_매개변수_종목)

        # ui 상태 업데이트
        self.lb_run_sellbot.setText('[ 매도봇 ] 동작 대기')

    ###################################################################################################################

    def get_전일날짜(self):
        """ 캐시변환 폴더에서 전일 날짜 찾아서 s_전일 리턴 """
        li_일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                 if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]
        li_일자 = [일자 for 일자 in li_일자 if 일자 < self.s_오늘]
        s_전일 = max(li_일자)

        return s_전일

    @staticmethod
    def make_호가단위():
        """ 호가단위 생성 후 list 리턴 """
        '''
        ### 호가단위 기준 (23.01.25 기준, 코스피/코스닥 동일)
            2,000원 미만 : 1원
            2,000원 이상 ~ 5,000원 미만 : 5원
            5,000원 이상 ~ 20,000원 미만 : 10원
            20,000원 이상 ~ 50,000원 미만 : 50원
            50,000원 이상 ~ 200,000원 미만 : 100원
            200,000원 이상 ~ 500,000원 미만 : 500원
            500,000원 이상 : 1,000원
        '''

        li_호가단위 = list()
        for n_호가 in range(0, 2 * 1000, 1):
            li_호가단위.append(n_호가)
        for n_호가 in range(2 * 1000, 5 * 1000, 5):
            li_호가단위.append(n_호가)
        for n_호가 in range(5 * 1000, 20 * 1000, 10):
            li_호가단위.append(n_호가)
        for n_호가 in range(20 * 1000, 50 * 1000, 50):
            li_호가단위.append(n_호가)
        for n_호가 in range(50 * 1000, 200 * 1000, 100):
            li_호가단위.append(n_호가)
        for n_호가 in range(200 * 1000, 500 * 1000, 500):
            li_호가단위.append(n_호가)

        return li_호가단위

    def get_대상선정(self, n_초봉=None, li_매매대상=None):
        """ 백테스팅 데이터 읽어와서 성능 데이터 기준 초봉(int), 매매대상(list) 선정 후 리턴 """
        # 초봉, 매매대상 입력 시 bypass 리턴
        if n_초봉 is not None and li_매매대상 is not None:
            return n_초봉, li_매매대상

        # 수익요약 파일 읽어오기
        try:
            df_수익요약 = pd.read_pickle(os.path.join(self.folder_수익요약, f'df_수익요약_{self.s_전일}.pkl'))
        except FileNotFoundError:
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_수익요약) if 'df_수익요약' in 파일명 and '.pkl' in 파일명]
            df_수익요약 = pd.read_pickle(os.path.join(self.folder_수익요약, max(li_파일명)))

        # 성능 기준 max 찾기
        s_성능 = df_수익요약.set_index('일자').T['10성능%'].idxmax()

        # 초봉, 매매대상 생성
        li_성능 = s_성능.split('|')
        n_초봉 = int(li_성능[0].replace('초', ''))
        dic_매매대상 = dict(일봉='일봉변동', vi='vi발동', 거래='거래량급증')
        li_매매대상 = [dic_매매대상[li_성능[1]]]

        return n_초봉, li_매매대상

    def get_대상종목(self, li_매매대상):
        """ 트레이더 구동 시 감시할 대상종목 읽어와서 전체 대상종목(dict), 매매 대상종목(df), 코드2종목명(dict) 리턴 """
        # 코드2종목명 생성
        try:
            df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_오늘}.pkl'))
        except FileNotFoundError:
            df_전체종목 = pd.read_pickle(os.path.join(self.folder_전체종목, f'df_전체종목_{self.s_전일}.pkl'))
        dic_코드2종목명 = df_전체종목.set_index('종목코드')['종목명'].to_dict()

        # 기존 대상종목 파일 읽어오기
        try:
            dic_대상종목 = pd.read_pickle(os.path.join(self.folder_대상종목, f'dic_대상종목_{self.s_오늘}.pkl'))
        except FileNotFoundError:
            dic_대상종목 = dict()

        # 기존 대상종목 갯수 확인
        path_기존_수집 = os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_수집.pkl')
        path_기존_매매 = os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_매매.pkl')
        n_대상종목_기존_수집 = len(pd.read_pickle(path_기존_수집)) if os.path.isfile(path_기존_수집) else 0
        n_대상종목_기존_매매 = len(pd.read_pickle(path_기존_매매)) if os.path.isfile(path_기존_매매) else 0

        # 대상종목 업데이트
        li_선정사유 = ['일봉변동', 'vi발동', '거래량급증']
        li_조건검색 = ['vi발동', '거래량급증']
        dic_조건검색 = self.api.get_조건검색_전체(li_대상=li_조건검색)
        for s_선정사유 in li_선정사유:
            # 기존 대상종목 불러오기
            df_대상종목_기존 = dic_대상종목[s_선정사유] if s_선정사유 in dic_대상종목.keys() else pd.DataFrame()
            df_대상종목_신규 = pd.DataFrame()

            # 대상1) 일봉변동 추가 (전일)
            if s_선정사유 == '일봉변동':
                df_대상종목_신규 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'df_일봉변동_{self.s_전일}.pkl'))
                df_대상종목_신규['선정사유'] = s_선정사유

            # 대상2) 조건검색 추가 (실시간)
            if s_선정사유 in li_조건검색:
                df_대상종목_신규 = pd.DataFrame()
                df_대상종목_신규['종목코드'] = dic_조건검색[s_선정사유]
                df_대상종목_신규['종목명'] = df_대상종목_신규['종목코드'].apply(lambda x: dic_코드2종목명[x]
                                                                                if x in dic_코드2종목명.keys() else None)
                df_대상종목_신규['선정사유'] = s_선정사유

            # 추가정보 생성
            df_대상종목_신규['추가시점'] = pd.Timestamp('now').strftime('%H:%M:%S')
            df_대상종목_신규 = df_대상종목_신규.loc[:, ['종목코드', '종목명', '선정사유', '추가시점']]

            # 대상종목 합치기
            df_대상종목 = pd.concat([df_대상종목_기존, df_대상종목_신규], axis=0).drop_duplicates(subset='종목코드')
            dic_대상종목[s_선정사유] = df_대상종목

        # 수집용 대상종목 생성
        li_df = [dic_대상종목[s_사유] for s_사유 in dic_대상종목.keys() if s_사유 in ['일봉변동', 'vi발동', '거래량급증']]
        df_대상종목_수집 = pd.concat(li_df, axis=0).drop_duplicates(subset='종목코드')

        # 매매용 대상종목 생성
        li_df = [dic_대상종목[s_사유] for s_사유 in dic_대상종목.keys() if s_사유 in li_매매대상]
        df_대상종목_매매 = pd.concat(li_df, axis=0).drop_duplicates(subset='종목코드')
        df_대상종목_매매['초봉'] = self.n_초봉

        # 실시간 종목등록
        s_대상종목_수집 = ';'.join(list(df_대상종목_수집['종목코드'])[:99])
        self.api.set_실시간_종목등록(s_종목코드=s_대상종목_수집, s_등록형태='신규')

        # 파일 저장
        pd.to_pickle(dic_대상종목, os.path.join(self.folder_대상종목, f'dic_대상종목_{self.s_오늘}.pkl'))
        df_대상종목_수집.to_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_수집.pkl'))
        df_대상종목_수집.to_csv(os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_수집.csv'),
                          index=False, encoding='cp949')
        df_대상종목_매매.to_pickle(os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_매매.pkl'))
        df_대상종목_매매.to_csv(os.path.join(self.folder_대상종목, f'df_대상종목_{self.s_오늘}_매매.csv'),
                          index=False, encoding='cp949')

        # 로그 기록
        s_대상별종목수 = ', '.join([f'{사유[:2]}-{len(df_대상종목_매매[df_대상종목_매매["선정사유"] == 사유])}'
                                    for 사유 in df_대상종목_매매['선정사유'].unique()])
        self.make_log(f'수집종목 업데이트 {n_대상종목_기존_수집} -> {len(df_대상종목_수집)}\n'
                      f'\t 매매종목 업데이트 {n_대상종목_기존_매매} -> {len(df_대상종목_매매)} ({s_대상별종목수})')

        return dic_대상종목, df_대상종목_매매, dic_코드2종목명

    def find_주문단가(self, n_현재가, n_호가보정):
        """ 주문가 산정을 위해 해당 종목의 현재가 대비 호가보정 후 int 리턴 """
        # 50만원 이상 구간 확인
        li_호가단위_추가 = list()
        if n_현재가 >= 500000:
            for n_호가 in range(500000, n_현재가 + 10 * 1000, 1000):
                li_호가단위_추가.append(n_호가)

        # 호가 확인
        if n_호가보정 > 0:
            li_호가 = self.li_호가단위 + li_호가단위_추가
            li_호가 = [호가 for 호가 in li_호가 if 호가 > n_현재가]
            n_주문단가 = li_호가[n_호가보정 - 1]

        elif n_호가보정 < 0:
            li_호가 = self.li_호가단위 + li_호가단위_추가
            li_호가 = [호가 for 호가 in li_호가 if 호가 < n_현재가]
            n_주문단가 = li_호가[n_호가보정]

        else:
            n_주문단가 = n_현재가

        return n_주문단가

    def update_주문정보파일(self, s_매수매도, dic_매개변수):
        """ 매수매도 주문정보 수집 후 csv 파일로 저장 """
        # 데이터 포맷 준비 (컬럼명 통일 문제로 df 사용)
        df_주문정보 = pd.DataFrame()
        df_주문정보['일자'] = [dic_매개변수['s_일자']]
        df_주문정보['종목코드'] = dic_매개변수['s_종목코드']
        df_주문정보['종목명'] = dic_매개변수['s_종목명']

        # 데이터 생성 - 매수
        if s_매수매도 == '매수':
            df_주문정보['주문시간'] = dic_매개변수['s_주문시간_매수봇']
            df_주문정보['현재가'] = dic_매개변수['n_현재가_매수봇']
            df_주문정보['주문구분'] = s_매수매도
            df_주문정보['주문단가'] = dic_매개변수['n_주문단가_매수봇'] if dic_매개변수['n_주문단가_매수봇'] is not None else 0
            df_주문정보['주문수량'] = dic_매개변수['n_주문수량_매수봇'] if dic_매개변수['n_주문수량_매수봇'] is not None else 0
            df_주문정보['주문금액'] = df_주문정보['주문단가'].values[0] * df_주문정보['주문수량'].values[0]
            df_주문정보['n_초봉'] = dic_매개변수['n_초봉']
            df_주문정보['n_z매수량'] = dic_매개변수['n_z매수량_매수봇']
            df_주문정보['n_z매도량'] = dic_매개변수['n_z매도량_매수봇']
            df_주문정보['n_매수금액'] = dic_매개변수['n_매수금액_매수봇']
            df_주문정보['n_체결강도'] = dic_매개변수['n_체결강도_매수봇']
            df_주문정보['li_신호종류_매수'] = f"[{', '.join(dic_매개변수['li_신호종류_매수봇'])}]"

        # 데이터 생성 - 매도
        if s_매수매도 == '매도':
            li_매도신호 = dic_매개변수['li_매도신호_매도봇']
            li_매도신호종류 = dic_매개변수['li_신호종류_매도봇']
            df_주문정보['주문시간'] = dic_매개변수['s_주문시간_매도봇']
            df_주문정보['현재가'] = dic_매개변수['n_현재가_매도봇']
            df_주문정보['주문구분'] = s_매수매도
            df_주문정보['주문단가'] = dic_매개변수['n_주문단가_매도봇'] if dic_매개변수['n_주문단가_매도봇'] is not None else 0
            df_주문정보['주문수량'] = dic_매개변수['n_주문수량_매도봇'] if dic_매개변수['n_주문수량_매도봇'] is not None else 0
            df_주문정보['주문금액'] = df_주문정보['주문단가'].values[0] * df_주문정보['주문수량'].values[0]
            for idx in range(len(li_매도신호)):
                df_주문정보[f'매도{idx + 1}{li_매도신호종류[idx]}'] = li_매도신호[idx]
            df_주문정보['n_수익률'] = dic_매개변수['n_수익률_매도봇']
            df_주문정보['n_경과초'] = dic_매개변수['n_경과초_매도봇']
            df_주문정보['n_매도량_누적'] = dic_매개변수['n_매도량_누적_매도봇']
            df_주문정보['n_매수량_누적'] = dic_매개변수['n_매수량_누적_매도봇']
            df_주문정보['n_매수량max'] = dic_매개변수['n_매수량max_매도봇']
            df_주문정보['n_매수량'] = dic_매개변수['n_매수량_매도봇']
            df_주문정보['li_신호종류_매도'] = f"[{', '.join(dic_매개변수['li_신호종류_매도봇'])}]"
            df_주문정보['li_매도신호_수치'] = f"[{', '.join(dic_매개변수['li_매도신호_수치_매도봇'])}]"
            df_주문정보['매도사유'] = dic_매개변수['s_매도사유_매도봇']

        # df 통합
        s_파일명 = f'주문정보_{self.s_오늘}'
        path_주문정보 = os.path.join(self.folder_주문정보, f'{s_파일명}.pkl')
        df_주문정보_기존 = pd.read_pickle(path_주문정보) if os.path.isfile(path_주문정보) else pd.DataFrame()
        df_주문정보 = pd.concat([df_주문정보_기존, df_주문정보], axis=0).drop_duplicates(subset=['종목코드', '주문시간'])

        # df 저장
        df_주문정보.to_pickle(os.path.join(self.folder_주문정보, f'{s_파일명}.pkl'))
        df_주문정보.to_csv(os.path.join(self.folder_주문정보, f'{s_파일명}.csv'), index=False, encoding='cp949')

    def update_신호탐색파일(self, s_매수매도, dic_매개변수):
        """ 매수/매도 신호탐색 결과 수집 후 txt 파일로 저장 """
        # 데이터 포맷 준비
        dic_신호탐색 = dict()
        dic_신호탐색['일자'] = dic_매개변수['s_일자']
        dic_신호탐색['종목코드'] = dic_매개변수['s_종목코드']
        dic_신호탐색['종목명'] = dic_매개변수['s_종목명']
        dic_신호탐색['초봉'] = dic_매개변수['n_초봉']

        # 데이터 생성 - 매수
        if s_매수매도 == '매수':
            # 변수 지정
            li_매수신호 = dic_매개변수['li_매수신호_매수봇']
            li_신호종류 = dic_매개변수['li_신호종류_매수봇']

            # 신호탐색 결과 생성
            dic_신호탐색['시간'] = dic_매개변수['s_탐색시간_매수봇']
            dic_신호탐색['매수신호'] = sum(li_매수신호) == len(li_매수신호)
            for i in range(len(li_매수신호)):
                dic_신호탐색[f'매수{i + 1}{li_신호종류[i]}'] = li_매수신호[i]
            dic_신호탐색['현재가'] = dic_매개변수['n_현재가_매수봇']
            dic_신호탐색['z매수량'] = dic_매개변수['n_z매수량_매수봇']
            dic_신호탐색['z매도량'] = dic_매개변수['n_z매도량_매수봇']
            dic_신호탐색['매수금액'] = dic_매개변수['n_매수금액_매수봇']
            dic_신호탐색['체결강도'] = dic_매개변수['n_체결강도_매수봇']

        # 데이터 생성 - 매도
        if s_매수매도 == '매도':
            # 변수 지정
            li_매도신호 = dic_매개변수['li_매도신호_매도봇']
            li_신호종류 = dic_매개변수['li_신호종류_매도봇']

            # 신호탐색 결과 생성
            dic_신호탐색['시간'] = dic_매개변수['s_탐색시간_매도봇']
            dic_신호탐색['현재가'] = dic_매개변수['n_현재가_매도봇']
            dic_신호탐색['수익률'] = dic_매개변수['n_수익률_매도봇']
            dic_신호탐색['매도신호'] = sum(li_매도신호) > 0
            for i in range(len(li_매도신호)):
                dic_신호탐색[f'매도{i + 1}{li_신호종류[i]}'] = li_매도신호[i]
            dic_신호탐색['주문단가'] = dic_매개변수['n_주문단가_매도봇'] if 'n_주문단가_매도봇' in dic_매개변수.keys() else None
            dic_신호탐색['주문수량'] = dic_매개변수['n_주문수량_매도봇'] if 'n_주문수량_매도봇' in dic_매개변수.keys() else None
            dic_신호탐색['주문금액'] = dic_주문정보['주문단가'] * dic_주문정보['주문수량']\
                if dic_신호탐색['주문단가'] is not None and dic_신호탐색['주문수량'] is not None else None
            dic_신호탐색['n_수익률'] = dic_매개변수['n_수익률_매도봇']
            dic_신호탐색['n_경과초'] = dic_매개변수['n_경과초_매도봇']
            dic_신호탐색['n_매도량_누적'] = dic_매개변수['n_매도량_누적_매도봇']
            dic_신호탐색['n_매수량_누적'] = dic_매개변수['n_매수량_누적_매도봇']
            dic_신호탐색['n_매수량max'] = dic_매개변수['n_매수량max_매도봇']
            dic_신호탐색['n_매수량'] = dic_매개변수['n_매수량_매도봇']
            dic_신호탐색['li_신호종류_매도'] = dic_매개변수['li_신호종류_매도봇']
            dic_신호탐색['li_매도신호_수치'] = dic_매개변수['li_매도신호_수치_매도봇']

        # 텍스트 파일 저장
        path_신호탐색 = os.path.join(self.folder_신호탐색, f'신호탐색_{self.s_오늘}_{s_매수매도}.csv')
        s_헤더 = ','.join([str(x) for x in dic_신호탐색.keys()])
        s_데이터 = ','.join([str(x) for x in dic_신호탐색.values()])
        s_신호탐색 = f'{s_데이터}' if os.path.isfile(path_신호탐색) else f'{s_헤더}\n{s_데이터}'
        with open(path_신호탐색, mode='at', encoding='cp949') as file:
            file.write(f'{s_신호탐색}\n')

    def convert_이력파일(self):
        """ pkl 형식으로 저장된 이력 파일을 csv 형식으로 변환 후 저장 """
        # 기준정보 정의
        li_li_폴더파일 = [[self.folder_주문정보, f'주문정보_{self.s_오늘}']]

        # 파일 변환 후 저장
        for li_폴더파일 in li_li_폴더파일:
            path_pkl = os.path.join(li_폴더파일[0], f'{li_폴더파일[1]}.pkl')
            path_csv = os.path.join(li_폴더파일[0], f'{li_폴더파일[1]}.csv')
            if not os.path.isfile(path_csv):
                try:
                    df_파일 = pd.read_pickle(path_pkl)
                    df_파일.to_csv(path_csv, index=False, encoding='cp949')
                    self.make_log(f'csv 생성 완료 - {li_폴더파일[1]}')
                except FileNotFoundError:
                    self.make_log(f'pkl 미존재 - {li_폴더파일[1]}')

    def setui_초기설정(self):
        """ ui 내 정보 표시 및 버튼 동작 설정 """
        # 서버 정보 표시
        self.lb_info_server.setText(self.s_접속서버)
        self.lb_info_account.setText(f'[ 계좌 ] {self.s_계좌번호}')

        # 설정 정보 표시
        self.lb_time_start.setText(f'[ 시작시각 ] {self.s_시작시각}')
        self.lb_time_end.setText(f'[ 종료시각 ] {self.s_종료시각}')
        self.lb_cash_max.setText(f'[ 자본금 ] {int(self.s_자본금.replace(",", "")):,}')

        # 버튼 동작 설정
        self.pb_run_buybot.clicked.connect(self.run_buybot)
        self.pb_run_sellbot.clicked.connect(self.run_sellbot)

    def setui_실시간(self):
        """ 상태표시줄 업데이트 """
        # 정보 생성
        dt_현재 = pd.Timestamp('now')
        n_초 = int(dt_현재.strftime('%S'))
        dic_요일 = {'Mon': '월', 'Tue': '화', 'Wed': '수', 'Thu': '목', 'Fri': '금', 'Sat': '토', 'Sun': '일'}

        # 상태표시줄 업데이트
        s_깜빡이 = '□' if n_초 % 2 == 0 else '■'
        s_추가정보 = ''
        if len(self.df_계좌잔고_종목별) > 0:
            s_종목코드 = self.df_계좌잔고_종목별['종목코드'].values[0]
            s_종목명 = self.df_계좌잔고_종목별['종목명'].values[0]
            n_매수단가 = int(self.df_계좌잔고_종목별['매입가'].values[0])
            n_보유수량 = int(self.df_계좌잔고_종목별['보유수량'].values[0])
            n_매수금액 = int(n_매수단가 * n_보유수량)
            n_수익률 = self.df_계좌잔고_종목별['수익률'].values[0]
            n_수익금 = int(self.df_계좌잔고_종목별['평가손익'].values[0])
            s_추가정보1 = f'    [보유종목] {s_종목명}({s_종목코드}) | {n_매수단가:,}원 | {n_보유수량:,}주 | {n_매수금액:,}원'
            s_추가정보2 = f'    [예상손익] {n_수익률:.2f}% | {n_수익금:,}원'
            s_추가정보 = s_추가정보1 + s_추가정보2

        self.statusbar.showMessage(f'    {s_깜빡이} 서버 접속 중 | {self.s_접속서버} | {self.s_계좌번호}' + s_추가정보)

        # 일자 및 시각 정보 업데이트
        s_날짜_ui = f'{dt_현재.strftime("%y-%m-%d")} ({dic_요일[dt_현재.strftime("%a")]})'
        s_시각_ui = dt_현재.strftime('%H:%M:%S')
        self.lb_info_date.setText(s_날짜_ui)
        self.lb_info_time.setText(s_시각_ui)

    def setui_예수금(self):
        """ D+2 예수금 조회 후 ui에 표시 및 변수 업데이트 (주문가능금액 동시 업데이트) """
        self.n_예수금 = self.api.get_tr_예수금(s_계좌번호=self.s_계좌번호)
        self.lb_info_cash.setText(f'[ 예수금 ] {self.n_예수금:,}')

        self.n_주문가능금액 = min(int(self.n_예수금), int(self.s_자본금.replace(',', '')))

    def setui_거래이력(self):
        """ 체결잔고 csv 파일 읽어와서 ui에 표시 """
        # 체결잔고 읽어오기 (없으면 이전 파일에서 양식 가져오기)
        s_일자 = self.s_오늘
        try:
            df_체결잔고 = pd.read_csv(os.path.join(self.folder_체결잔고, f'체결잔고_{s_일자}.csv'), encoding='cp949')
            df_체결잔고 = df_체결잔고[pd.notna(df_체결잔고['주문상태'])]
        except FileNotFoundError:
            s_파일명_최근 = max([파일명 for 파일명 in os.listdir(self.folder_체결잔고) if '체결잔고' in 파일명])
            df_체결잔고 = pd.read_csv(os.path.join(self.folder_체결잔고, s_파일명_최근), encoding='cp949')
            df_체결잔고 = df_체결잔고[0:0]

        # df 정리 (전체 컬럼 str으로 변환 필요)
        df_거래이력 = pd.DataFrame()
        df_거래이력['시간'] = df_체결잔고['시간']
        df_거래이력['계좌번호'] = df_체결잔고['계좌번호'].apply(lambda x: str(x))
        df_거래이력['종목코드'] = df_체결잔고['종목코드']
        df_거래이력['종목명'] = df_체결잔고['종목명'].apply(lambda x: x.strip())
        df_거래이력['주문상태'] = df_체결잔고['주문상태']
        df_거래이력['주문구분'] = df_체결잔고['주문구분']
        df_거래이력['주문수량'] = df_체결잔고['주문수량'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['주문가격'] = df_체결잔고['주문가격'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['체결량'] = df_체결잔고['체결량'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['체결가'] = df_체결잔고['체결가'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['미체결수량'] = df_체결잔고['미체결수량'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['체결누계금액'] = df_체결잔고['체결누계금액'].apply(lambda x: f'{float(x):,.0f}')
        df_거래이력['현재가'] = df_체결잔고['현재가'].apply(lambda x: f'{abs(float(x)):,.0f}')
        df_거래이력['주문체결시간'] = df_체결잔고['주문체결시간']
        df_거래이력['일자'] = f'{s_일자[4:6]}-{s_일자[6:8]}'

        li_컬럼명 = ['일자'] + [컬럼명 for 컬럼명 in df_거래이력.columns if 컬럼명 not in ['일자']]
        df_거래이력 = df_거래이력.loc[:, li_컬럼명]

        # 계좌 걸러내기
        df_거래이력 = df_거래이력[df_거래이력['계좌번호'] == self.s_계좌번호]
        df_거래이력 = df_거래이력[df_거래이력['주문상태'] == '체결']
        ary_거래이력 = df_거래이력.values

        # 테이블 모델 생성
        model_거래이력 = QStandardItemModel(df_거래이력.shape[0], df_거래이력.shape[1])
        model_거래이력.setHorizontalHeaderLabels(df_거래이력.columns)

        for n_row, ary_row in enumerate(ary_거래이력):
            for n_col, s_항목 in enumerate(ary_row):
                obj_정렬 = Qt.AlignRight if n_col in [] else Qt.AlignCenter
                obj_항목 = QStandardItem(str(s_항목))
                obj_항목.setTextAlignment(obj_정렬)
                model_거래이력.setItem(n_row, n_col, obj_항목)

        # 테이블 모델 연결
        tv_거래이력 = self.tv_history_trade
        tv_거래이력.setModel(model_거래이력)
        tv_거래이력.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

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

    def make_log_신호(self, s_text):
        """ 입력 받은 s_text에 시간 붙여서 self.path_log_신호에 저장 """
        # 정보 설정
        s_시각 = pd.Timestamp('now').strftime('%H:%M:%S')
        s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
        s_모듈 = sys._getframe(1).f_code.co_name

        # log 생성
        s_log = f'[{s_시각}] {s_파일} | {s_모듈} | {s_text}'

        # log 출력 (콘솔)
        print(s_log)

        # log 출력 (log_주문 파일)
        with open(self.path_log_신호, mode='at', encoding='cp949') as file:
            file.write(f'{s_log}\n')

        # log 출력 (ui)
        s_대상 = '매수' if s_모듈 == 'run_매수봇' else '매도' if s_모듈 == 'run_매도봇' else None
        if s_대상 is None:
            return
        obj_로그창 = self.te_info_buybot if s_대상 == '매수' else self.te_info_sellbot
        obj_로그창.appendPlainText(f'{s_log}\n')
        obj_스크롤바 = obj_로그창.verticalScrollBar()
        obj_스크롤바.setValue(obj_스크롤바.maximum())

    def make_log_주문(self, s_text):
        """ 입력 받은 s_text에 시간 붙여서 self.path_log_주문에 저장 """
        # 정보 설정
        s_시각 = pd.Timestamp('now').strftime('%H:%M:%S')
        s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
        s_모듈 = sys._getframe(1).f_code.co_name

        # log 생성
        s_log = f'[{s_시각}] {s_파일} | {s_모듈} | {s_text}'

        # log 출력 (콘솔)
        print(s_log)

        # log 출력 (log_주문 파일)
        with open(self.path_log_주문, mode='at', encoding='cp949') as file:
            file.write(f'{s_log}\n')

        # log 출력 (ui)
        s_대상 = '매수' if s_모듈 == 'run_매수봇' else '매도' if s_모듈 == 'run_매도봇' else None
        if s_대상 is None:
            return
        obj_로그창 = self.te_info_buybot if s_대상 == '매수' else self.te_info_sellbot
        obj_로그창.appendPlainText(f'{s_log}\n')
        obj_스크롤바 = obj_로그창.verticalScrollBar()
        obj_스크롤바.setValue(obj_스크롤바.maximum())

    def update_모니터링파일(self):
        """ 접속 확인용 모니터링 파일 업데이트 """
        # 파일 업데이트
        pd.to_pickle(self.s_접속서버, self.path_모니터링)

        # ui 업데이트
        s_시각 = pd.Timestamp('now').strftime('%H:%M:%S')
        s_text = f'{s_시각} | MNT update'
        obj_로그창 = self.te_info_run
        obj_로그창.appendPlainText(f'{s_text}')
        obj_스크롤바 = obj_로그창.verticalScrollBar()
        obj_스크롤바.setValue(obj_스크롤바.maximum())

    def run_mainbot(self):
        """ 모듈 이름 변경용 버퍼 """
        self.run_메인봇()

    def run_buybot(self):
        """ 모듈 이름 변경용 버퍼 """
        self.run_매수봇()

    def run_sellbot(self):
        """ 모듈 이름 변경용 버퍼 """
        self.run_매도봇()


#######################################################################################################################

if __name__ == "__main__":
    app = QApplication(sys.argv)
    style_fusion = QStyleFactory.create('Fusion')
    app.setStyle(style_fusion)

    t = Trader()
    t.show()
    app.exec_()

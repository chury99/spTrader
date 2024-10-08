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

import analyzer_sr알고리즘 as Logic
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
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_체결잔고 = dic_폴더정보['이력|체결잔고']
        self.folder_탐색결과 = dic_폴더정보['이력|탐색결과']
        self.folder_주문정보 = dic_폴더정보['이력|주문정보']
        self.folder_일봉변동 = dic_폴더정보['sr종목선정|10_일봉변동']
        self.folder_지지저항 = dic_폴더정보['sr종목선정|20_지지저항']
        self.folder_감시대상 = dic_폴더정보['sr종목선정|50_종목선정']
        os.makedirs(self.folder_탐색결과, exist_ok=True)
        os.makedirs(self.folder_주문정보, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        s_로그이름 = dic_config['로그이름_trader']
        self.path_log = os.path.join(dic_config['folder_log'], f'{s_로그이름}_{self.s_오늘}.log')
        self.path_log_신호 = os.path.join(dic_config['folder_log'], f'{s_로그이름}_신호_{self.s_오늘}.log')
        self.path_log_주문 = os.path.join(dic_config['folder_log'], f'{s_로그이름}_주문_{self.s_오늘}.log')
        self.path_모니터링 = os.path.join(self.folder_run, '모니터링_trader.pkl')
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
        self.li_대상종목, self.dic_코드2종목명_대상종목 = self.  get_대상종목()
        self.dic_지지저항, self.df_지지저항 = self.get_지지저항()

        # log 기록
        self.make_log(f'### Short Punch Trader 시작 ({self.s_접속서버}) ###')
        self.make_log(f'대상종목 감시 시작 - {len(self.li_대상종목)}개 종목')

        # 초기 설정
        self.setui_초기설정()
        self.setui_예수금()
        self.setui_거래이력()

        self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
        self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0
        self.dic_3분봉 = dict()

        # 실시간 설정
        # s_대상종목 = ';'.join(self.li_대상종목)
        # self.api.set_실시간_종목등록(s_종목코드=s_대상종목, s_등록형태='신규')

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
        n_현재_분 = int(dt_현재.strftime('%M'))
        n_현재_초 = int(dt_현재.strftime('%S'))

        # 1초 단위 업데이트
        self.setui_실시간()

        # 모니터링 파일 생성 (재구동 대기시간 절반 주기)
        if n_현재_초 % self.n_모니터링파일생성주기 == 0:
            self.update_모니터링파일()

        # 매수봇 호출
        if self.flag_종목보유 is False:
            if n_현재_분 % 3 == 0 and n_현재_초 == 1:
                self.run_매수봇()

        # 매도봇 호출
        if self.flag_종목보유 is True:
            if n_현재_초 % 1 == 0:
                self.run_매도봇()

        # 초기 설정 (3분 주기)
        if n_현재_분 % 3 == 0 and n_현재_초 == 30:
            self.setui_예수금()
            self.setui_거래이력()

            self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
            self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

        # 파일 변환
        if dt_현재 > pd.Timestamp('15:19:00') and n_현재_초 == 10:
            self.convert_이력파일()

    def run_매수봇(self):
        """ 매수 조건 확인하여 조건 만족 시 매수 주문 실행 """
        # ui 상태 업데이트 및 log 기록
        self.lb_run_buybot.setText('[ 매수봇 ] 동작중')
        self.make_log(f'매수신호 탐색')
        self.make_log_신호(f'##### 매수신호 탐색 #####\n'
                         f'  ===== 자리검증 | 추세검증 | 배열검증 | sr검증 | 시간검증 =====')

        # 대상 종목별 매수신호 탐색
        for s_종목코드 in self.li_대상종목:
            # 모니터링 파일 생성 주기 확인 및 업데이트
            n_현재_초 = int(pd.Timestamp('now').strftime('%S'))
            if n_현재_초 % self.n_모니터링파일생성주기 == 0:
                self.update_모니터링파일()

            # 3분봉 데이터 조회 (실시간 데이터 자동 등록)
            time.sleep(0.2)
            df_3분봉 = self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=3)
            self.dic_3분봉[s_종목코드] = df_3분봉
            # self.api.set_실시간_종목등록(s_종목코드=s_종목코드, s_등록형태='신규')

            # 현재가 초기값 생성
            self.api.dic_실시간_현재가 = dict() if not hasattr(self.api, 'dic_실시간_현재가') else self.api.dic_실시간_현재가
            self.api.dic_실시간_현재가[s_종목코드] = df_3분봉['종가'].values[0]

            # 추가 데이터 생성
            # df_3분봉 = Chart.find_전일종가(df_ohlcv=df_3분봉)     # 장시간 소요 (꼭 필요 시 대상 최소화 후 진행)
            df_3분봉 = Chart.make_이동평균(df_ohlcv=df_3분봉)

            # 지지저항 업데이트
            df_지지저항_전일 = self.df_지지저항[self.df_지지저항['종목코드'] == s_종목코드].copy()
            df_지지저항 = Logic.find_지지저항_추가통합(df_지지저항_기존=df_지지저항_전일, df_ohlcv_신규=df_3분봉[-130:])
            li_지지저항 = list(df_지지저항['고가'].values)
            self.dic_지지저항[s_종목코드] = li_지지저항

            # 매수신호 탐색
            ret_매수신호 = Logic.find_매수신호(df_ohlcv=df_3분봉, li_지지저항=li_지지저항)
            li_매수신호, dic_신호상세 = ret_매수신호
            li_신호종류 = ['자리', '추세', '배열', 'sr', '시간']
            b_매수신호 = sum(li_매수신호) == len(li_매수신호)

            # log 기록
            s_종목명 = self.dic_코드2종목명_대상종목[s_종목코드]
            s_매수신호 = 'ok' if b_매수신호 else 'NG'
            n_반대신호 = len(li_매수신호) - sum(li_매수신호) if b_매수신호 == False else ''
            li_s_매수신호 = ['ok' if b_신호 else 'NG' for b_신호 in li_매수신호]
            s_li_매수신호 = ', '.join([f'{li_신호종류[i]}_{li_s_매수신호[i]}' for i in range(len(li_신호종류))])
            self.make_log_신호(f'{s_종목명}({s_종목코드})\n'
                             f'  #{s_매수신호}-{n_반대신호}# {s_li_매수신호}')

            # 매수 주문 (매수신호 모두 True 조건)
            n_현재가 = self.api.dic_실시간_현재가[s_종목코드]
            if b_매수신호:
                # 매수 주문 요청
                n_주문단가 = self.find_주문단가(n_현재가=n_현재가, n_호가보정=+3)
                n_주문수량 = int(self.n_주문가능금액 / n_주문단가)
                self.api.send_주문(s_계좌번호=self.s_계좌번호, s_주문유형='매수', s_종목코드=s_종목코드,
                                 n_주문수량=n_주문수량, n_주문단가=n_주문단가, s_거래구분='지정가IOC')
                self.make_log_주문(f'#### 매수 주문 ####\n'
                                 f'\t{s_종목명}({s_종목코드}) - 현재가 {n_현재가:,}\n'
                                 f'\t단가 {n_주문단가:,}원 | 수량 {n_주문수량:,}주 | 금액 {n_주문단가 * n_주문수량:,}원\n')

                # 주문정보 파일 업데이트
                dic_주문정보 = dict(df_3분봉=df_3분봉, s_주문구분='매수', n_현재가=n_현재가,
                                n_주문단가=n_주문단가, n_주문수량=n_주문수량, li_지지저항=li_지지저항)
                self.update_주문정보파일(dic_주문정보=dic_주문정보, dic_신호상세=dic_신호상세)

                # 계좌정보 업데이트
                self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
                self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

                # 매수 탐색 종료
                break

            else:
                self.api.set_실시간_종목해제(s_종목코드=s_종목코드)

            # 매수탐색 파일 업데이트
            dic_탐색정보 = dict(df_3분봉=df_3분봉, li_매수신호=li_매수신호, li_신호종류=li_신호종류, n_현재가=n_현재가)
            self.update_매수탐색파일(dic_탐색정보=dic_탐색정보, dic_신호상세=dic_신호상세)

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

        # 지지저항 확인 (주문정보 확인)
        df_주문정보 = pd.read_pickle(os.path.join(self.folder_주문정보, f'매수매도주문_{self.s_오늘}.pkl'))
        df_주문정보 = df_주문정보[df_주문정보['주문구분'] == '매수']
        df_주문정보 = df_주문정보[df_주문정보['종목코드'] == s_종목코드].sort_values('시간')
        n_지지선 = df_주문정보['지지선'].values[-1] if len(df_주문정보) > 0 else None
        n_저항선 = df_주문정보['저항선'].values[-1] if len(df_주문정보) > 0 else None

        # 3분봉 확인 (최종 조회 이후 3분 경과 시 재조회)
        df_3분봉 = self.dic_3분봉[s_종목코드] if s_종목코드 in self.dic_3분봉.keys()\
                                         else self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=3)
        s_최종조회시간 = df_3분봉[df_3분봉['일자'] == df_3분봉['일자'].max()]['시간'].max()
        if pd.Timestamp('now') - pd.Timestamp(s_최종조회시간) > pd.Timedelta(minutes=3):
            df_3분봉 = self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=3)
        self.dic_3분봉[s_종목코드] = df_3분봉

        # 추가 데이터 생성
        df_3분봉 = Chart.make_이동평균(df_ohlcv=df_3분봉)

        # dic_기준정보 생성
        dic_기준정보 = dict(df_3분봉=df_3분봉, n_매수단가=n_매수단가, n_지지선=n_지지선, n_저항선=n_저항선)

        # 현재가 확인 (실처리 미존재 시 tr 요청 => 현재가 확인 + 실시간 등록)
        try:
            n_현재가 = self.api.dic_실시간_현재가[s_종목코드]
        except AttributeError:
            df_3분봉 = self.api.get_tr_분봉조회(s_종목코드=s_종목코드, n_틱범위=3)
            self.api.dic_실시간_현재가 = dict() if not hasattr(self.api, 'dic_실시간_현재가') else self.api.dic_실시간_현재가
            self.api.dic_실시간_현재가[s_종목코드] = df_3분봉['종가'].values[0]
            n_현재가 = self.api.dic_실시간_현재가[s_종목코드]

        # 매도신호 탐색
        ret_매도신호 = Logic.find_매도신호(dic_기준정보=dic_기준정보, n_현재가=n_현재가)
        li_매도신호, dic_신호상세 = ret_매도신호
        b_매도신호 = sum(li_매도신호) > 0

        # log 기록
        n_수익률 = (n_현재가 / n_매수단가 - 1) * 100 - 0.2
        self.make_log_신호(f'{s_종목명}({s_종목코드})\n'
                         f'\t[매수 {n_매수단가:,}원 | 현재 {n_현재가:,}원 | 수익 {n_수익률:.2f}%]'
                         f' tr-{dic_신호상세["s_3분봉시간"][:5]}')

        # 매도 주문 (매도신호 중 1개 이상 True 조건)
        if b_매도신호:
            # 매도 주문 요청
            n_주문단가 = self.find_주문단가(n_현재가=n_현재가, n_호가보정=-2)
            n_주문수량 = int(n_보유수량)
            self.api.send_주문(s_계좌번호=self.s_계좌번호, s_주문유형='매도', s_종목코드=s_종목코드,
                             n_주문수량=n_주문수량, n_주문단가=n_주문단가, s_거래구분='지정가IOC')

            # log 기록
            li_s_매도신호 = ['ON' if b_신호 else 'off' for b_신호 in li_매도신호]
            li_신호종류 = ['저항터치', '지지붕괴', '추세이탈', '하락한계', '장종료']
            s_매도신호 = ', '.join([f'{li_신호종류[i]}_{li_s_매도신호[i]}' for i in range(len(li_신호종류))])
            self.make_log_주문(f'#### 매도 주문 ####\n'
                             f'\t{s_종목명}({s_종목코드}) - 현재가 {n_현재가:,}\n'
                             f'\t단가 {n_주문단가:,}원 | 수량 {n_주문수량:,}주 | 금액 {n_주문단가 * n_주문수량:,}원\n'
                             f'\t[{s_매도신호}]')

            # 주문정보 파일 업데이트
            dic_주문정보 = dict(df_3분봉=df_3분봉, s_주문구분='매도', n_현재가=n_현재가,
                            n_주문단가=n_주문단가, n_주문수량=n_주문수량, li_매도신호=li_매도신호, li_매도신호종류=li_신호종류)
            self.update_주문정보파일(dic_주문정보=dic_주문정보, dic_신호상세=dic_신호상세)

            # 계좌정보 업데이트
            self.df_계좌잔고_전체, self.df_계좌잔고_종목별 = self.api.get_tr_계좌잔고(s_계좌번호=self.s_계좌번호)
            self.flag_종목보유 = len(self.df_계좌잔고_종목별) > 0

        # 이력 파일 업데이트
        dic_탐색정보 = dict(s_종목코드=s_종목코드, s_종목명=s_종목명,
                        n_매수단가=n_매수단가, n_현재가=n_현재가, n_수익률=n_수익률, li_매도신호=li_매도신호,
                        n_주문단가=n_주문단가 if b_매도신호 else None, n_주문수량=n_주문수량 if b_매도신호 else None)
        self.update_매도탐색파일(dic_탐색정보=dic_탐색정보, dic_신호상세=dic_신호상세)

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

    def get_대상종목(self):
        """ 트레이더 구동 시 감시할 대상종목 읽어와서 종목코드(list), 종목명(dict)  리턴 """
        # 감시대상 파일 읽어오기
        # df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_종목선정_{self.s_전일}.pkl'))
        df_감시대상 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'df_일봉변동_{self.s_전일}.pkl'))

        # 종목코드 골라내기
        li_대상종목 = list(df_감시대상['종목코드'].sort_values().unique())

        # 종목명 변환용 dict 생성
        dic_코드2종목명_대상종목 = df_감시대상.set_index('종목코드')['종목명'].to_dict()

        return li_대상종목, dic_코드2종목명_대상종목

    def get_지지저항(self):
        """ 감시대상 종목의 지지저항 정보 읽어와서 종목별 지지저항 list (dict) 리턴 """
        # 지지저항 파일 읽어오기
        df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{self.s_전일}.pkl'))

        # 종목코드 골라내기
        dic_지지저항 = dict()
        for s_종목코드 in df_지지저항['종목코드'].unique():
            dic_지지저항[s_종목코드] = list(df_지지저항[df_지지저항['종목코드'] == s_종목코드]['고가'].values)

        return dic_지지저항, df_지지저항

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

    def update_주문정보파일(self, dic_주문정보, dic_신호상세):
        """ 매수매도 주문정보 수집 후 csv 파일로 저장 """
        # 변수 지정
        df_3분봉 = dic_주문정보['df_3분봉'].sort_values(['일자', '시간'])
        s_주문구분 = dic_주문정보['s_주문구분']
        n_현재가 = dic_주문정보['n_현재가']
        n_주문단가 = dic_주문정보['n_주문단가']
        n_주문수량 = dic_주문정보['n_주문수량']
        li_지지저항 = dic_주문정보['li_지지저항'] if s_주문구분 == '매수' else list()
        li_매도신호 = dic_주문정보['li_매도신호'] if s_주문구분 == '매도' else list()
        li_매도신호종류 = dic_주문정보['li_매도신호종류'] if s_주문구분 == '매도' else list()
        # n_지지선 = dic_신호상세['n_지지선'] if s_주문구분 == '매도' else None
        # n_저항선 = dic_신호상세['n_저항선'] if s_주문구분 == '매도' else None

        # df 생성
        df_주문정보_종목 = df_3분봉.loc[:, ['일자', '종목코드', '종목명']].copy()[-1:]
        df_주문정보_종목['시간'] = pd.Timestamp('now').strftime('%H:%M:%S')
        df_주문정보_종목['현재가'] = n_현재가
        df_주문정보_종목['주문구분'] = s_주문구분
        df_주문정보_종목['주문단가'] = n_주문단가
        df_주문정보_종목['주문수량'] = n_주문수량
        df_주문정보_종목['주문금액'] = n_주문단가 * n_주문수량
        df_주문정보_종목['지지선'] = dic_신호상세['n_지지선']
        df_주문정보_종목['저항선'] = dic_신호상세['n_저항선']
        if s_주문구분 == '매도':
            for idx in range(len(li_매도신호)):
                df_주문정보_종목[f'매도{idx + 1}{li_매도신호종류[idx]}'] = li_매도신호[idx]

        # 신호상세 추가
        df_주문정보_종목 = df_주문정보_종목.reset_index()
        df_신호상세 = pd.DataFrame([dic_신호상세.values()], columns=dic_신호상세.keys())
        df_주문정보_종목 = pd.concat([df_주문정보_종목, df_신호상세], axis=1)

        # df 업데이트
        s_파일명 = '매수매도주문'
        try:
            df_주문정보 = pd.read_pickle(os.path.join(self.folder_주문정보, f'{s_파일명}_{self.s_오늘}.pkl'))
        except FileNotFoundError:
            df_주문정보 = pd.DataFrame()
        df_주문정보 = pd.concat([df_주문정보, df_주문정보_종목], axis=0).drop_duplicates()
        df_주문정보.to_pickle(os.path.join(self.folder_주문정보, f'{s_파일명}_{self.s_오늘}.pkl'))
        df_주문정보.to_csv(os.path.join(self.folder_주문정보, f'{s_파일명}_{self.s_오늘}.csv'), index=False, encoding='cp949')

    def update_매수탐색파일(self, dic_탐색정보, dic_신호상세):
        """ 매수탐색 결과 수집 후 csv 파일로 저장 """
        # 변수 지정
        df_3분봉 = dic_탐색정보['df_3분봉']
        li_매수신호 = dic_탐색정보['li_매수신호']
        li_신호종류 = dic_탐색정보['li_신호종류']
        n_현재가 = dic_탐색정보['n_현재가']

        # df 생성
        df_매수탐색_종목 = df_3분봉.loc[:, ['일자', '종목코드', '종목명']].copy()[-1:]
        df_매수탐색_종목['시간'] = pd.Timestamp('now').strftime('%H:%M:%S')
        df_매수탐색_종목['매수신호'] = sum(li_매수신호) == len(li_매수신호)
        for idx in range(len(li_매수신호)):
            df_매수탐색_종목[f'매수{idx + 1}{li_신호종류[idx]}'] = li_매수신호[idx]
        df_매수탐색_종목['현재가'] = n_현재가

        # 신호상세 추가
        df_매수탐색_종목 = df_매수탐색_종목.reset_index()
        df_신호상세 = pd.DataFrame([dic_신호상세.values()], columns=dic_신호상세.keys())
        df_매수탐색_종목 = pd.concat([df_매수탐색_종목, df_신호상세], axis=1)

        # df 업데이트
        s_파일명 = '매수신호탐색'
        try:
            df_매수탐색 = pd.read_pickle(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.pkl'))
        except FileNotFoundError:
            df_매수탐색 = pd.DataFrame()
        df_매수탐색 = pd.concat([df_매수탐색, df_매수탐색_종목], axis=0).drop_duplicates()
        df_매수탐색.to_pickle(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.pkl'))
        df_매수탐색.to_csv(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.csv'), index=False, encoding='cp949')

    def update_매도탐색파일(self, dic_탐색정보, dic_신호상세):
        """ 매도탐색 결과 수집 후 csv 파일로 저장 """
        # 변수 지정
        s_종목코드 = dic_탐색정보['s_종목코드']
        s_종목명 = dic_탐색정보['s_종목명']
        n_매수단가 = dic_탐색정보['n_매수단가']
        n_현재가 = dic_탐색정보['n_현재가']
        n_수익률 = dic_탐색정보['n_수익률']
        li_매도신호 = dic_탐색정보['li_매도신호']
        n_주문단가 = dic_탐색정보['n_주문단가']
        n_주문수량 = dic_탐색정보['n_주문수량']
        n_지지선 = dic_신호상세['n_지지선']
        n_저항선 = dic_신호상세['n_저항선']

        # df 생성
        df_매도탐색_종목 = pd.DataFrame()
        df_매도탐색_종목['일자'] = [self.s_오늘]
        df_매도탐색_종목['종목코드'] = s_종목코드
        df_매도탐색_종목['종목명'] = s_종목명
        df_매도탐색_종목['시간'] = pd.Timestamp('now').strftime('%H:%M:%S')
        df_매도탐색_종목['매수단가'] = n_매수단가
        df_매도탐색_종목['현재가'] = n_현재가
        df_매도탐색_종목['수익률'] = n_수익률
        df_매도탐색_종목['지지선'] = n_지지선
        df_매도탐색_종목['저항선'] = n_저항선
        df_매도탐색_종목['매도신호'] = sum(li_매도신호) > 0
        for i in range(len(li_매도신호)):
            df_매도탐색_종목[f'매도{i + 1}'] = li_매도신호[i]
        df_매도탐색_종목['주문단가'] = n_주문단가
        df_매도탐색_종목['주문수량'] = n_주문수량
        df_매도탐색_종목['주문금액'] = n_주문단가 * n_주문수량 if n_주문단가 is not None and n_주문수량 is not None else None

        # 신호상세 추가
        df_매도탐색_종목 = df_매도탐색_종목.reset_index()
        df_신호상세 = pd.DataFrame([dic_신호상세.values()], columns=dic_신호상세.keys())
        df_매도탐색_종목 = pd.concat([df_매도탐색_종목, df_신호상세], axis=1)

        # df 업데이트
        s_파일명 = '매도신호탐색'
        try:
            df_매도탐색 = pd.read_pickle(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.pkl'))
        except FileNotFoundError:
            df_매도탐색 = pd.DataFrame()
        df_매도탐색 = pd.concat([df_매도탐색, df_매도탐색_종목], axis=0).drop_duplicates()
        df_매도탐색.to_pickle(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.pkl'))
        df_매도탐색.to_csv(os.path.join(self.folder_탐색결과, f'{s_파일명}_{self.s_오늘}.csv'), index=False, encoding='cp949')

    def convert_이력파일(self):
        """ pkl 형식으로 저장된 이력 파일을 csv 형식으로 변환 후 저장 """
        # 기준정보 정의
        li_li_폴더파일 = [[self.folder_주문정보, f'매수매도주문_{self.s_오늘}'],
                      [self.folder_탐색결과, f'매수신호탐색_{self.s_오늘}'],
                      [self.folder_탐색결과, f'매도신호탐색_{self.s_오늘}']]

        # 파일 변환 후 저장
        for li_폴더파일 in li_li_폴더파일:
            path_pkl = os.path.join(li_폴더파일[0], f'{li_폴더파일[1]}.pkl')
            path_csv = os.path.join(li_폴더파일[0], f'{li_폴더파일[1]}.csv')
            if not os.path.isfile(path_csv):
                df_파일 = pd.read_pickle(path_pkl)
                df_파일.to_csv(path_csv, index=False, encoding='cp949')

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

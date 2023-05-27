import os
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import numpy as np
import pandas as pd
import json
import time
import sqlite3
import pickle

# [ 키움 API 통신 순서 ] (TR 주고, event 받음)
# 1) SetInputValue : 입력 데이터 설정 @spTrader.py
# 2) CommRqData : 기능 요청 (TR을 서버로 전송) => 초당 5회 제한 (조회 제한 시 에러코드 -200 반환) @spTrader.py
# 3) 이벤트 루프로 대기
# 4) OnReceiveTRData : 이벤트 호출 @API_kiwoom.py
# 5) GetCommData : 데이터 획득 @API_kiwoom.py
# ※ 키움 API를 호출하려면 QAwWidget 클래스 필요 *


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
class KiwoomAPI(QAxWidget):
    def __init__(self):
        # QAxWidget init 설정 상속
        super().__init__()

        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 폴더 정의
        folder_work = dic_config['folder_work']
        self.folder_run = os.path.join(folder_work, 'run')
        os.makedirs(self.folder_run, exist_ok=True)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.n_딜레이 = 0.2
        # self.path_buysell = os.path.join(self.folder_log, f'DB_buysell_history_{s_date}.db')
        # self.dic_data = {}

        # 키움 API 연결
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

        # [ 시그널 슬랏 설정 ]
        # 로그인 이벤트
        self.OnEventConnect.connect(self._on_event_connect)
        # 체결 잔고 이벤트
        # self.OnReceiveChejanData.connect(self._receive_chejan_data)
        # 조건식 종목 조회 이벤트
        self.OnReceiveConditionVer.connect(self._on_receive_condition_ver)
        self.OnReceiveTrCondition.connect(self._on_receive_tr_condition)
        # 실시간 데이터 처리 이벤트
        # self.OnReceiveRealData.connect(self._receive_real_data)
        # TR 처리 이벤트
        # self.OnReceiveTrData.connect(self._receive_tr_data)

    ###################################################################################################################

    # [ 접속 및 로그인 모듈 ]
    def comm_connect(self):
        """ 통신 연결 및 로그인 \n
        OnEventConnect 발생 대기 """
        # 접속 요청
        self.dynamicCall("CommConnect()")

        # 이벤트 발생 대기 (OnEventConnect)
        self.eventloop_로그인 = QEventLoop()
        self.eventloop_로그인.exec_()

    # [ 리턴값 반환 모듈 (get) ]
    def get_접속서버(self):
        """ 접속 서버 확인 후 결과 리턴 """
        # 서버 정보 요청 (리턴값: ['1']모의투자서버, [나머지]실서버)
        s_ret = self.dynamicCall('KOA_Functions(QString, QString)', 'GetServerGubun', '')

        # 리턴값 변환
        s_접속서버 = '모의서버' if s_ret == '1' else '실서버'

        return s_접속서버

    def get_접속상태(self):
        """ 접속 상태 확인 후 결과 리턴 """
        # 접속상태 요청 (리턴값: [1]연결, [0]연결안됨)
        n_ret = self.dynamicCall('GetConnectState()')

        # 리턴값 변환
        s_접속상태 = '연결' if n_ret == 1 else '연결안됨'

        return s_접속상태

    def get_로그인정보(self, s_코드):
        """ 로그인 정보 요청 \n
        # tag에 따라 리턴값 달라짐 \n
            '계좌수', '계좌목록'(;로 구분), '아이디', '사용자명', '키보드보안해지여부', '방화벽설정여부' """
        # 조회 요청 코드 정의
        dic_조회코드 = {'계좌수': 'ACCOUNT_CNT', '계좌목록': 'ACCNO', '아이디': 'USER_ID', '사용자명': 'USER_NAME',
                    '키보드보안해지여부': 'KEY_BSECGB', '방화벽설정여부': 'FIREW_SECGB'}
        # 입력된 tag에 따른 로그인 정보 요청
        s_로그인정보 = self.dynamicCall("GetLoginInfo(Qstring)", dic_조회코드[s_코드])

        return s_로그인정보

    def get_전체종목코드(self, s_주식시장):
        """ 입력한 주식시장 값에 따른 전체 종목코드 list 리턴 \n
        # 주식시장 값 : '코스피', '코스닥', 'ELW', 'ETF', 'KONEX', '뮤추얼펀드', '신주인수권', '리츠', '하이얼펀드', 'K-OTC' """
        # 주식시장별 조회 코드 정의
        dic_조회코드 = {'코스피': '0', '코스닥': '10',
                    'ELW': '3', 'ETF': '8', 'KONEX': '50', '뮤추얼펀드': '4', '신주인수권': '5', '리츠': '6',
                    '하이얼펀드': '9', 'K-OTC': '30'}

        # 주식시장에 속해 있는 전체 종목코드 요청
        s_전체종목코드 = self.dynamicCall('GetCodeListByMarket(QString)', dic_조회코드[s_주식시장])

        # list 형식으로 변환
        li_전체종목코드 = s_전체종목코드.split(';')[:-1]

        return li_전체종목코드

    def get_코드별종목명(self, s_종목코드):
        """ 입력한 종목코드 값을 조회하여 종목명 리턴 """
        # 종목코드별 종목명 조회 요청
        s_종목명 = self.dynamicCall('GetMasterCodeName(QString)', s_종목코드)

        return s_종목명

    def get_코드별상장주식수(self, s_종목코드):
        """ 입력한 종목코드 값을 조회하여 상장 주식 수 리턴 """
        # 종목코드별 상장 주식 수 조회 요청
        s_상장주식수 = self.dynamicCall('KOA_Functions(QString, QString)', 'GetMasterListedStockCntEx', s_종목코드)
        n_상장주식수 = int(s_상장주식수)

        return n_상장주식수

    def get_코드별감리구분(self, s_종목코드):
        """ 입력한 종목코드 값을 조회하여 감리구분 리턴 """
        # 종목코드별 감리 구분 조회 요청
        s_감리구분 = self.dynamicCall('GetMasterConstruction(QString)', s_종목코드)

        return s_감리구분

    def get_코드별상장일(self, s_종목코드):
        """ 입력한 종목코드 값을 조회하여 상장일 리턴 """
        # 종목코드별 상장일 조회 요청
        s_상장일 = self.dynamicCall('GetMasterListedStockDate(QString)', s_종목코드)

        return s_상장일

    def get_코드별기준가(self, s_종목코드):
        """ 입력한 종목코드 값을 조회하여 당일 기준가 리턴 """
        # 종목코드별 기준가 조회 요청
        s_ret = self.dynamicCall('GetMasterLastPrice(QString)', s_종목코드)
        n_기준가 = int(s_ret)

        return n_기준가

    def get_종목정보(self, s_종목코드):
        """ 입력한 종목코드 값에 따른 종목정보 조회하여 dict 리턴 (key: 대분류, 중분류, 업종구분) """
        # 종목코드별 종목정보 조회 요청
        s_종목정보 = self.dynamicCall('KOA_Functions(QString, QString)', 'GetMasterStockInfo', s_종목코드)

        # dic 형태로 변환
        li_종목정보 = s_종목정보.split(';')[:-1]
        li_li_종목정보 = [s_정보.split('|') for s_정보 in li_종목정보]
        dic_종목정보 = dict(li_li_종목정보)
        dic_종목정보 = {key_old: dic_종목정보[key_new] for key_old, key_new
                    in [['대분류', '시장구분0'], ['중분류', '시장구분1'], ['업종구분', '업종구분']]}

        return dic_종목정보

    def get_투자유의종목(self, s_종목코드):
        """ 입력한 종목코드 값에 따른 투자유의종목 여부 리턴 """
        # 리턴값 코드 정의
        dic_리턴코드 = {'0': '해당없음', '2': '정리매매', '3': '단기과열', '4': '투자위험', '5': '투자경고'}

        # 종목코드별 투자유의종목 여부 조회 요청
        s_ret = self.dynamicCall('KOA_Functions(QString, QString)', 'IsOrderWarningStock', s_종목코드)

        # 리턴코드 처리
        s_투자유의종목 = dic_리턴코드[s_ret]

        return s_투자유의종목

    # [ 조건검색 관련 모듈 ]
    def get_조건검색_전체(self):
        """ 서버에 등록되어 있는 전체 조건검색 조회하여 검색식명 별 종목코드 list 조회할 수 있는 dict 리턴 """
        # 조건검색 연결
        n_ret = self.dynamicCall('GetConditionLoad()')
        self.s_조건검색_요청결과 = '성공' if n_ret == 1 else '실패'

        # 조건검색 조회용 이벤트 대기 (OnReceiveConditionVer 발생)
        self.eventloop_조건검색 = QEventLoop()
        self.eventloop_조건검색.exec_()

        # 조건검색 데이터 조회
        df_조건검색 = self.df_조건검색.copy()
        dic_조건검색_검색식명2번호 = df_조건검색.set_index('검색식명').to_dict()['검색식번호']

        # 검색식명 기준 종목코드 가져오기
        li_li_검색종목 = []
        for s_검색식명 in df_조건검색['검색식명'].values:
            s_화면번호 = '9000'
            n_검색식번호 = int(dic_조건검색_검색식명2번호[s_검색식명])
            n_실시간옵션 = 0  # [0]조건검색만, [1]조건검색+실시간
            n_ret = self.dynamicCall('SendCondition(QString, QString, int, int)',
                                     s_화면번호, s_검색식명, n_검색식번호, n_실시간옵션)
            self.s_조건검색_종목조회성공여부 = '성공' if n_ret == 1 else '실패'

            # 조건검색 조회용 이벤트 대기 (OnReceiveTrCondition 발생)
            self.eventloop_조건검색 = QEventLoop()
            self.eventloop_조건검색.exec_()

            # 결과 데이터 정리
            s_검색식번호 = str(self.n_조건검색_검색식번호)
            s_검색식명 = self.s_조건검색_검색식명
            li_검색종목 = self.li_검색종목

            # df_조건검색 추가
            li_li_검색종목.append(li_검색종목)

        # df 추가
        df_조건검색['검색종목'] = li_li_검색종목

        # 리턴값 생성
        dic_조건검색_검색식명2종목코드 = df_조건검색.set_index('검색식명').to_dict()['검색종목']

        return dic_조건검색_검색식명2종목코드

    # [ TR 요청 모듈 (tr_get) ]


    # [ 실시간 요청 모듈 (real_get) ]





    def tget_ohlcv_day(self, s_code, s_date):
        ''' 종목코드별 일봉 데이터 조회하여 df 리턴 '''
        # TR 요청 (일봉)
        self.set_input_value('종목코드', s_code)
        self.set_input_value('기준일자', s_date)
        self.comm_rq_data('opt10081_req', 'opt10081', 0, '2001')

        # TR 결과 (일봉)
        ret = self.df_ohlcv_day
        return ret

    def tget_ohlcv_day_long(self, s_code, s_date=None):
        ''' 종목코드별 일봉 데이터 조회하여 df 리턴 '''
        # TR 요청 (일봉)
        self.set_input_value('종목코드', s_code)
        # self.set_input_value('기준일자', s_date)
        self.comm_rq_data('opt10081_req', 'opt10081', 0, '2001')

        # TR 결과 (일봉)
        ret = self.df_ohlcv_day
        return ret

    def tget_ohlcv_min(self, s_code, n_bong):
        ''' 종목코드별 분봉 데이터 조회하여 df 리턴 '''
        # TR 요청 (분봉)
        self.set_input_value('종목코드', s_code)
        self.set_input_value('틱범위', str(n_bong))  # [1]1분, [3]3분, [5]5분, [10]10분, [15]15분, [30]30분, [45]45분, [60]60분
        self.comm_rq_data('opt10080_req', 'opt10080', 0, '2002')

        # TR 결과 (분봉)
        ret = self.df_ohlcv_min
        return ret

    def tget_ohlcv_min_long(self, s_code, n_bong, s_date):
        ''' 종목코드별 분봉 데이터 조회하여 df 리턴 (요청한 날짜까지 장기간 조회) '''
        # TR 요청 (분봉)
        self.set_input_value('종목코드', s_code)
        self.set_input_value('틱범위', str(n_bong))  # [1]1분, [3]3분, [5]5분, [10]10분, [15]15분, [30]30분, [45]45분, [60]60분
        self.comm_rq_data('opt10080_req', 'opt10080', 0, '2002')

        # TR 결과 (분봉)
        df_ret = self.df_ohlcv_min

        # 기간 포함 확인용 데이터 생성
        df_ret['n_date'] = df_ret['date'].apply(lambda x: int(x.replace('-', '')))
        ary_date = df_ret['n_date'].unique()

        # 데이터 없으면 비어있는 df 리턴
        if len(df_ret) == 0:
            return df_ret

        # 데이터가 900개 미만이면 df 리턴
        if len(df_ret) < 900:
            return df_ret

        # 조회 일자 미포함 시 추가 조회 진행
        li_df_ret = [df_ret]
        while ary_date.min() >= int(s_date):
            time.sleep(0.2)
            self.set_input_value('종목코드', s_code)
            self.set_input_value('틱범위', str(n_bong))
            self.comm_rq_data('opt10080_req', 'opt10080', 2, '2002')

            df_ret = self.df_ohlcv_min
            df_ret['n_date'] = df_ret['date'].apply(lambda x: int(x.replace('-', '')))
            ary_date = df_ret['n_date'].unique()
            li_df_ret.append(df_ret)

        # df 정리
        df_result = pd.concat(li_df_ret, axis=0).drop_duplicates()
        df_result = df_result[df_result['n_date'] >= int(s_date)].sort_values(['date', 'time'], ascending=False)
        del df_result['n_date']

        return df_result

    def tget_ohlcv_day_allcodes(self, s_market):
        ''' 시장 내 전체 종목코드의 현재가 조회하여 df 리턴 '''
        # dic 정의
        dic_market = {'kospi': '0', 'kosdaq': '1'}
        dic_section = {'kospi': '001', 'kosdaq': '101'}

        # TR 요청 (업종별 전체종목 현재가)
        self.flag_tr_rq_1st = 1
        self.set_input_value('시장구분', dic_market[s_market])
        self.set_input_value('업종코드', dic_section[s_market])
        self.comm_rq_data('opt20002_req', 'opt20002', 0, '2001')

        # 데이터가 남아 있으면 이어서 조회
        while self.remained_data:
            time.sleep(0.2)
            self.flag_tr_rq_1st = 0
            self.set_input_value('시장구분', dic_market[s_market])
            self.set_input_value('업종코드', dic_section[s_market])
            self.comm_rq_data('opt20002_req', 'opt20002', 2, '2001')

        # TR 결과 (업종별 전체종목 현재가)
        ret = self.df_ohlcv_day_allcodes
        return ret

    def tget_d2_deposit(self, s_account_number):
        ''' 입력받은 계좌의 D+2일 추정 예수금 조회하여 int 리턴'''
        # TR 요청
        self.set_input_value('계좌번호', s_account_number)
        self.set_input_value('비밀번호입력매체구분', '00')
        self.set_input_value('조회구분', '3')  # 3:추정조회, 2:일반조회
        self.comm_rq_data('opw00001_req', 'opw00001', 0, '2000')

        # 예수금 데이터 표기
        ret = self.n_d2_deposit
        return ret

    def tget_balance(self, s_account_number):
        ''' 계좌 잔고 요청하여 dic 리턴 (전체는 total, 종목은 종목코드)'''
        # TR 요청
        self.set_input_value('계좌번호', s_account_number)
        self.comm_rq_data('opw00018_req', 'opw00018', 0, '2000')

        # 계좌 잔고 표기
        ret = self.dic_balance
        return ret

    def tget_trade_history(self, s_date, s_account_number):
        ''' 계좌별 주문체결 현황 요청하여 df 리턴 '''
        # TR 요청
        self.flag_tr_rq_1st = 1
        self.set_input_value('주문일자', s_date)  # YYYYMMDD
        self.set_input_value('계좌번호', s_account_number)
        self.set_input_value('비밀번호입력매체구분', '00')
        self.set_input_value('주식채권구분', '0')  # [0]전체, [1]주식, [2]채권
        self.set_input_value('시장구분', '0')  # [0]전체, [1]장내, [2]코스닥, [3]OTCBB, [4]ECN
        self.set_input_value('매도수구분', '0')  # [0]전체, [1]매도, [2]매수
        self.set_input_value('조회구분', '0')  # [0]전체, [1]체결
        self.comm_rq_data('opw00009_req', 'opw00009', 0, '2001')

        # 데이터가 남아 있으면 이어서 조회
        while self.remained_data == True:
            time.sleep(0.2)
            self.flag_tr_rq_1st = 0
            self.set_input_value('주문일자', s_date)  # YYYYMMDD
            self.set_input_value('계좌번호', s_account_number)
            self.set_input_value('비밀번호입력매체구분', '00')
            self.set_input_value('주식채권구분', '0')  # [0]전체, [1]주식, [2]채권
            self.set_input_value('시장구분', '0')  # [0]전체, [1]장내, [2]코스닥, [3]OTCBB, [4]ECN
            self.set_input_value('매도수구분', '0')  # [0]전체, [1]매도, [2]매수
            self.set_input_value('조회구분', '0')  # [0]전체, [1]체결
            self.comm_rq_data('opw00009_req', 'opw00009', 2, '2001')

        # 거래이력 표기
        ret = self.df_trade_history
        return ret

    def tset_real_reg(self, s_code, s_type):
        ''' 실시간 데이터 감시 요청하는 함수 (장중일 때만 요청 전송, 아닐 때는 현재가 1원 리턴) '''
        # 실시간 등록타입 설정 ('0'이면 갱신, '1'이면 추가)
        if s_type in ['0', 'w', 'new', 'reset']:
            s_reg_type = '0'
        elif s_type in ['1', 'a', 'append', 'add']:
            s_reg_type = '1'
        else:
            s_reg_type = '0'

        # dic 초기화
        if s_reg_type == '0':
            self.dic_contract_real = {}
            self.dic_hogajan_real = {}

        # 장중에만 동작
        dt_now = pd.Timestamp('now')
        s_weekday_now = dt_now.strftime('%a')

        if ((s_weekday_now in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'])
                and (dt_now >= pd.Timestamp('09:00:00')) and (dt_now <= pd.Timestamp('15:30:00'))):
            # 실시간 데이터 감시 요청 (장중일 시)
            self.set_real_reg(s_screen_no='0001', s_code=s_code, s_fid='9001;10;21;121;125', s_reg_type=s_reg_type)

    def tset_real_remove(self, s_code):
        ''' 실시간 데이터 감시 종료하는 함수 '''
        self.set_real_remove(s_screen_no='0001', s_code=s_code)

    def tget_price_real(self, s_code):
        ''' 실시간 체결정보(누적)에서 마지막 체결가 가져와서 int로 리턴 '''
        if s_code in self.dic_contract_real.keys():
            # 데이터 가져오기
            li_contract = self.dic_contract_real[s_code]
            n_price_real = li_contract[-1][2]
        else:
            # 데이터 없을 시 1 리턴
            n_price_real = 1

        return n_price_real

    def tget_contract_real(self, s_code):
        ''' 실시간 체결정보(누적)을 가져와서 df로 리턴 '''
        # 데이터 가져오기
        li_contract = self.dic_contract_real[s_code]

        # df 형식으로 정리
        df = pd.DataFrame(li_contract, columns=['code', 'time', 'price', 'volume', 'buysell', 'cash'])
        df['time'] = df['time'].astype('datetime64')
        return df

    def tget_hogajan_real(self, s_code):
        ''' 실시간 호가잔량(갱신)을 가져와서 dic으로 리턴 '''
        # 데이터 가져오기
        dic = self.dic_hogajan_real[s_code]
        return dic

    ### 주문, 체결 관련
    def send_order(self, s_rqname, s_screen_no, s_acc_no, n_order_type, s_code, n_quantity, n_price, s_hoga,
                   s_order_no):
        ''' 주문 요청 (OnReceiveChejanData 이벤트 자동 호출) '''
        ''' [rqname]사용자, [screen_no]화면번호, [acc_no]계좌번호 10자리,
            [order_type]주문유형 1:신규매수 2:신규매도 3:매수취소 4:매도취소 5:매수정정 6:매도정정,
            [code]종목코드, [quantity]주문수량, [price]주문가격, [hoga]거래구분(호가구분) 00:지정가, 03:시장가,
            [order_no]원주문번호(신규주문은 공백) '''
        ret = self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                               [s_rqname, s_screen_no, s_acc_no, n_order_type, s_code, n_quantity, n_price, s_hoga,
                                s_order_no])
        # print(f'주문전송 요청 리턴값: {ret}')

        ''' 체결잔고 데이터는 eventloop 사용 안함 (실시간 데이터 처리) '''
        # self.eventloop_order = QEventLoop()
        # self.eventloop_order.exec_()

        ''' db파일에 저장 => self.path_buysell '''



    ### 실시간 데이터 처리
    def set_real_reg(self, s_screen_no, s_code, s_fid, s_reg_type):
        ''' 실시간 데이터 요청 (OnReceiveRealData 이벤트 자동 호출 '''
        ''' [screen_no]화면번호, [code]종목코드, [fid]FID 번호(';'로 구분),
            [reg_type] 0-이전등록 해지(replace), 1-이전등록 유지(append) '''
        self.dynamicCall('SetRealReg(QString, QString, QString, QString)', s_screen_no, s_code, s_fid, s_reg_type)

        ''' 실시간 데이터에서는 eventloop 사용 안함 (사용 시 이후 loop 종료시까지 대기) '''
        # self.eventloop_real = QEventLoop()
        # self.eventloop_real.exec_()

        ''' !! 호출 전 결과 dic 초기화 필요 !! '''
        ''' self.dic_contract_real  : 실시간 체결정보(누적)
            self.dic_hogajan_real   : 실시간 호가잔량(갱신) '''

    def set_real_remove(self, s_screen_no, s_code):
        ''' 실시간 데이터 종료 '''
        ''' [screen_no]화면번호 or 'ALL', [code]종목코드 or 'ALL' '''
        self.dynamicCall('SetRealRemove(QString, QString)', s_screen_no, s_code)

    ### TR 데이터 처리
    def set_input_value(self, s_id, s_value):
        ''' TR 데이터 요청항목 설정 '''
        '''[id]아이템명-'종목코드' 등, [value]입력값-'000660' 등 '''
        self.dynamicCall('SetInputValue(QString, QString)', s_id, s_value)

    def comm_rq_data(self, s_rqname, s_trcode, n_next, s_screen_no):
        ''' TR 데이터 요청 (OnReceiveTrData 이벤트 자동 호출) '''
        ''' [rqname]사용자, [trcode]조회하려는 TR 이름, [next]연속조회여부 (0:조회, 2:연속), [screen_no]화면번호(4자리)
            리턴값: [0]조회요청 정상, [-200]시세과부하, [-201]조회전문작성 에러 '''
        ret = self.dynamicCall('CommRqData(QString, QString, int, QString', s_rqname, s_trcode, n_next, s_screen_no)
        # print(f'TR 조회 요청 리턴값: {ret}')

        self.eventloop_tr = QEventLoop()
        self.eventloop_tr.exec_()

        ''' self.df_ohlcv_day           : 주식일봉차트조회 (opt10081)
            self.n_d2_deposit           : D+2일 예수금 (opw00001)
            self.dic_balance            : 계좌 평가잔고 조회 (opw00018)
            self.df_volume_jump         : 거래량 급등 조회 (opt10023)
            self.df_ohlcv_min           : 주식분봉차트조회 (opt10080)
            self.df_trade_history       : 계좌별 주문체결 현황 (opw00009)
            self.df_profit              : 일자별 종목 실현손익 (opt10073)
            self.df_ohlcv_day_allcodes  : 업종별주가요청-전체종목현재가 (opt20002)
            self.df_basic_inform        : 주식기본정보요청-액면가 (opt10001)
            self.df_contract            : 체결정보 요청 (opt10003)'''









    ###################################################################################################################

    def _on_event_connect(self, n_에러코드):
        """ 통신 연결 결과 출력 (OnEventConnect 이벤트 연결) """
        # 리턴값 정의
        dic_에러코드 = {0: '성공', 100: '사용자 정보교환 실패', 101: '서버접속 실패', 102: '버전처리 실패'}

        # 접속 결과 생성
        self.s_접속결과 = dic_에러코드[n_에러코드]

        # 접속 결과 출력
        if n_에러코드 == 0:
            print('*** Connected at Kiwoom API server ***')
        else:
            print(f'*** Connection Fail - {self.s_접속결과} ***')

        self.eventloop_로그인.exit()

    def _on_receive_condition_ver(self, n_리턴코드, s_메세지):
        """ 조건검색 목록 받아와서 해당 조건검색 결과 조회 요청 (OnReceiveConditionVer 이벤트 연결)\n
        OnReceiveTrCondition 발생 대기"""
        # 호출결과 정의
        self.s_조건검색_검색식_호출결과 = '성공' if n_리턴코드 == 1 else '실패'

        # 조건검색식 목록 읽어오기
        s_조건검색_검색식목록 = self.dynamicCall('GetConditionNameList()')
        li_조건검색_종목 = s_조건검색_검색식목록.split(';')[:-1]
        li_li_조건검색_종목 = [s_검색종목.split('^') for s_검색종목 in li_조건검색_종목]
        df_조건검색 = pd.DataFrame(li_li_조건검색_종목, columns=['검색식번호', '검색식명'])
        self.df_조건검색 = df_조건검색.sort_values(['검색식번호'], ascending=True).reset_index(drop=True)

        # 이벤트 루프 종료
        self.eventloop_조건검색.exit()

    def _on_receive_tr_condition(self, s_화면번호, s_검색종목, s_검색식명, n_검색식번호, n_연속조회여부):
        """ 조건검색 결과 조회 후 체결/잔고 데이터 받아서 저장/출력 (OnReceiveTrCondition 이벤트 연결) """
        # tr 연속조회를 위한 딜레이 설정
        time.sleep(self.n_딜레이)

        # 리턴 데이터 정리
        self.n_조건검색_검색식번호 = n_검색식번호
        self.s_조건검색_검색식명 = s_검색식명
        self.li_검색종목 = s_검색종목.split(';')[:-1]

        # eventloop 종료
        try:
            self.eventloop_조건검색.exit()
        except AttributeError:
            pass


#######################################################################################################################
if __name__ == "__main__":
    app = QApplication(sys.argv)
    api = KiwoomAPI()
    api.comm_connect()

    # 테스트용 실행 코드
    # api.get_접속서버()
    # api.get_접속상태()
    # api.get_로그인정보('계좌수')
    # api.get_로그인정보('계좌목록')
    # api.get_로그인정보('아이디')
    # api.get_로그인정보('사용자명')
    # api.get_로그인정보('키보드보안해지여부')
    # api.get_로그인정보('방화벽설정여부')
    # api.get_전체종목코드('코스피')
    # api.get_전체종목코드('코스닥')
    # api.get_전체종목코드('ELW')
    # api.get_전체종목코드('ETF')
    # api.get_전체종목코드('KONEX')
    # api.get_전체종목코드('뮤추얼펀드')
    # api.get_전체종목코드('신주인수권')
    # api.get_전체종목코드('리츠')
    # api.get_전체종목코드('하이얼펀드')
    # api.get_전체종목코드('K-OTC')
    # api.get_코드별종목명('000020')
    # api.get_코드별상장주식수('000020')
    # api.get_코드별감리구분('000020')
    # api.get_코드별상장일('000020')
    # api.get_코드별기준가('000020')
    # api.get_종목정보('000020')
    # api.get_투자유의종목('000020')

    # api.get_조건검색('52주신고가')
    api.get_조건검색_전체()

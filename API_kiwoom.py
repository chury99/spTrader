import os
import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
import pandas as pd
import json
import time

# [ 키움 API 통신 순서 ] (TR 주고, event 받음)
# 1) SetInputValue : 입력 데이터 설정 @spTrader.py
# 2) CommRqData : 기능 요청 (TR을 서버로 전송) => 초당 5회 제한 (조회 제한 시 에러코드 -200 반환) @spTrader.py
# 3) 이벤트 루프로 대기
# 4) OnReceiveTRData : 이벤트 호출 @API_kiwoom.py
# 5) GetCommData : 데이터 획득 @API_kiwoom.py
# ※ 키움 API를 호출하려면 QAwWidget 클래스 필요 *


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class KiwoomAPI(QAxWidget):
    def __init__(self):
        # QAxWidget init 설정 상속
        super().__init__()

        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.n_딜레이 = 0.2

        # 폴더 정의
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        folder_체결잔고 = dic_폴더정보['이력|체결잔고']
        folder_메세지 = dic_폴더정보['이력|메세지']
        folder_실시간 = dic_폴더정보['이력|실시간']
        os.makedirs(folder_체결잔고, exist_ok=True)
        os.makedirs(folder_메세지, exist_ok=True)
        os.makedirs(folder_실시간, exist_ok=True)
        self.path_체결잔고 = os.path.join(folder_체결잔고, f'체결잔고_{self.s_오늘}.csv')
        self.path_메세지 = os.path.join(folder_메세지, f'메세지_{self.s_오늘}.txt')
        self.folder_실시간 = folder_실시간

        # 체결잔고 정보 불러오기
        try:
            self.df_체결잔고 = pd.read_csv(self.path_체결잔고, encoding='cp949')
        except FileNotFoundError:
            self.df_체결잔고 = pd.DataFrame()

        # 이벤트루프 생성
        self.eventloop_로그인 = QEventLoop()
        self.eventloop_주문조회 = QEventLoop()
        self.eventloop_조건검색 = QEventLoop()
        self.eventloop_tr조회 = QEventLoop()

        # 키움 API 연결
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

        # [ 시그널 슬랏 설정 ]
        # 로그인 이벤트
        self.OnEventConnect.connect(self.on_event_connect)
        # 체결 잔고 이벤트
        self.OnReceiveMsg.connect(self.on_receive_msg)
        self.OnReceiveChejanData.connect(self.on_receive_chejan_data)
        # 조건식 종목 조회 이벤트
        self.OnReceiveConditionVer.connect(self.on_receive_condition_ver)
        self.OnReceiveTrCondition.connect(self.on_receive_tr_condition)
        # 실시간 데이터 처리 이벤트
        self.OnReceiveRealData.connect(self.on_receive_real_data)
        # TR 처리 이벤트
        self.OnReceiveTrData.connect(self.on_receive_tr_data)

    ###################################################################################################################

    # ***** [ 접속 및 로그인 모듈 ] *****
    def comm_connect(self):
        """ 통신 연결 및 로그인 \n
        OnEventConnect 발생 대기 """
        # 접속 요청
        self.dynamicCall("CommConnect()")

        # 이벤트 발생 대기 (OnEventConnect)
        self.eventloop_로그인.exec_()

    # ***** [ 리턴값 반환 모듈 (get) ] *****
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

        # df 형태로 변환
        df_종목정보 = pd.DataFrame([dic_종목정보.values()], columns=list(dic_종목정보.keys()))

        return df_종목정보

    def get_투자유의종목(self, s_종목코드):
        """ 입력한 종목코드 값에 따른 투자유의종목 여부 리턴 """
        # 리턴값 코드 정의
        dic_리턴코드 = {'0': '해당없음', '2': '정리매매', '3': '단기과열', '4': '투자위험', '5': '투자경고'}

        # 종목코드별 투자유의종목 여부 조회 요청
        s_ret = self.dynamicCall('KOA_Functions(QString, QString)', 'IsOrderWarningStock', s_종목코드)

        # 리턴코드 처리
        s_투자유의종목 = dic_리턴코드[s_ret]

        return s_투자유의종목

    # ***** [ 주문/체결 관련 모듈 ] *****
    def send_주문(self, s_계좌번호, s_주문유형, s_종목코드, n_주문수량, n_주문단가, s_거래구분=None, s_원주문번호=None):
        """ 주문 요청 \n
        SendOrder 후 3가지 이벤트 발생 (OnReceiveTRData, OnReceiveMsg, OnReceiveChejanData) \n
        # 계좌번호 : 10자리 계좌번호 \n
        # 주문유형 : '매수', '매도', '매수취소', '매도취소', '매수정정', '매도정정' \n
        # 주문단가 : 시장가 주문 시 주문단가 0으로 입력(상한가 기준 주문수량 자동 계산), 취소 주문 시 주문단가 0으로 입력 \n
        # 거래구분 : '지정가', '시장가', '조건부지정가', '최유리지정가', '최우선지정가', '지정가IOC', '시장가IOC', '최유리IOC',
            '지정가FOK', '시장가FOK', '최유리FOK', '장전시간외종가', '시간외단일가매매', '장후시간외종가' (기본값: '지정가IOC') \n
        # 원주문번호 : 신규주문 시에는 공백 """
        # OnReceiveTRData (주문응답) - 주문발생시 첫번째 서버응답. 주문번호 취득 (주문번호가 없다면 주문거부 등 비정상주문)
        # OnReceiveMsg (주문메세지 수신) - 주문거부 사유를 포함한 서버메세지 수신
        # OnReceiveChejanData (주문접수/체결) - 주문 상태에 따른 실시간 수신 (주문접수, 주문체결, 잔고변경 각 단계별로 수신됨)

        dic_주문유형 = {'매수': 1, '매도': 2, '매수취소': 3, '매도취소': 4, '매수정정': 5, '매도정정': 6}
        dic_거래구분 = {'지정가': '00', '시장가': '03', '조건부지정가': '05', '최유리지정가': '06', '최우선지정가': '07',
                    '지정가IOC': '10', '시장가IOC': '13', '최유리IOC': '16', '지정가FOK': '20', '시장가FOK': '23',
                    '최유리FOK': '26', '장전시간외종가': '61', '시간외단일가매매': '62', '장후시간외종가': '81'}
        # 지정가IOC : 체결 안되는 건 자동 취소

        s_요청명 = '주문요청'
        s_화면번호 = '0001'
        n_주문유형 = dic_주문유형[s_주문유형]
        s_거래구분 = '10' if s_거래구분 is None else dic_거래구분[s_거래구분]
        s_원주문번호 = '' if s_원주문번호 is None else s_원주문번호

        n_리턴값 = self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                     [s_요청명, s_화면번호, s_계좌번호, n_주문유형, s_종목코드, n_주문수량, n_주문단가, s_거래구분, s_원주문번호])

        # 전송결과 정보 생성
        s_주문전송 = '전송성공' if n_리턴값 == 0 else '전송실패'
        self.s_주문_전송결과 = '주문횟수초과(초당5회)' if n_리턴값 == -308 else s_주문전송

        s_텍스트 = f'주문전송결과 | {self.s_주문_전송결과}'
        print(s_텍스트)
        with open(self.path_메세지, 'at') as file:
            file.write(f'{s_텍스트}\n')

        # 이벤트 루프 생성
        self.eventloop_주문조회.exec_()

    # ***** [ 조건검색 관련 모듈 ] *****
    def get_조건검색_전체(self):
        """ 서버에 등록되어 있는 전체 조건검색 조회하여 검색식명 별 종목코드 list 조회할 수 있는 dict 리턴 """
        # 조건검색 연결
        n_ret = self.dynamicCall('GetConditionLoad()')
        self.s_조건검색_요청결과 = '성공' if n_ret == 1 else '실패'

        # 조건검색 조회용 이벤트 대기 (OnReceiveConditionVer 발생)
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
        dic_조건검색식별종목코드 = df_조건검색.set_index('검색식명').to_dict()['검색종목']

        return dic_조건검색식별종목코드

    # ***** [ 실시간 데이터 요청 모듈 (real) ] *****
    def set_실시간_종목등록(self, s_종목코드, s_등록형태):
        """ 실시간 데이터 감시 종목에 등록 요청 (장중일 때만 요청 전송) \n
        # 등록형태 : '신규', '추가' 중 선택 """
        # 등록형태 정의
        dic_등록형태 = {'신규': '0', '추가': '1'}

        # FID 정의
        dic_fid = {'종목코드': 9001, '현재가': 10, '호가시간': 21, '매도호가총잔량': 121, '매수호가총잔량': 125,
                   '계좌번호': 9201, '주문번호': 9203, '주문상태': 913, '종목명': 302, '주문수량': 900,
                   '주문가격': 901, '미체결수량': 902, '체결누계금액': 903, '원주문번호': 904, '주문구분': 905, '매매구분': 906,
                   '매도수구분': 907, '주문체결시간': 908, '체결번호': 909, '체결가': 910, '체결량': 911,
                   '(최우선)매도호가': 27, '(최우선)매수호가': 28, '단위체결가': 914, '단위체결량': 915, '거부사유': 919,
                   '화면번호': 920, '신용구분': 917, '대출일': 916, '보유수량': 930, '매입단가': 931, '총매입가': 932,
                   '주문가능수량': 933, '당일순매수수량': 945, '매도매수구분': 946, '당일총매도손익': 950, '기준가': 307,
                   '손익율': 8019, '신용금액': 957, '신용이자': 958, '만기일': 918, '당일실현손익(유가)': 990,
                   '당일실현손익률(유가)': 991, '당일실현손익(신용)': 992, '당일실현손익률(신용)': 993, '파생상품거래단위': 397,
                   '상한가': 305, '하한가': 306}

        # dic 초기화 (신규일때만)
        # if s_등록형태 == '신규':
        #     self.dic_실시간_현재가 = dict()
        #     self.dic_실시간_체결 = dict()
        #     self.dic_실시간_호가잔량 = dict()

        # 요일, 시간 확인하여 장중일 때만 실시간 등록 요청
        dt_현재 = pd.Timestamp('now')
        s_요일 = dt_현재.strftime('%a')

        if ((s_요일 in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'])
                and (dt_현재 >= pd.Timestamp('09:00:00')) and (dt_현재 <= pd.Timestamp('15:30:00'))):
            # 실시간 데이터 감시 요청 (장중일 시)
            s_fid = f"{dic_fid['종목코드']};{dic_fid['현재가']};{dic_fid['호가시간']};" \
                    f"{dic_fid['매도호가총잔량']};{dic_fid['매수호가총잔량']}"
            self.set_real_reg('1001', s_종목코드, s_fid, dic_등록형태[s_등록형태])

    def set_실시간_종목해제(self, s_종목코드):
        """ 실시간 데이터 감시 종료 """
        self.set_real_remove('1001', s_종목코드)

    def get_실시간_현재가(self, s_종목코드):
        """ 실시간 현재가(갱신) 가져와서 int 리턴 (데이터 없으면 None 리턴) """
        try:
            n_실시간_현재가 = self.dic_실시간_현재가[s_종목코드] if s_종목코드 in self.dic_실시간_현재가.keys() else None
        except AttributeError:
            n_실시간_현재가 = '[error] 실시간 종목 등록 필요'

        return n_실시간_현재가

    def get_실시간_체결(self, s_종목코드):
        """ 실시간 체결정보(누적) 가져와서 df로 리턴 """
        try:
            if s_종목코드 not in self.dic_실시간_체결.keys():
                df_체결 = None
            else:
                # 데이터 가져오기
                li_체결 = self.dic_실시간_체결[s_종목코드]
                # df 형식으로 정리
                df_체결 = pd.DataFrame(li_체결, columns=['종목코드', '체결시간', '현재가', '거래량', '매수매도', '거래대금'])
                df_체결['체결시간'] = df_체결['체결시간'].astype('datetime64')
        except AttributeError:
            df_체결 = '[error] 실시간 종목 등록 필요'

        return df_체결

    def get_실시간_호가잔량(self, s_종목코드):
        """ 실시간 호가잔량(갱신)을 가져와서 dict 리턴 """
        try:
            # 데이터 가져오기
            if s_종목코드 not in self.dic_실시간_호가잔량.keys():
                df_호가잔량 = None
            else:
                # 데이터 가져오기
                dic_호가잔량 = self.dic_실시간_호가잔량[s_종목코드]
                # df 형식으로 정리
                df_호가잔량 = pd.DataFrame([dic_호가잔량.values()], columns=dic_호가잔량.keys())
        except AttributeError:
            df_호가잔량 = '[error] 실시간 종목 등록 필요'

        return df_호가잔량

    # ***** [ TR 요청 모듈 (tr) ] *****
    def get_tr_일봉조회(self, s_종목코드, s_기준일자_부터=None):
        """ 종목코드별 일봉 데이터 조회하여 df 리턴 - 600개 (OnReceiveTrData 이벤트 발생) \n
        # 기준일자 : 기준일자부터 현재까지 데이터를 600개 단위로 조회 (default: 오늘일자) """
        # 변수 정의
        s_기준일자_부터 = self.s_오늘 if s_기준일자_부터 is None else s_기준일자_부터

        # TR 요청
        self.set_input_value('종목코드', s_종목코드)
        self.comm_rq_data('주식일봉차트조회요청', 'opt10081', 0, '2001')

        # 결과 가져오기
        df_일봉 = self.df_일봉.copy()

        # 기준일자 확인
        s_최소일자 = df_일봉['일자'].min() if len(df_일봉) > 0 else '0'
        while s_기준일자_부터 < s_최소일자:
            # 예외처리 (데이터 600개 미만)
            if len(df_일봉) < 600:
                break

            # TR 추가 요청
            time.sleep(self.n_딜레이)
            self.set_input_value('종목코드', s_종목코드)
            self.comm_rq_data('주식일봉차트조회요청', 'opt10081', 2, '2001')

            # 결과 가져와서 합치기
            df_일봉_추가 = self.df_일봉.copy()
            df_일봉 = pd.concat([df_일봉, df_일봉_추가], axis=0)
            df_일봉 = df_일봉.sort_values('일자', ascending=False).drop_duplicates().reset_index(drop=True)

            # 기준일자 재확인
            s_최소일자 = df_일봉['일자'].min()

        # 데이터 정리
        df_일봉['종목코드'] = s_종목코드
        df_일봉['종목명'] = self.get_코드별종목명(s_종목코드)
        df_일봉['종가'] = df_일봉['현재가']
        df_일봉 = df_일봉.loc[:, ['일자', '종목코드', '종목명', '시가', '고가', '저가', '종가', '거래량', '거래대금(백만)']]

        # 데이터 타입 지정
        li_컬럼명_str = ['일자', '종목코드', '종목명']
        li_컬럼명_int = [s_컬럼명 for s_컬럼명 in df_일봉.columns if s_컬럼명 not in li_컬럼명_str]
        for s_컬럼명 in li_컬럼명_int:
            df_일봉[s_컬럼명] = df_일봉[s_컬럼명].astype(int)

        return df_일봉

    def get_tr_분봉조회(self, s_종목코드, n_틱범위=None, s_기준일자_부터=None):
        """ 종목코드별 분봉 데이터 조회하여 df 리턴 - 900개 (OnReceiveTrData 이벤트 발생) \n
        # 틱범위 : [1]1분, [3]3분, [5]5분, [10]10분, [15]15분, [30]30분, [45]45분, [60]60분 (default: 1) \n
        # 기준일자 : 기준일자부터 현재까지 데이터를 900개 단위로 조회 (default: 오늘일자) """
        # 변수 정의
        n_틱범위 = 1 if n_틱범위 is None else n_틱범위
        s_기준일자_부터 = self.s_오늘 if s_기준일자_부터 is None else s_기준일자_부터

        # TR 요청
        self.set_input_value('종목코드', s_종목코드)
        self.set_input_value('틱범위', str(n_틱범위))
        self.comm_rq_data('주식분봉차트조회요청', 'opt10080', 0, '2001')

        # 결과 가져오기
        df_분봉 = self.df_분봉.copy()

        # 기준일자 확인
        if len(df_분봉['체결시간'].apply(lambda x: x[:8]).unique()) > 1:
            s_최소일자 = df_분봉['체결시간'].apply(lambda x: x[:8]).unique()[:-1].min()
        else:
            s_최소일자 = '0'
        while s_기준일자_부터 < s_최소일자:
            # 예외처리 (데이터 900개 미만)
            if len(df_분봉) < 900:
                break

            # TR 추가 요청
            time.sleep(self.n_딜레이)
            self.set_input_value('종목코드', s_종목코드)
            self.set_input_value('틱범위', str(n_틱범위))
            self.comm_rq_data('주식분봉차트조회요청', 'opt10080', 2, '2001')

            # 결과 가져와서 합치기
            df_분봉_추가 = self.df_분봉.copy()
            df_분봉 = pd.concat([df_분봉, df_분봉_추가], axis=0)
            df_분봉 = df_분봉.sort_values('체결시간', ascending=False).drop_duplicates().reset_index(drop=True)

            # 기준일자 재확인
            s_최소일자 = df_분봉['체결시간'].apply(lambda x: x[:8]).unique()[:-1].min()

            # 예외처리 (추가 조회한 분봉의 데이터 900개 미만)
            if len(df_분봉_추가) < 900:
                break

        # 데이터 정리
        df_분봉['종목코드'] = s_종목코드
        df_분봉['종목명'] = self.get_코드별종목명(s_종목코드)
        df_분봉['종가'] = df_분봉['현재가']
        df_분봉['일자'] = df_분봉['체결시간'].apply(lambda x: x[:8])
        df_분봉['시간'] = df_분봉['체결시간'].apply(lambda x: f'{x[8:10]}:{x[10:12]}:{x[12:14]}')
        df_분봉 = df_분봉.loc[:, ['일자', '종목코드', '종목명', '시간', '시가', '고가', '저가', '종가', '거래량']]

        # 데이터 타입 지정
        li_컬럼명_str = ['일자', '종목코드', '종목명', '시간']
        li_컬럼명_int = [s_컬럼명 for s_컬럼명 in df_분봉.columns if s_컬럼명 not in li_컬럼명_str]
        for s_컬럼명 in li_컬럼명_int:
            df_분봉[s_컬럼명] = abs(df_분봉[s_컬럼명].astype(int))

        return df_분봉

    def get_tr_예수금(self, s_계좌번호):
        """ 계좌의 D+2일 추정 예수금 조회하여 int 리턴 (OnReceiveTrData 이벤트 발생) """
        # TR 요청
        self.set_input_value('계좌번호', s_계좌번호)
        self.set_input_value('비밀번호입력매체구분', '00')
        self.set_input_value('조회구분', '3')  # ['3']추정조회, ['2']일반조회
        self.comm_rq_data('예수금상세현황요청', 'opw00001', 0, '2001')

        # 결과 가져오기
        s_예수금 = self.s_예수금

        # 데이터 타입 지정
        n_예수금 = int(s_예수금)

        return n_예수금

    def get_tr_계좌잔고(self, s_계좌번호):
        """ 계좌 현황 조회하여 df_계좌잔고, df_종목별잔고 리턴 (OnReceiveTrData 이벤트 발생) """
        # TR 요청
        self.set_input_value('계좌번호', s_계좌번호)
        self.comm_rq_data('계좌평가잔고내역요청', 'opw00018', 0, '2001')

        # 결과 가져오기
        df_계좌잔고 = self.df_계좌잔고
        df_종목별잔고 = self.df_종목별잔고

        # 데이터 타입 지정
        for s_컬럼명 in df_계좌잔고.columns:
            df_계좌잔고[s_컬럼명] = df_계좌잔고[s_컬럼명].astype(float)
        df_계좌잔고['수익률'] = df_계좌잔고['수익률'] / 100 if self.s_접속서버 == '실서버' else df_계좌잔고['수익률']

        df_종목별잔고['종목코드'] = df_종목별잔고['종목코드'].apply(lambda x: x[-6:])
        li_컬럼명_str = ['종목코드', '종목명']
        li_컬럼명_int = [s_컬럼명 for s_컬럼명 in df_종목별잔고.columns if s_컬럼명 not in li_컬럼명_str]
        for s_컬럼명 in li_컬럼명_int:
            df_종목별잔고[s_컬럼명] = df_종목별잔고[s_컬럼명].astype(float)
        df_종목별잔고['수익률'] = df_종목별잔고['수익률'] / 100 if self.s_접속서버 == '실서버' else df_종목별잔고['수익률']

        return df_계좌잔고, df_종목별잔고

    def get_tr_거래량급증(self, s_시장구분=None, s_정렬구분=None, s_시간구분=None, s_거래량구분=None, s_시간=None,
                     s_종목조건=None, s_가격구분=None):
        """ 거래량 급증하는 종목 조회하여 df 리턴 (OnReceiveTrData 이벤트 발생) \n
        # 시장구분 : '전체', '코스피', '코스닥' (None 값은 '전체') \n
        # 정렬구분 : '급증량', '급증률' (None 값은 '급증률') \n
        # 시간구분 : '분', '전일' (None 값은 '분') \n
        # 거래량구분 : '5천주이상', '만주이상', '5만주이상', '10만주이상', '20만주이상', '30만주이상', '50만주이상', '백만주이상'
                    (None 값은 '10만주이상') \n
        # 시간 : 분단위 시간 입력 (None 값은 10분) \n
        # 종목조건 : '전체조회', '관리종목제외', '증100제외', '증100만보기', '증40만보기', '증30만보기', '증20만보기'
                    (None 값은 '관리종목제외') \n
        # 가격구분 : '전체조회', '5만원이상', '1만원이상', '5천원이상', '1천원이상', '10만원이상' (None 값은 '전체조회') """
        # 변수 정의
        dic_시장구분 = {'전체': '000', '코스피': '001', '코스닥': '101'}
        dic_정렬구분 = {'급증량': '1', '급증률': '2'}
        dic_시간구분 = {'분': '1', '전일': '2'}
        dic_거래량구분 = {'5천주이상': '5', '만주이상': '10', '5만주이상': '50',
                     '10만주이상': '100', '20만주이상': '200', '30만주이상': '300', '50만주이상': '500', '백만주이상': '1000'}
        dic_종목조건 = {'전체조회': '0', '관리종목제외': '1',
                    '증100제외': '5', '증100만보기': '6', '증40만보기': '7', '증30만보기': '8', '증20만보기': '9'}
        dic_가격구분 = {'전체조회': '0', '5만원이상': '2', '1만원이상': '5', '5천원이상': '6', '1천원이상': '8', '10만원이상': '9'}

        s_시장구분 = dic_시장구분['전체'] if s_시장구분 is None else dic_시장구분[s_시장구분]
        s_정렬구분 = dic_정렬구분['급증률'] if s_정렬구분 is None else dic_정렬구분[s_정렬구분]
        s_시간구분 = dic_시간구분['분'] if s_시간구분 is None else dic_시간구분[s_시간구분]
        s_거래량구분 = dic_거래량구분['10만주이상'] if s_거래량구분 is None else dic_거래량구분[s_거래량구분]
        s_시간 = '10' if s_시간 is None else s_시간
        s_종목조건 = dic_종목조건['관리종목제외'] if s_종목조건 is None else dic_종목조건[s_종목조건]
        s_가격구분 = dic_가격구분['전체조회'] if s_가격구분 is None else dic_가격구분[s_가격구분]

        # TR 요청
        self.set_input_value('시장구분', s_시장구분)
        self.set_input_value('정렬구분', s_정렬구분)
        self.set_input_value('시간구분', s_시간구분)
        self.set_input_value('거래량구분', s_거래량구분)
        self.set_input_value('시간', s_시간)
        self.set_input_value('종목조건', s_종목조건)
        self.set_input_value('가격구분', s_가격구분)
        self.comm_rq_data('거래량급증요청', 'opt10023', 0, '2001')

        # 결과 가져오기
        df_거래량급증 = self.df_거래량급증
        # [ 참고사항 ] 현재거래량: 일누적 거래량, 급증량: 이번틱 거래량, 급증률: 누적 거래량 대비 이번틱 거래량

        # 데이터 타입 지정
        li_컬럼명_abs = ['현재가']
        li_컬럼명_int = ['전일대비', '이전거래량', '현재거래량', '급증량']
        li_컬럼명_float = ['등락률', '급증률']

        for s_컬럼명 in li_컬럼명_abs:
            df_거래량급증[s_컬럼명] = abs(df_거래량급증[s_컬럼명].astype(int))
        for s_컬럼명 in li_컬럼명_int:
            df_거래량급증[s_컬럼명] = df_거래량급증[s_컬럼명].astype(int)
        for s_컬럼명 in li_컬럼명_float:
            df_거래량급증[s_컬럼명] = df_거래량급증[s_컬럼명].astype(float)

        df_거래량급증['전일대비기호'] = df_거래량급증['전일대비기호'].apply(lambda x:
                                                      '상승' if x == '2' else '하락' if x == '5' else '보합')

        return df_거래량급증

    def get_tr_종목별기본정보(self, s_종목코드):
        """ 종목별 기본정보 조회하여 df 리턴 (OnReceiveTrData 이벤트 발생) """
        # TR 요청
        self.set_input_value('종목코드', s_종목코드)
        self.comm_rq_data('주식기본정보요청', 'opt10001', 0, '2001')

        # 결과 가져오기
        df_기본정보 = self.df_기본정보

        # 데이터 타입 지정
        li_컬럼명_abs = ['연중최고', '연중최저', '250최고', '250최저', '시가', '고가', '저가', '현재가']
        li_컬럼명_int = ['액면가', '상장주식', '시가총액', '거래량', '전일대비']
        li_컬럼명_float = ['외인소진률', 'PER', '거래대비']

        for s_컬럼명 in li_컬럼명_abs:
            df_기본정보[s_컬럼명] = abs(df_기본정보[s_컬럼명].astype(int))
        for s_컬럼명 in li_컬럼명_int:
            df_기본정보[s_컬럼명] = df_기본정보[s_컬럼명].astype(int)
        for s_컬럼명 in li_컬럼명_float:
            try:
                df_기본정보[s_컬럼명] = df_기본정보[s_컬럼명].astype(float)
            except ValueError:
                df_기본정보[s_컬럼명] = float(0)

        return df_기본정보

    ###################################################################################################################

    # ***** [ 이벤트 연계 실행 모듈 ] *****
    def on_event_connect(self, n_에러코드):
        """ 통신 연결 결과 출력 (OnEventConnect 이벤트 연결) """
        # 리턴값 정의
        dic_에러코드 = {0: '성공', 100: '사용자 정보교환 실패', 101: '서버접속 실패', 102: '버전처리 실패'}

        # 접속 결과 생성
        try:
            self.s_접속결과 = dic_에러코드[n_에러코드]
        except KeyError:
            self.s_접속결과 = '알수 없는 에러'

        # 접속 서버 확인
        self.s_접속서버 = self.get_접속서버()

        # 접속 결과 출력
        if n_에러코드 == 0:
            s_텍스트 = f'*** Connected at Kiwoom API server [{self.s_접속서버}] ***'
        else:
            s_텍스트 = f'*** Connection Fail - {self.s_접속결과} ***'

        print(s_텍스트)
        with open(self.path_메세지, 'at') as file:
            file.write(f'{s_텍스트}\n')

        self.eventloop_로그인.exit()

    def on_receive_msg(self, s_화면번호, s_요청명, s_tr코드, s_메세지):
        """ 주문전송 서버 메시지 수신 (OnReceiveMsg 이벤트 연결, 이벤트루프 필요) """
        # 메세지 처리
        self.s_주문_메세지 = s_메세지
        s_텍스트 = f'메세지수신 | {s_메세지}'

        print(s_텍스트)
        with open(self.path_메세지, 'at') as file:
            file.write(f'{s_텍스트}\n')

        # 이벤트루프 종료
        self.eventloop_주문조회.exit()

    def on_receive_chejan_data(self, s_구분, n_항목수, sFID목록):
        """ 주문전송 후 주문접수, 체결통보, 잔고통보 정보 처리 (OnReceiveChejanData 이벤트 연결, FID 항목별 값 확인) """
        # 기준정보 정의
        dic_구분 = {'0': '주문/체결', '1': '주식잔고변경', '4': '파생잔고변경'}
        dic_FID목록 = {'계좌번호': 9201, '주문번호': 9203, '종목코드': 9001, '주문상태': 913, '종목명': 302, '주문수량': 900,
                     '주문가격': 901, '미체결수량': 902, '체결누계금액': 903, '원주문번호': 904, '주문구분': 905, '매매구분': 906,
                     '매도수구분': 907, '주문체결시간': 908, '체결번호': 909, '체결가': 910, '체결량': 911, '현재가': 10,
                     '(최우선)매도호가': 27, '(최우선)매수호가': 28, '단위체결가': 914, '단위체결량': 915, '거부사유': 919,
                     '화면번호': 920, '신용구분': 917, '대출일': 916, '보유수량': 930, '매입단가': 931, '총매입가': 932,
                     '주문가능수량': 933, '당일순매수수량': 945, '매도매수구분': 946, '당일총매도손익': 950, '기준가': 307,
                     '손익율': 8019, '신용금액': 957, '신용이자': 958, '만기일': 918, '당일실현손익(유가)': 990,
                     '당일실현손익률(유가)': 991, '당일실현손익(신용)': 992, '당일실현손익률(신용)': 993, '파생상품거래단위': 397,
                     '상한가': 305, '하한가': 306}

        # 주문/체결 정보 정리
        dic_체결잔고 = dict()
        dic_체결잔고['구분'] = [dic_구분[s_구분]]
        dic_체결잔고['시간'] = [pd.Timestamp('now').strftime('%H:%M:%S')]
        for s_항목 in dic_FID목록.keys():
            dic_체결잔고[s_항목] = [self._get_chejan_data(dic_FID목록[s_항목])]
        df_체결잔고 = pd.DataFrame(dic_체결잔고)

        # df 정리
        self.df_체결잔고 = pd.concat([self.df_체결잔고, df_체결잔고], axis=0)

        # df 저장
        self.df_체결잔고.to_csv(self.path_체결잔고, index=False, encoding='cp949')

    def _get_chejan_data(self, n_fid):
        """ FID별 체결/잔고 데이터 요청 (OnReceiveChejanData 내부에서 사용) """
        s_리턴값 = self.dynamicCall('GetChejanData(int)', n_fid)
        return s_리턴값

    def on_receive_condition_ver(self, n_리턴코드, s_메세지):
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

    # ***** [ 실시간 연계 실행 모듈 ] *****
    def on_receive_tr_condition(self, s_화면번호, s_검색종목, s_검색식명, n_검색식번호, n_연속조회여부):
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

    def set_real_reg(self, s_화면번호, s_종목코드, s_fid, s_등록형태):
        """ 실시간 데이터 요청 (OnReceiveRealData 이벤트 자동 호출) \n
        # fid : ;으로 구분 \n
        # 등록형태 : ['0']이전등록 해지(신규), ['1']이전등록 유지(추가) """

        self.dynamicCall('SetRealReg(QString, QString, QString, QString)', s_화면번호, s_종목코드, s_fid, s_등록형태)

    def set_real_remove(self, s_화면번호, s_종목코드):
        """ 실시간 데이터 종료 \n
        # [screen_no]화면번호 or 'ALL', [code]종목코드 or 'ALL' """
        self.dynamicCall('SetRealRemove(QString, QString)', s_화면번호, s_종목코드)

    def on_receive_real_data(self, s_종목코드, s_실시간타입, s_실시간데이터):
        """ 실시간 데이터 받아오기 (OnReceiveRealData 이벤트 연결) """
        # 현재가 관리 (갱신)
        # if s_실시간타입 == '주식시세':
        #     s_현재가 = self._get_comm_real_data(s_종목코드, 10)
        #     n_현재가 = abs(int(s_현재가))
        #     # dict 저장
        #     self.dic_실시간_현재가[s_종목코드] = n_현재가
        #
        #     # 화면 출력
        #     li_데이터 = [s_종목코드, n_현재가]
        #     s_텍스트 = f'실시간 | {s_실시간타입} | {li_데이터}'
        #     print(s_텍스트)
        #     try:
        #         with open(os.path.join(self.folder_실시간, f'실시간_{s_실시간타입}_{self.s_오늘}.txt'), 'at') as file:
        #             file.write(f'{s_텍스트}\n')
        #     except PermissionError:
        #         pass

        # dic 정의
        self.dic_실시간_현재가 = dict() if not hasattr(self, 'dic_실시간_현재가') else self.dic_실시간_현재가
        self.dic_실시간_체결 = dict() if not hasattr(self, 'dic_실시간_체결') else self.dic_실시간_체결
        self.dic_실시간_호가잔량 = dict() if not hasattr(self, 'dic_실시간_호가잔량') else self.dic_실시간_호가잔량

        # 주식체결 데이터 수집 (누적)
        if s_실시간타입 == "주식체결":
            s_체결시간 = self._get_comm_real_data(s_종목코드, 20)
            s_현재가 = self._get_comm_real_data(s_종목코드, 10)
            s_거래량 = self._get_comm_real_data(s_종목코드, 15)

            # 데이터 정리
            s_체결시간 = f'{s_체결시간[0:2]}:{s_체결시간[2:4]}:{s_체결시간[4:6]}'
            n_현재가 = abs(int(s_현재가))
            s_매수매도 = '매수' if int(s_거래량) > 0 else '매도'
            n_거래량 = abs(int(s_거래량))
            n_거래대금 = n_현재가 * n_거래량

            # 현재가 등록 (갱신)
            try:
                self.dic_실시간_현재가[s_종목코드] = n_현재가
            except AttributeError:
                pass

            # 화면 출력
            li_데이터 = [s_종목코드, s_체결시간, n_현재가]
            s_텍스트 = f'실시간 | 주식시세 | {li_데이터}'
            # print(s_텍스트)
            try:
                with open(os.path.join(self.folder_실시간, f'실시간_주식시세_{self.s_오늘}.txt'), 'at') as file:
                    file.write(f'{s_텍스트}\n')
            except PermissionError:
                pass

            # 주식체결 데이터 저장 (누적)
            li_데이터 = [s_종목코드, s_체결시간, n_현재가, n_거래량, s_매수매도, n_거래대금]
            try:
                if s_종목코드 in self.dic_실시간_체결.keys():
                    self.dic_실시간_체결[s_종목코드].append(li_데이터)
                else:
                    self.dic_실시간_체결[s_종목코드] = [li_데이터]
            except AttributeError:
                return

            # 화면 출력
            s_텍스트 = f'실시간 | {s_실시간타입} | {li_데이터}'
            print(s_텍스트)
            try:
                with open(os.path.join(self.folder_실시간, f'실시간_{s_실시간타입}_{self.s_오늘}.txt'), 'at') as file:
                    file.write(f'{s_텍스트}\n')
            except PermissionError:
                pass

        # 호가잔량 데이터 수집 (갱신)
        if s_실시간타입 == "주식호가잔량":
            s_호가시간 = self._get_comm_real_data(s_종목코드, 21)
            s_매도호가잔량 = self._get_comm_real_data(s_종목코드, 121)
            s_매수호가잔량 = self._get_comm_real_data(s_종목코드, 125)

            # 데이터 정리
            s_호가시간 = f'{s_호가시간[0:2]}:{s_호가시간[2:4]}:{s_호가시간[4:6]}'
            n_매도호가잔량 = abs(int(s_매도호가잔량))
            n_매수호가잔량 = abs(int(s_매수호가잔량))

            # dict 저장
            self.dic_실시간_호가잔량[s_종목코드] = {'종목코드': s_종목코드, '호가시간': s_호가시간,
                                         '매도호가잔량': n_매도호가잔량, '매수호가잔량': n_매수호가잔량}

            # 화면 출력
            li_데이터 = [s_종목코드, s_호가시간, n_매도호가잔량, n_매수호가잔량]
            s_텍스트 = f'실시간 | {s_실시간타입} | {li_데이터}'
            print(s_텍스트)
            try:
                with open(os.path.join(self.folder_실시간, f'실시간_{s_실시간타입}_{self.s_오늘}.txt'), 'at') as file:
                    file.write(f'{s_텍스트}\n')
            except PermissionError:
                pass

    def _get_comm_real_data(self, s_종목코드, n_fid):
        """ 실시간 데이터 값 요청 (OnReceiveRealData 내부에서 사용) """
        ret = self.dynamicCall('GetCommRealData(QString, int)', s_종목코드, n_fid)
        return ret.strip()

    # ***** [ TR 연계 실행 모듈 ] *****
    def set_input_value(self, s_항목명, s_설정값):
        """ TR 요청을 위한 input 값 설정 """
        self.dynamicCall('SetInputValue(QString, QString)', s_항목명, s_설정값)

    def comm_rq_data(self, s_요청명, s_tr코드, n_연속조회여부, s_화면번호):
        """ TR 조회 요청 (OnReceiveTrData 이벤트 자동 호출) \n
        # 연속조회여부 : [0]신규, [2]연속 """
        n_리턴값 = self.dynamicCall('CommRqData(QString, QString, int, QString)',
                                 s_요청명, s_tr코드, n_연속조회여부, s_화면번호)

        # 리턴값 확인
        dic_리턴값 = {0: '조회요청성공', -200: '시세과부하', -201: '조회전문작성에러'}
        s_리턴값 = dic_리턴값[n_리턴값]

        # 이벤트루프 실행
        self.eventloop_tr조회.exec_()

    def on_receive_tr_data(self, s_화면번호, s_요청명, s_tr코드, s_레코드명, s_연속조회, unused1, unused2, unused3, unused4):
        """ TR 데이터 받아오기 (OnReceiveTrData 이벤트 발생 시 연결) """
        # 변수 정의 (s_연속조회 : ['0']추가 데이터 없음, ['2']추가 데이터 있음)
        self.b_추가데이터존재 = True if s_연속조회 == '2' else False if s_연속조회 == '0' else None

        # 요청 종류별 데이터 수신 모듈 연결
        if s_요청명 == '주식일봉차트조회요청': self._opt10081(s_요청명, s_tr코드)     # self.df_일봉
        if s_요청명 == '주식분봉차트조회요청': self._opt10080(s_요청명, s_tr코드)     # self.df_분봉
        if s_요청명 == '예수금상세현황요청': self._opw00001(s_요청명, s_tr코드)       # self.df_예수금
        if s_요청명 == '계좌평가잔고내역요청': self._opw00018(s_요청명, s_tr코드)     # self.df_계좌잔고, self.df_종목별잔고
        if s_요청명 == '거래량급증요청': self._opt10023(s_요청명, s_tr코드)          # self.df_거래량급증
        if s_요청명 == '주식기본정보요청': self._opt10001(s_요청명, s_tr코드)         # self.df_기본정보

        try:
            self.eventloop_tr조회.exit()
        except AttributeError:
            pass

    def _opt10081(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (주식일봉차트조회요청) """
        li_데이터 = self._get_comm_data_ex(s_tr코드, s_요청명)
        li_컬럼명 = ['종목코드', '현재가', '거래량', '거래대금(백만)', '일자', '시가', '고가', '저가',
                  '수정주가구분', '수정비율', '대업종구분', '소업종구분', '종목정보', '수정주가이벤트', '전일종가']
        # 수신 데이터 정리 (비어있는 항목 포함)
        self.df_일봉 = pd.DataFrame(li_데이터, columns=li_컬럼명)

    def _opt10080(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (주식분봉차트조회요청) """
        li_데이터 = self._get_comm_data_ex(s_tr코드, s_요청명)
        li_컬럼명 = ['현재가', '거래량', '체결시간', '시가', '고가', '저가',
                  '수정주가구분', '수정비율', '대업종구분', '소업종구분', '종목정보', '수정주가이벤트', '전일종가']
        # 수신 데이터 정리 (비어있는 항목 포함)
        self.df_분봉 = pd.DataFrame(li_데이터, columns=li_컬럼명)

    def _opw00001(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (예수금상세현황요청) """
        self.s_예수금 = self._get_comm_data(s_tr코드, s_요청명, 0, 'd+2추정예수금')

    def _opw00018(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (계좌평가잔고내역요청) """
        # 잔고 데이터 받아오기 (계좌잔고)
        li_계좌잔고 = list()
        li_계좌잔고.append(self._get_comm_data(s_tr코드, s_요청명, 0, '총매입금액'))
        li_계좌잔고.append(self._get_comm_data(s_tr코드, s_요청명, 0, '총평가금액'))
        li_계좌잔고.append(self._get_comm_data(s_tr코드, s_요청명, 0, '총평가손익금액'))
        li_계좌잔고.append(self._get_comm_data(s_tr코드, s_요청명, 0, '총수익률(%)'))
        li_계좌잔고.append(self._get_comm_data(s_tr코드, s_요청명, 0, '추정예탁자산'))

        # df 생성 (계좌잔고)
        li_컬럼명 = ['매입금액', '평가금액', '평가손익', '수익률', '추정자산']
        df_계좌잔고 = pd.DataFrame([li_계좌잔고], columns=li_컬럼명)
        self.df_계좌잔고 = df_계좌잔고

        # 잔고 데이터 받아오기 (종목별잔고)
        n_데이터길이 = self._데이터길이확인(s_tr코드, s_요청명)
        li_종목별잔고 = []
        for i in range(n_데이터길이):
            # 잔고 데이터 받아오기 (종목별)
            li_잔고 = list()
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '종목번호'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '종목명'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '보유수량'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '매입가'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '현재가'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '평가손익'))
            li_잔고.append(self._get_comm_data(s_tr코드, s_요청명, i, '수익률(%)'))

            li_종목별잔고.append(li_잔고)

        # df 생성 (종목별잔고)
        li_컬럼명 = ['종목코드', '종목명', '보유수량', '매입가', '현재가', '평가손익', '수익률']
        df_종목별잔고 = pd.DataFrame(li_종목별잔고, columns=li_컬럼명)
        self.df_종목별잔고 = df_종목별잔고

    def _opt10023(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (거래량급증요청) """
        li_데이터 = self._get_comm_data_ex(s_tr코드, s_요청명)
        li_컬럼명 = ['종목코드', '종목명', '현재가', '전일대비기호', '전일대비', '등락률', '이전거래량', '현재거래량',
                  '급증량', '급증률']
        # 수신 데이터 정리
        # [ 참고사항 ] 현재거래량: 일누적 거래량, 급증량: 이번틱 거래량, 급증률: 누적 거래량 대비 이번틱 거래량
        self.df_거래량급증 = pd.DataFrame(li_데이터, columns=li_컬럼명)

    def _opt10001(self, s_요청명, s_tr코드):
        """ 데이터 수신 모듈 (거래량급증요청) """
        li_데이터 = list()
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '종목코드'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '종목명'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '액면가'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '상장주식'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '연중최고'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '연중최저'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '시가총액'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '외인소진률'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, 'PER'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '250최고'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '250최저'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '시가'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '고가'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '저가'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '현재가'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '거래량'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '전일대비'))
        li_데이터.append(self._get_comm_data(s_tr코드, s_요청명, 0, '거래대비'))
        li_데이터 = [li_데이터]

        li_컬럼명 = ['종목코드', '종목명', '액면가', '상장주식', '연중최고', '연중최저', '시가총액', '외인소진률', 'PER',
                  '250최고', '250최저', '시가', '고가', '저가', '현재가', '거래량', '전일대비', '거래대비']

        # 수신 데이터 정리
        self.df_기본정보 = pd.DataFrame(li_데이터, columns=li_컬럼명)

    def _데이터길이확인(self, s_tr코드, s_요청명):
        """ TR 결과 데이터 갯수 확인 """
        n_리턴값 = self.dynamicCall('GetRepeatCnt(QString, QString)', s_tr코드, s_요청명)
        return n_리턴값

    def _get_comm_data(self, s_tr코드, s_요청명, n_인덱스, s_항목명):
        """ 요청한 TR 데이터 받아오기 (OnReceiveTrData 내부에서 사용) """
        s_리턴값 = self.dynamicCall('GetCommData(QString, QString, int, QString)', s_tr코드, s_요청명, n_인덱스, s_항목명)

        return s_리턴값.strip()

    def _get_comm_data_ex(self, s_tr코드, s_요청명):
        """ 요청한 TR 데이터 일괄로 받아오기 (OnReceiveTrData 내부에서 사용) """
        li_리턴값 = self.dynamicCall('GetCommDataEx(QString, QString)', s_tr코드, s_요청명)

        return li_리턴값


#######################################################################################################################
if __name__ == "__main__":
    app = QApplication(sys.argv)
    api = KiwoomAPI()
    api.comm_connect()

    # 테스트용 실행 코드
    s_접속서버 = api.get_접속서버()
    s_접속상태 = api.get_접속상태()
    s_로그인정보_코드 = api.get_로그인정보('계좌수')
    s_로그인정보_계좌목록 = api.get_로그인정보('계좌목록')
    s_로그인정보_아이디 = api.get_로그인정보('아이디')
    s_로그인정보_사용자명 = api.get_로그인정보('사용자명')
    s_로그인정보_키보드보안해지여부 = api.get_로그인정보('키보드보안해지여부')
    s_로그인정보_방화벽설정여부 = api.get_로그인정보('방화벽설정여부')
    li_전체종목코드_코스피 = api.get_전체종목코드('코스피')
    li_전체종목코드_코스닥 = api.get_전체종목코드('코스닥')
    li_전체종목코드_ELW = api.get_전체종목코드('ELW')
    li_전체종목코드_ETF = api.get_전체종목코드('ETF')
    li_전체종목코드_KONEX = api.get_전체종목코드('KONEX')
    li_전체종목코드_뮤추얼펀드 = api.get_전체종목코드('뮤추얼펀드')
    li_전체종목코드_신주인수권 = api.get_전체종목코드('신주인수권')
    li_전체종목코드_리츠 = api.get_전체종목코드('리츠')
    li_전체종목코드_하이얼펀드 = api.get_전체종목코드('하이얼펀드')
    li_전체종목코드_KOTC = api.get_전체종목코드('K-OTC')
    s_종목명 = api.get_코드별종목명('000020')
    n_상장주식수 = api.get_코드별상장주식수('000020')
    s_감리구분 = api.get_코드별감리구분('000020')
    s_상장일 = api.get_코드별상장일('000020')
    n_기준가 = api.get_코드별기준가('000020')
    df_종목정보 = api.get_종목정보('000020')
    s_투자유의종목 = api.get_투자유의종목('000020')

    # api.send_주문(s_계좌번호='5292685210', s_주문유형='매수', s_종목코드='000020', n_주문수량=1000, n_주문단가=1000)
    # api.send_주문(s_계좌번호='5292685210', s_주문유형='매수', s_종목코드='133750', n_주문수량=1, n_주문단가=2500)  # 메가엠디
    # api.send_주문(s_계좌번호='5292685210', s_주문유형='매도', s_종목코드='133750', n_주문수량=1, n_주문단가=2000)  # 메가엠디

    dic_조건검색식별종목코드 = api.get_조건검색_전체()
    #
    api.set_실시간_종목등록(s_종목코드='042670', s_등록형태='신규')
    # api.set_실시간_종목등록(s_종목코드='042670', s_등록형태='추가')
    n_실시간_현재가 = api.get_실시간_현재가(s_종목코드='042670')
    df_체결 = api.get_실시간_체결(s_종목코드='042670')
    df_호가잔량 = api.get_실시간_호가잔량(s_종목코드='042670')
    # api.set_실시간_종목해제(s_종목코드='000020')
    #
    df_일봉 = api.get_tr_일봉조회(s_종목코드='000020', s_기준일자_부터='20220525')
    time.sleep(0.2)
    df_분봉 = api.get_tr_분봉조회(s_종목코드='000020', s_기준일자_부터='20230524')
    time.sleep(0.2)
    n_예수금 = api.get_tr_예수금(s_계좌번호='5292685210')
    time.sleep(0.2)
    df_계좌잔고, df_종목별잔고 = api.get_tr_계좌잔고(s_계좌번호='5397778810')
    time.sleep(0.2)
    df_거래량급증 = api.get_tr_거래량급증()
    time.sleep(0.2)
    df_기본정보 = api.get_tr_종목별기본정보(s_종목코드='000020')
    time.sleep(0.2)

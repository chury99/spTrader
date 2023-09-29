import os
import sys
import pandas as pd
import json
import re

import shutil


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
        import UT_폴더정보
        dic_폴더정보 = UT_폴더정보.dic_폴더정보
        self.folder_work = dic_폴더정보['work']

        self.dic_folder = dict()
        self.dic_folder['log'] = dic_config['folder_log']

        self.dic_folder['analyzer'] = {'변동성종목': dic_폴더정보['분석1종목|10_변동성종목'],
                                       '데이터셋': dic_폴더정보['분석1종목|20_데이터셋'],
                                       '모델': dic_폴더정보['분석1종목|30_모델_종목'],
                                       '성능평가': dic_폴더정보['분석1종목|40_성능평가'],
                                       '감시대상': dic_폴더정보['분석1종목|감시대상'],
                                       '감시대상모델': dic_폴더정보['분석1종목|모델_감시대상'],
                                       '상승예측': dic_폴더정보['백테스팅|10_상승예측'],
                                       '수익검증': dic_폴더정보['백테스팅|20_수익검증']}

        self.dic_folder['collector'] = {'ohlcv': dic_폴더정보['데이터|ohlcv'],
                                        '정보수집': dic_폴더정보['데이터|정보수집'],
                                        '캐시변환': dic_폴더정보['데이터|캐시변환']}

        self.dic_folder['trader'] = {'run': dic_폴더정보['run'],
                                     '메세지': dic_폴더정보['이력|메세지'],
                                     '실시간': dic_폴더정보['이력|실시간'],
                                     '체결잔고': dic_폴더정보['이력|체결잔고']}

        # 변수 설정
        self.n_보관기간_log = int(dic_config['파일보관기간(개월)_log'])
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(개월)_analyzer'])
        self.n_보관기간_collector = int(dic_config['파일보관기간(개월)_collector'])
        self.n_보관기간_trader = int(dic_config['파일보관기간(개월)_trader'])

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])

        # log 기록
        self.make_log(f'### 파일 보관기간 관리 시작 ###')

    def 파일관리_log(self):
        """ 로그파일 확인하여 보관기간 지난 파일 삭제 """
        # 기준 일자 정의
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(months=self.n_보관기간_log)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        # 삭제대상 파일 찾기
        s_폴더 = self.dic_folder['log']
        li_파일_전체 = os.listdir(s_폴더)
        li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
        li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
        li_패스_삭제대상 = [os.path.join(s_폴더, 파일) for 파일 in li_파일_삭제대상]

        # 대상 파일 삭제
        for s_패스 in li_패스_삭제대상:
            os.system(f'del {s_패스}')

        # log 기록
        self.make_log(f'파일 삭제 완료({self.n_보관기간_log}개월 경과, {s_기준일자} 기준, {len(li_패스_삭제대상):,}개 파일)')

    def 파일관리_analyzer(self):
        """ analyzer에서 생성되는 파일 확인하여 보관기간 지난 파일 삭제 """
        # 기준 일자 정의
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(months=self.n_보관기간_analyzer)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        # 삭제대상 파일 찾기
        dic_폴더 = self.dic_folder['analyzer']
        li_패스_삭제대상 = list()
        for s_폴더명 in dic_폴더.keys():
            # 예외 폴더 정의
            if s_폴더명 in []:
                continue
            # 대상 폴더 처리
            s_폴더 = dic_폴더[s_폴더명]
            li_파일_전체 = os.listdir(s_폴더)
            li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
            li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
            [li_패스_삭제대상.append(os.path.join(s_폴더, 파일)) for 파일 in li_파일_삭제대상]

        # 대상 파일 삭제
        for s_패스 in li_패스_삭제대상:
            os.system(f'del {s_패스}')

        # log 기록
        self.make_log(f'파일 삭제 완료({self.n_보관기간_analyzer}개월 경과, {s_기준일자} 기준, {len(li_패스_삭제대상):,}개 파일)')

    def 파일관리_collector(self):
        """ collector에서 생성되는 파일 확인하여 보관기간 지난 파일 삭제 """
        # 기준 일자 정의
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(months=self.n_보관기간_collector)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        # 삭제대상 파일 찾기
        dic_폴더 = self.dic_folder['collector']
        li_패스_삭제대상 = list()
        for s_폴더명 in dic_폴더.keys():
            # 예외 폴더 정의
            if s_폴더명 in ['ohlcv', '캐시변환']:
                continue
            # 대상 폴더 처리
            s_폴더 = dic_폴더[s_폴더명]
            li_파일_전체 = os.listdir(s_폴더)
            li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
            li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
            [li_패스_삭제대상.append(os.path.join(s_폴더, 파일)) for 파일 in li_파일_삭제대상]

        # 대상 파일 삭제
        for s_패스 in li_패스_삭제대상:
            os.system(f'del {s_패스}')

        # log 기록
        self.make_log(f'파일 삭제 완료({self.n_보관기간_collector}개월 경과, {s_기준일자} 기준, {len(li_패스_삭제대상):,}개 파일)')

    def 파일관리_trader(self):
        """ trader에서 생성되는 파일 확인하여 보관기간 지난 파일 삭제 """
        # 기준 일자 정의
        dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(months=self.n_보관기간_trader)
        s_기준일자 = dt_기준일자.strftime('%Y%m%d')

        # 삭제대상 파일 찾기
        dic_폴더 = self.dic_folder['trader']
        li_패스_삭제대상 = list()
        for s_폴더명 in dic_폴더.keys():
            # 예외 폴더 정의
            if s_폴더명 in ['run']:
                continue
            # 대상 폴더 처리
            s_폴더 = dic_폴더[s_폴더명]
            li_파일_전체 = os.listdir(s_폴더)
            li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
            li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
            [li_패스_삭제대상.append(os.path.join(s_폴더, 파일)) for 파일 in li_파일_삭제대상]

        # 대상 파일 삭제
        for s_패스 in li_패스_삭제대상:
            os.system(f'del {s_패스}')

        # log 기록
        self.make_log(f'파일 삭제 완료({self.n_보관기간_trader}개월 경과, {s_기준일자} 기준, {len(li_패스_삭제대상):,}개 파일)')

    def 잔여공간확인(self):
        """ folder_work 폴더가 위치한 드라이브의 잔여 공간 확인하여 출력 """
        # 드라이브 위치 확인
        s_드라이브 = self.folder_work[0]

        # 용량 확인
        obj_디스크공간 = shutil.disk_usage(self.folder_work)
        n_잔여공간_GB = obj_디스크공간.free / (1024 ** 3)
        n_전체공간_GB = obj_디스크공간.total / (1024 ** 3)
        n_잔여비율_퍼센트 = n_잔여공간_GB / n_전체공간_GB * 100

        # 카톡 보내기 (잔여공간 10GB 미만 시)
        if n_잔여공간_GB < 10:
            import API_kakao
            k = API_kakao.KakaoAPI()
            s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
            result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{s_파일}] 저장 공간 Full 경고',
                                    s_button_title=f'잔여 공간 - {n_잔여공간_GB:.1f}GB ({n_잔여비율_퍼센트:.0f}%)')

        # log 기록
        self.make_log(f'{s_드라이브}드라이브 잔여 공간 - {n_잔여공간_GB:.1f}GB ({n_잔여비율_퍼센트:.0f}%)')

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

    # r.파일관리_log()
    # ### log 파일은 안 지우는 게 좋을 듯 (icloud와 충돌 발생) ###
    r.파일관리_analyzer()
    r.파일관리_collector()
    r.파일관리_trader()
    r.잔여공간확인()

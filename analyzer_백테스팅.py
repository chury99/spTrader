import os
import sys
import pandas as pd
import json
import re

import matplotlib.pyplot as plt
from tqdm import tqdm

import analyzer_알고리즘 as Logic

# 그래프 한글 설정
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


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
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_감시대상 = dic_폴더정보['분석1종목|50_종목_감시대상']
        self.folder_종목준비 = dic_폴더정보['백테스팅|10_종목준비']
        self.folder_데이터셋 = dic_폴더정보['백테스팅|20_데이터셋']
        self.folder_공통모델검증 = dic_폴더정보['백테스팅|30_공통모델검증']
        self.folder_종목모델검증 = dic_폴더정보['백테스팅|40_종목모델검증']
        self.folder_매도검증 = dic_폴더정보['백테스팅|50_매도검증']
        os.makedirs(self.folder_종목준비, exist_ok=True)
        os.makedirs(self.folder_데이터셋, exist_ok=True)
        # os.makedirs(self.folder_공통모델검증, exist_ok=True)
        # os.makedirs(self.folder_종목모델검증, exist_ok=True)
        # os.makedirs(self.folder_매도검증, exist_ok=True)

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 백테스팅_종목준비(self, s_모델):
        """ 감시대상 종목 불러와서 10분봉 데이터 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 감시대상 불러오기 (전일 기준 - 전일 생성한 데이터 불러와서 당일 감시)
            df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_모델}_{s_일자_전일}.pkl'))
            df_감시대상['li_조건'] = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': '확률스펙'].values]
            try:
                dic_종목조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
            except KeyError:
                dic_종목조건 = dict()

            # 10분봉 데이터 불러오기 (당일 기준 - 실제는 10분봉 tr 조회 후 리턴값 사용)
            dic_10분봉 = dict()
            dic_10분봉_전체 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            for s_종목코드 in dic_종목조건.keys():
                df_10분봉 = dic_10분봉_전체[s_종목코드]
                dic_10분봉[s_종목코드] = df_10분봉

            # 결과 저장
            pd.to_pickle(dic_종목조건, os.path.join(self.folder_종목준비, f'dic_종목조건_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(dic_10분봉, os.path.join(self.folder_종목준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'종목준비 완료({s_일자}, {len(dic_10분봉):,}개 종목, {s_모델})')

    def 백테스팅_데이터셋(self, s_모델):
        """ 10분봉 데이터 기준으로 모델 입력용 추가 데이터 생성 후 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_데이터셋)
                    if f'dic_데이터셋_종목_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 종목별 10분봉 파일 불러오기
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_종목준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))

            # 공통모델 데이터셋 생성

            # 종목모델 데이터셋 생성

            # 결과 저장
            # pd.to_pickle(dic_데이터셋_공통, os.path.join(self.folder_데이터셋, f'dic_데이터셋_공통_{s_모델}_{s_일자}.pkl'))
            # pd.to_pickle(dic_데이터셋_종목, os.path.join(self.folder_데이터셋, f'dic_데이터셋_종목_{s_모델}_{s_일자}.pkl'))

            # log 기록
            # self.make_log(f'데이터셋 준비 완료({s_일자}, {s_모델})')

            pass

    def 백테스팅_공통모델(self, s_모델):
        """ 10분 단위로 공통모델 검증 후 결과 저장 """
        # 감시대상종목 전체 대상
        # 확률스펙 55 % 기준 판정
        # 시간별 판정 결과 df 저장
        pass

    def 백테스팅_종목모델(self, s_모델):
        """ 공통모델 결과를 바탕으로 종목모델 검증 후 결과 저장 """
        # 공통모델 중 ok 종목만 대상
        # 모델 조건 기준 판정
        # 시간별 판정 결과 df 저장
        pass

    def 백테스팅_매도검증(self, s_모델):
        """ 1분봉 기준 매도 조건 검증 후 결과 저장 """
        # 공통/종목모델 모두 ok인 항목 대상
        # 1분봉 기준 h가 +3% 이상이면 +2.5%
        # 1분봉 기준 l이 -3% 이하이면 -3.5%
        # 결과는 df로 저장 (매수시점, 매도시점 표기)
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

    a.백테스팅_종목준비(s_모델='rf')
    a.백테스팅_데이터셋(s_모델='rf')
    a.백테스팅_공통모델(s_모델='rf')
    a.백테스팅_종목모델(s_모델='rf')
    a.백테스팅_매도검증(s_모델='rf')

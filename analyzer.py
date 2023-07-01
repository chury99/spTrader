import os
import sys
import pandas as pd
import json

import pandas.errors
from tqdm import tqdm


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
        folder_work = dic_config['folder_work']
        folder_데이터 = os.path.join(folder_work, '데이터')
        self.folder_ohlcv = os.path.join(folder_데이터, 'ohlcv')
        self.folder_캐시변환 = os.path.join(folder_데이터, '캐시변환')
        self.folder_정보수집 = os.path.join(folder_데이터, '정보수집')
        folder_분석 = os.path.join(folder_work, '분석')
        self.folder_감시대상 = os.path.join(folder_분석, '감시대상')
        self.folder_모델_lstm = os.path.join(folder_분석, '모델_lstm')
        self.folder_모델_rf = os.path.join(folder_분석, '모델_rf')
        os.makedirs(self.folder_감시대상, exist_ok=True)
        os.makedirs(self.folder_모델_lstm, exist_ok=True)
        os.makedirs(self.folder_모델_rf, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        li_일자 = [파일명.split('_')[3].replace('.pkl', '') for 파일명 in os.listdir(self.folder_캐시변환)
                  if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명]
        self.li_일자_전체 = sorted(li_일자)[-90:]

        # log 기록
        self.make_log(f'### 종목 분석 시작 ###')

    def 분석_후보종목(self):
        """ 전체 종목 분석해서 감시대상 종목 선정 후 pkl, csv 저장 \n
            # 선정기준 : 10분봉 기준 3%이상 상승이 하루에 2회 이상 존재 """
        # 분석대상 일자 선정
        li_일자_완료 = [파일명.split('_')[2].replace('.pkl', '') for 파일명 in os.listdir(self.folder_감시대상)
                    if 'df_후보종목_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in self.li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 10분봉 데이터 불러오기 (from 캐시변환)
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))

            # 종목별 분석 진행
            li_후보종목 = list()
            for s_종목코드 in tqdm(dic_10분봉.keys(), desc=f'후보종목선정|{s_일자}'):
                # 분석대상 종목에 포함 여부 확인
                if s_종목코드 not in self.li_종목_분석대상:
                    continue

                # 10분봉 39개 존재 여부 확인
                df_10분봉 = dic_10분봉[s_종목코드]
                if len(df_10분봉) < 39:
                    continue

                # 상승률 생성 (10분봉 기준)
                li_상승률 = list((df_10분봉['종가'] / df_10분봉['종가'].shift(1) - 1) * 100)
                li_상승률_처음값 = [(df_10분봉['종가'].values[0] / df_10분봉['전일종가'].values[0] - 1) * 100]
                df_10분봉['상승률(%)'] = li_상승률_처음값 + li_상승률[1:]

                # 3% 이상 상승 갯수 확인
                li_상승여부 = [1 if n_상승률 >= 3 else 0 for n_상승률 in df_10분봉['상승률(%)']]
                if sum(li_상승여부) >= 2:
                    s_종목명 = self.dic_코드2종목명[s_종목코드]
                    n_상승갯수 = sum(li_상승여부)
                    li_후보종목.append([s_종목코드, s_종목명, n_상승갯수])

            # df 저장
            df_후보종목 = pd.DataFrame(li_후보종목, columns=['종목코드', '종목명', '상승갯수'])
            df_후보종목.to_pickle(os.path.join(self.folder_감시대상, f'df_후보종목_{s_일자}.pkl'))
            df_후보종목.to_csv(os.path.join(self.folder_감시대상, f'후보종목_{s_일자}.csv'), index=False, encoding='cp949')

            # log 기록
            self.make_log(f'후보종목 선정 완료({s_일자}) - {len(df_후보종목):,}종목')

    def 분석_lstm(self):
        """ 감시대상 종목 기준으로 lstm 분석해서 종목별 모델 생성 """
        pass

    def 분석_rf(self):
        """ 감시대상 종목 기준으로 random forest 분석해서 종목별 모델 생성 """
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

    a.분석_후보종목()
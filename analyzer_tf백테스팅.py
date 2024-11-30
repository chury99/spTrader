import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import UT_차트maker as Chart
import analyzer_tf알고리즘 as Logic

# 그래프 한글 설정
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames,PyUnusedLocal
class Analyzer:
    def __init__(self, n_분석일수=None):
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
        self.folder_분봉확인 = dic_폴더정보['tf초봉분석|20_분봉확인']
        self.folder_매수매도 = dic_폴더정보['tf백테스팅|10_매수매도']
        self.folder_결과정리 = dic_폴더정보['tf백테스팅|20_결과정리']
        self.folder_결과요약 = dic_폴더정보['tf백테스팅|30_결과요약']
        self.folder_수익요약 = dic_폴더정보['tf백테스팅|40_수익요약']
        os.makedirs(self.folder_매수매도, exist_ok=True)
        os.makedirs(self.folder_결과정리, exist_ok=True)
        os.makedirs(self.folder_결과요약, exist_ok=True)
        os.makedirs(self.folder_수익요약, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수
        self.li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                        if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 검증_매수매도(self, b_차트, n_초봉):
        """ 지표 추가된 초봉 데이터 기준 매수, 매도 검증 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_분봉확인'
        s_파일명_생성 = 'dic_매수매도'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_분봉확인)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 초봉 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_분봉확인, f'dic_분봉확인_{s_일자}_{n_초봉}초봉.pkl'))

            # 종목별 분석 진행
            dic_매수매도 = dict()
            for s_종목코드 in tqdm(dic_초봉.keys(), desc=f'매수매도-{n_초봉}초봉-{s_일자}'):
                # df 정의
                df_초봉 = dic_초봉[s_종목코드].sort_index().copy()

                # 검증 초기값 생성
                b_매수신호 = False
                b_매도신호 = False
                b_보유신호 = False
                n_매수가 = None
                n_매도가 = None
                s_매수시간 = None
                s_매도시간 = None
                s_매도사유 = None

                # 시간별 검증
                li_df_매수매도 = list()
                for idx in df_초봉.index:
                    # 초봉 데이터 준비
                    df_초봉_시점 = df_초봉[:idx]
                    df_초봉_시점 = df_초봉_시점[-50:]

                    # 매수검증
                    if not b_보유신호:
                        # 매수신호 검증
                        li_매수신호, dic_신호상세 = Logic.make_매수신호(df_초봉=df_초봉_시점, dt_일자시간=idx)
                        b_매수신호 = sum(li_매수신호) == len(li_매수신호)

                        # 매수 정보 생성
                        if b_매수신호:
                            n_시가 = df_초봉_시점['시가'].values[-1]
                            n_매수가 = n_시가 if pd.notna(n_시가) else df_초봉_시점['종가'].values[-2]
                            s_매수시간 = df_초봉_시점['체결시간'].values[-1]
                            b_보유신호 = True

                    # 매도검증
                    if b_보유신호:
                        # 매도신호 검증
                        li_매도신호, dic_신호상세 = Logic.make_매도신호(df_초봉=df_초봉_시점,
                                                                    n_매수가=n_매수가, s_매수시간=s_매수시간, dt_일자시간=idx)
                        b_매도신호 = sum(li_매도신호) > 0
                        li_신호종류 = ['매도우세', '하락한계', '타임아웃', '시장종료']

                        # 매도 정보 생성
                        if b_매도신호:
                            n_시가 = df_초봉_시점['시가'].values[-1]
                            n_매도가 = n_시가 if pd.notna(n_시가) else df_초봉_시점['종가'].values[-2]
                            s_매도시간 = df_초봉_시점['체결시간'].values[-1]
                            s_매도사유 = [li_신호종류[i] for i in range(len(li_매도신호)) if li_매도신호[i]][0]

                    # df 정리
                    df_매수매도 = df_초봉_시점[-1:].copy()
                    df_매수매도['매수신호'] = b_매수신호
                    df_매수매도['매도신호'] = b_매도신호
                    df_매수매도['보유신호'] = b_보유신호
                    df_매수매도['매수가'] = n_매수가
                    df_매수매도['매도가'] = n_매도가
                    df_매수매도['매수시간'] = s_매수시간
                    df_매수매도['매도시간'] = s_매도시간
                    df_매수매도['매도사유'] = s_매도사유
                    df_매수매도['수익률%'] = (df_매수매도['시가'] / df_매수매도['매수가'] - 1) * 100 - 0.2
                    li_df_매수매도.append(df_매수매도)

                    # 초기화
                    if b_매도신호:
                        b_매수신호 = False
                        b_매도신호 = False
                        b_보유신호 = False
                        n_매수가 = None
                        n_매도가 = None
                        s_매수시간 = None
                        s_매도시간 = None
                        s_매도사유 = None

                # df 생성
                df_매수매도 = pd.concat(li_df_매수매도, axis=0).sort_index()

                # dic 추가
                dic_매수매도[s_종목코드] = df_매수매도

                # csv 저장
                s_폴더 = os.path.join(self.folder_매수매도, f'종목별_{s_일자}')
                os.makedirs(s_폴더, exist_ok=True)
                df_매수매도.to_csv(os.path.join(s_폴더, f'df_매수매도_{s_일자}_{n_초봉}초봉_{s_종목코드}.csv'),
                             index=False, encoding='cp949')

            # dic 저장
            pd.to_pickle(dic_매수매도, os.path.join(self.folder_매수매도, f'dic_매수매도_{s_일자}_{n_초봉}초봉.pkl'))

            # log 기록
            self.make_log(f'신호생성 완료({s_일자}, {n_초봉}초봉, {len(dic_매수매도):,}개 종목)')

    def 검증_결과정리(self, b_차트, n_초봉):
        """ 매수 매도 신호 기준으로 결과 정리 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'dic_매수매도'
        s_파일명_생성 = 'df_결과정리'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 초봉 불러오기
            dic_초봉 = pd.read_pickle(os.path.join(self.folder_매수매도, f'dic_매수매도_{s_일자}_{n_초봉}초봉.pkl'))

            # 종목별 매수매도 통합
            li_df_결과정리 = list()
            for s_종목코드 in dic_초봉.keys():
                df_초봉 = dic_초봉[s_종목코드].sort_index().copy()
                li_df_결과정리.append(df_초봉[df_초봉['매도시간'] > '00:00:00'])
            df_결과정리 = pd.concat(li_df_결과정리, axis=0)

            # 추가정보 생성
            df_결과정리['수익률%'] = (df_결과정리['매도가'] / df_결과정리['매수가'] - 1) * 100 - 0.2
            df_결과정리['보유초'] = (pd.to_datetime(df_결과정리['매도시간'], format='%H:%M:%S')
                                    - pd.to_datetime(df_결과정리['매수시간'], format='%H:%M:%S')).dt.total_seconds()
            df_결과정리['타임아웃'] = df_결과정리['매도사유'].apply(lambda x: True if x == '타임아웃' else None)

            # 파일 저장
            df_결과정리.to_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl'))
            df_결과정리.to_csv(os.path.join(self.folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'결과정리 완료({s_일자}, {n_초봉}초봉, 수익률: {df_결과정리["수익률%"].sum():,.1f}%)')

    def 검증_결과요약(self, b_차트, n_초봉):
        """ 결과정리 데이터 기준으로 일별 데이터 요약 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_결과정리'
        s_파일명_생성 = 'df_결과요약'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전체 결과정리 파일 확인
            li_파일일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                       if s_파일명_기준 in 파일명 and '.pkl' in 파일명 and f'{n_초봉}초봉' in 파일명]
            li_파일일자 = [파일일자 for 파일일자 in li_파일일자 if 파일일자 <= s_일자]

            # 파일별 결과 요약
            li_df_결과요약 = list()
            for s_파일일자 in li_파일일자:
                # 결과 파일 불러오기
                df_결과 = pd.read_pickle(os.path.join(self.folder_결과정리, f'df_결과정리_{s_파일일자}_{n_초봉}초봉.pkl'))
                dic_결과_선정사유 = dict()
                li_선정사유 = ['일봉변동', 'vi발동', '거래량급증']
                for s_선정사유 in li_선정사유:
                    dic_결과_선정사유[s_선정사유] = df_결과[df_결과['선정사유'] == s_선정사유]

                # 결과 요약
                df_일별요약 = pd.DataFrame({'일자': [s_파일일자]})
                df_일별요약['전체거래'] = int(len(df_결과))
                df_일별요약['수익거래'] = int(len(df_결과[df_결과['수익률%'] > 0]))
                df_일별요약['성공률%'] = (df_일별요약['수익거래'] / df_일별요약['전체거래']) * 100
                df_일별요약['수익률%'] = df_결과['수익률%'].sum()
                df_일별요약['평균수익률%'] = df_일별요약['수익률%'] / df_일별요약['전체거래']
                df_일별요약['평균보유초'] = df_결과['보유초'].mean()
                df_일별요약['종목수'] = len(df_결과['종목코드'].unique())
                df_일별요약['종목당거래'] = df_일별요약['전체거래'] / df_일별요약['종목수']
                for s_선정사유 in li_선정사유:
                    df_결과_선정사유 = dic_결과_선정사유[s_선정사유]
                    df_일별요약[f'{s_선정사유[:2]}|성공률%'] = (int(len(df_결과_선정사유[df_결과_선정사유['수익률%'] > 0]))
                                                / int(len(df_결과_선정사유))) * 100 if len(df_결과_선정사유) > 0 else None
                    df_일별요약[f'{s_선정사유[:2]}|수익률%'] = df_결과_선정사유['수익률%'].sum()
                li_df_결과요약.append(df_일별요약)

            # df 생성
            df_결과요약 = pd.concat(li_df_결과요약, axis=0).sort_values('일자', ascending=False)

            # 파일 저장
            df_결과요약.to_pickle(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{n_초봉}초봉.pkl'))
            df_결과요약.to_csv(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{n_초봉}초봉.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'결과요약 완료({s_일자}, {n_초봉}초봉, 누적 수익률: {df_결과요약["수익률%"].sum():,.1f}%)')

    def 검증_수익요약(self, b_카톡):
        """ 결과요약 데이터 기준 초봉별 수익률 정리 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_결과요약'
        s_파일명_생성 = 'df_수익요약'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익요약)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자 중복 제거
        li_일자_대상 = list(dict.fromkeys(li_일자_대상))

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 내 초봉 확인
            li_초봉 = [re.findall(r'\d+초봉', 파일명)[0] for 파일명 in os.listdir(self.folder_결과요약)
                     if s_일자 in 파일명 and '.pkl' in 파일명]
            li_초봉 = sorted(li_초봉, key=lambda x: int(x.replace('초봉', '')))

            # 초봉별 정리
            df_수익요약 = pd.DataFrame()
            li_선정사유 = ['일봉변동', 'vi발동', '거래량급증']
            for s_초봉 in li_초봉:
                # 전체 수익률
                df_결과 = pd.read_pickle(os.path.join(self.folder_결과요약, f'df_결과요약_{s_일자}_{s_초봉}.pkl'))
                df_수익요약['일자'] = ['누적%'] + list(df_결과['일자'])
                df_수익요약[f'전체|{s_초봉}'] = [df_결과['수익률%'].sum()] + list(df_결과['수익률%'])

                # 선정 사유별 수익률
                for s_선정사유 in li_선정사유:
                    df_수익요약[f'{s_선정사유[:2]}|{s_초봉}'] = ([df_결과[f'{s_선정사유[:2]}|수익률%'].sum()]
                                                            + list(df_결과[f'{s_선정사유[:2]}|수익률%']))

            # 컬럼 정리
            li_컬럼명 = ['일자']
            for s_선정사유 in ['전체'] + li_선정사유:
                li_컬럼명 = li_컬럼명 + [컬럼명 for 컬럼명 in df_수익요약.columns if f'{s_선정사유[:2]}|' in 컬럼명]
            df_수익요약 = df_수익요약.loc[:, li_컬럼명]

            # 파일 저장
            df_수익요약.to_pickle(os.path.join(self.folder_수익요약, f'df_수익요약_{s_일자}.pkl'))
            df_수익요약.to_csv(os.path.join(self.folder_수익요약, f'df_수익요약_{s_일자}.csv'),
                          index=False, encoding='cp949')

            # log 기록
            self.make_log(f'수익요약 완료({s_일자})')

            # df_리포트 생성
            li_컬럼명 = [컬럼명 for 컬럼명 in df_수익요약.columns if '일자' not in 컬럼명]
            df_리포트 = pd.DataFrame()
            df_리포트['일자'] = df_수익요약['일자']
            for s_컬럼명 in li_컬럼명:
                df_리포트[s_컬럼명] = df_수익요약[s_컬럼명].apply(lambda x: f'{x:.2f}')
            df_리포트['v+거|5'] = (df_수익요약['vi|5초봉'] + df_수익요약['거래|5초봉']).apply(lambda x: f'{x:.2f}')
            df_리포트 = df_리포트[:15]

            # 리포트 생성
            n_세로 = int(len(df_리포트) / 2)
            fig = plt.Figure(figsize=(16, n_세로))
            ax = fig.add_subplot(1, 1, 1)
            ax.axis('tight')
            ax.axis('off')
            obj_테이블 = ax.table(cellText=df_리포트.values, colLabels=df_리포트.columns, cellLoc='center', loc='center')
            obj_테이블.auto_set_font_size(False)
            obj_테이블.set_fontsize(8)
            obj_테이블.scale(1.0, 2.4)

            # 리포트 파일 저장
            folder_리포트 = os.path.join(self.folder_수익요약, '리포트')
            os.makedirs(folder_리포트, exist_ok=True)
            s_파일명_리포트 = f'백테스팅_리포트_{s_일자}.png'
            fig.savefig(os.path.join(folder_리포트, s_파일명_리포트), bbox_inches='tight', dpi=600)
            plt.close(fig)

            # 리포트 복사 to 서버
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/tf분석_백테스팅'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡:
                import API_kakao
                k = API_kakao.KakaoAPI()
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 백테스팅 완료',
                                        s_button_title=f'[tf분석] 백테스팅 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'수익요약 완료({s_일자})')

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
    a = Analyzer(n_분석일수=None)
    li_초봉 = [1, 2, 3, 5, 10]
    for n_초봉 in li_초봉:
        a.검증_매수매도(b_차트=True, n_초봉=n_초봉)
    for n_초봉 in li_초봉:
        a.검증_결과정리(b_차트=True, n_초봉=n_초봉)
    for n_초봉 in li_초봉:
        a.검증_결과요약(b_차트=True, n_초봉=n_초봉)
    a.검증_수익요약(b_카톡=True)

import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import analyzer_sr알고리즘 as Logic

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
        self.folder_일봉변동 = dic_폴더정보['sr종목선정|10_일봉변동']
        self.folder_지지저항 = dic_폴더정보['sr종목선정|20_지지저항']
        self.folder_종목선정 = dic_폴더정보['sr종목선정|50_종목선정']
        self.folder_매수매도 = dic_폴더정보['sr백테스팅|10_매수매도']
        self.folder_결과정리 = dic_폴더정보['sr백테스팅|20_결과정리']
        os.makedirs(self.folder_매수매도, exist_ok=True)
        os.makedirs(self.folder_결과정리, exist_ok=True)

        # 변수 설정
        self.n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
        self.n_분석일수 = n_분석일수

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 검증_매수매도(self, b_차트):
        """ 전일 종목선정 데이터 기준으로 당일 매수매도 분석하여 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_종목선정'
        s_파일명_생성 = 'df_매수매도'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목선정)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 날짜 확인
            li_전체일자 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                       if 'dic_코드별_분봉' in 파일명 and '.pkl' in 파일명]
            s_일자_당일 = s_일자
            s_일자_전일 = max(일자 for 일자 in li_전체일자 if 일자 < s_일자_당일)

            # 종목선정, 지지저항 불러오기 (전일)
            try:
                # df_종목선정 = pd.read_pickle(os.path.join(self.folder_종목선정, f'{s_파일명_기준}_{s_일자_전일}.pkl'))
                df_일봉변동 = pd.read_pickle(os.path.join(self.folder_일봉변동, f'df_일봉변동_{s_일자_전일}.pkl'))
                df_지지저항 = pd.read_pickle(os.path.join(self.folder_지지저항, f'df_지지저항_{s_일자_전일}.pkl'))
                # li_대상종목 = list(df_종목선정['종목코드'].sort_values().unique())
                li_대상종목 = list(df_일봉변동['종목코드'].sort_values().unique())
            except FileNotFoundError:
                continue

            # 3분봉, 1분봉 불러오기 (당일)
            dic_1분봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자_당일}.pkl'))
            dic_3분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_전일}.pkl'))
            dic_3분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_일자_당일}.pkl'))

            # 매수매도 검증
            li_df_매수매도 = list()
            for s_종목코드 in tqdm(li_대상종목, desc=f'매수매도 검증|{s_일자}'):
                # 데이터 준비
                try:
                    df_3분봉_당일 = dic_3분봉_당일[s_종목코드]
                except KeyError:
                    continue
                df_3분봉 = pd.concat([dic_3분봉_전일[s_종목코드], df_3분봉_당일], axis=0).sort_values(['일자', '시간'])
                df_3분봉['일자시간'] = pd.to_datetime(df_3분봉['일자'] + ' ' + df_3분봉['시간'], format='%Y%m%d %H:%M:%S')
                df_3분봉 = df_3분봉.set_index('일자시간').sort_index()

                df_1분봉 = dic_1분봉[s_종목코드]

                df_지지저항_전일 = df_지지저항[df_지지저항['종목코드'] == s_종목코드]
                df_지지저항_종목 = Logic.find_지지저항_추가통합(df_지지저항_기존=df_지지저항_전일, df_ohlcv_신규=df_3분봉)

                # 매수신호 탐색
                li_df_매수신호_종목 = list()
                for s_시간 in df_3분봉_당일['시간']:
                    # 데이터 준비 (해당 시점)
                    dt_일자시간 = pd.Timestamp(f'{s_일자} {s_시간}')
                    df_3분봉_시점 = df_3분봉[df_3분봉.index <= dt_일자시간]
                    df_지지저항_시점 = df_지지저항_종목[df_지지저항_종목.index <= dt_일자시간]
                    li_지지저항 = list(df_지지저항_시점['고가'].values)

                    # 매수신호 생성
                    li_매수신호 = Logic.find_매수신호(df_ohlcv=df_3분봉_시점, li_지지저항=li_지지저항, dt_일자시간=dt_일자시간)

                    # 매수신호 확인
                    if sum(li_매수신호) == len(li_매수신호):
                        df_매수신호_시점 = df_3분봉_시점.loc[:, ['일자', '종목코드', '종목명']].copy()[-1:]
                        df_매수신호_시점['매수시간'] = s_시간
                        df_매수신호_시점['매수단가'] = int(df_3분봉_시점['시가'].values[-1])
                        li_df_매수신호_종목.append(df_매수신호_시점)

                df_매수신호_종목 = pd.concat(li_df_매수신호_종목, axis=0) if len(li_df_매수신호_종목) > 0 else pd.DataFrame()

                # 매도신호 탐색
                li_df_매수매도_종목 = list()
                li_매수시간 = list(df_매수신호_종목['매수시간']) if len(df_매수신호_종목) > 0 else list()
                s_매도시간 = '00:00:00'
                for s_매수시간 in li_매수시간:
                    # 매도 전 매수 금지
                    if s_매수시간 <= s_매도시간:
                        continue

                    # 기준정보 정의
                    n_매수단가 = df_매수신호_종목[df_매수신호_종목['매수시간'] == s_매수시간]['매수단가'].values[0]

                    # 1분봉 데이터 확인
                    for s_시간 in df_1분봉['시간']:
                        # 매수시간 이전 데이터 skip
                        if s_시간 < s_매수시간:
                            continue

                        # 기준정보 정의
                        dt_일자시간 = pd.Timestamp(f'{s_일자} {s_시간}')
                        df_지지저항_시점 = df_지지저항_종목[df_지지저항_종목.index <= dt_일자시간]
                        li_지지저항 = list(df_지지저항_시점['고가'].values)
                        n_지지선 = max(지지 for 지지 in li_지지저항 if 지지 < n_매수단가) if min(li_지지저항) < n_매수단가 else None
                        n_저항선 = min(저항 for 저항 in li_지지저항 if 저항 > n_매수단가) if max(li_지지저항) > n_매수단가 else None
                        dic_지지저항 = {'n_매수단가': n_매수단가, 'n_지지선': n_지지선, 'n_저항선': n_저항선}

                        # 기준 데이터 정의
                        df_1분봉_시점 = df_1분봉[df_1분봉['시간'] == s_시간]
                        n_고가 = df_1분봉_시점['고가'].values[0]
                        n_저가 = df_1분봉_시점['저가'].values[0]

                        # 매도신호 생성
                        li_매도신호_고가, n_매도단가_고가 = Logic.find_매도신호(n_현재가=n_고가,
                                                                dic_지지저항=dic_지지저항, s_현재시간=s_시간)
                        li_매도신호_저가, n_매도단가_저가 = Logic.find_매도신호(n_현재가=n_저가,
                                                                dic_지지저항=dic_지지저항, s_현재시간=s_시간)
                        li_매도신호 = [(li_매도신호_고가[i] or li_매도신호_저가[i]) for i in range(len(li_매도신호_고가))]
                        n_매도단가 = n_매도단가_고가 or n_매도단가_저가

                        # 매도신호 확인
                        if sum(li_매도신호) > 0:
                            df_매수매도_시점 = df_매수신호_종목[df_매수신호_종목['매수시간'] == s_매수시간].copy()
                            df_매수매도_시점['매도시간'] = s_시간
                            df_매수매도_시점['매도단가'] = int(n_매도단가)
                            df_매수매도_시점['수익률(%)'] = (df_매수매도_시점['매도단가'] / df_매수매도_시점['매수단가'] - 1) * 100
                            li_df_매수매도_종목.append(df_매수매도_시점)
                            break

                # 종목별 df_매수매도 생성
                df_매수매도_종목 = pd.concat(li_df_매수매도_종목, axis=0) if len(li_df_매수매도_종목) > 0 else pd.DataFrame()

                # 매도 전 매수 케이스 제거
                if len(df_매수매도_종목) > 0:
                    df_매수매도_종목 = df_매수매도_종목.reset_index(drop=True)
                    df_매수매도_종목['검증'] = True
                    while True:
                        df_매수매도_종목 = df_매수매도_종목[df_매수매도_종목['검증']]
                        df_매수매도_종목['검증'] = (df_매수매도_종목['매수시간']
                                            > df_매수매도_종목['매도시간'].shift(1)) | (df_매수매도_종목.index == 0)
                        if sum(df_매수매도_종목['검증']) == len(df_매수매도_종목):
                            break
                # li_df 추가
                li_df_매수매도.append(df_매수매도_종목)

                # 차트 생성 및 저장
                if b_차트:
                    # 차트 생성
                    import UT_차트maker as chart
                    fig = chart.make_차트(df_ohlcv=df_3분봉_당일)
                    for n_지지저항 in df_지지저항_종목['고가'].values:
                        fig.axes[0].axhline(n_지지저항)

                    # 매수매도 표시 (매수는 ^, 매도는 v)
                    df_차트 = df_매수매도_종목.copy()
                    if len(df_차트) > 0:
                        df_차트['매도시간_3분봉'] = df_차트['매도시간'].apply(lambda x:
                                                            max(시간 for 시간 in df_3분봉['시간'].unique() if 시간 <= x))
                        df_차트['매수일시'] = df_차트['일자'].apply(lambda x:
                                                          f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매수시간']
                        df_차트['매도일시'] = df_차트['일자'].apply(lambda x:
                                                          f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_차트['매도시간_3분봉']
                        fig.axes[0].scatter(df_차트['매수일시'], df_차트['매수단가'], color='black', marker='^')
                        fig.axes[0].scatter(df_차트['매도일시'], df_차트['매도단가'], color='black', marker='v')

                    # 차트 저장
                    folder_그래프 = os.path.join(self.folder_매수매도, '그래프', f'매수매도_{s_일자}')
                    os.makedirs(folder_그래프, exist_ok=True)
                    fig.savefig(os.path.join(folder_그래프, f'매수매도_{s_종목코드}_{s_일자}.png'))

            # df_매수매도 생성
            df_매수매도 = pd.concat(li_df_매수매도, axis=0) if len(li_df_매수매도) > 0 else pd.DataFrame()
            if len(df_매수매도) == 0:
                s_파일명 = max(파일 for 파일 in os.listdir(self.folder_매수매도) if s_파일명_생성 in 파일 and '.pkl' in 파일)
                df_매수매도 = pd.read_pickle(os.path.join(self.folder_매수매도, s_파일명))[:0]

            # 매도 전 매수 케이스 제거
            df_매수매도 = df_매수매도.reset_index(drop=True)
            df_매수매도['검증'] = True
            while True:
                df_매수매도 = df_매수매도[df_매수매도['검증']]
                df_매수매도['검증'] = (df_매수매도['매수시간'] > df_매수매도['매도시간'].shift(1)) | (df_매수매도.index == 0)
                if sum(df_매수매도['검증']) == len(df_매수매도):
                    break

            # 누적 수익률 산출
            df_매수매도 = df_매수매도.loc[:, '일자': '수익률(%)']
            df_매수매도 = df_매수매도.sort_values('매수시간')
            df_매수매도['누적수익(%)'] = (1 + df_매수매도['수익률(%)'] / 100).cumprod() * 100

            # df 저장
            df_매수매도.to_pickle(os.path.join(self.folder_매수매도, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_매수매도.to_csv(os.path.join(self.folder_매수매도, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매수매도 검증 완료({s_일자}, {len(df_매수매도):,}건 매매)')

    def 검증_결과정리(self, b_카톡=False):
        """ 매수매도 검증 결과를 읽어와 정리 후 pkl, csv 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_매수매도'
        s_파일명_생성 = 'df_결과정리'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수매도)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_전체 = li_일자_전체[-1 * self.n_분석일수:] if self.n_분석일수 is not None else li_일자_전체
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_결과정리)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전체 매수매도 파일 확인
            li_파일일자 = [re.findall(r'\d{8}', 파일)[0] for 파일 in os.listdir(self.folder_매수매도)
                       if s_파일명_기준 in 파일 and '.pkl' in 파일]
            li_파일일자 = [파일일자 for 파일일자 in li_파일일자 if 파일일자 <= s_일자]

            # 파일별 결과 정리
            li_df_결과정리 = list()
            for s_파일일자 in tqdm(li_파일일자, desc=f'결과정리|{s_일자}'):
                # 파일 열기
                df_매수매도 = pd.read_pickle(os.path.join(self.folder_매수매도, f'{s_파일명_기준}_{s_파일일자}.pkl'))

                # 결과 정리
                df_결과정리_일별 = pd.DataFrame({'일자': [s_파일일자]})
                df_결과정리_일별['전체거래'] = int(len(df_매수매도))
                df_결과정리_일별['수익거래'] = int(len(df_매수매도[df_매수매도['수익률(%)'] > 0]))
                df_결과정리_일별['성공률(%)'] = (df_결과정리_일별['수익거래'] / df_결과정리_일별['전체거래']) * 100
                df_결과정리_일별['수익률(%)'] = df_매수매도['누적수익(%)'].values[-1] - 100 if len(df_매수매도) > 0 else None
                li_df_결과정리.append(df_결과정리_일별)

            # df_결과정리 생성
            df_결과정리 = pd.concat(li_df_결과정리, axis=0).sort_values('일자', ascending=False)

            # df 저장
            df_결과정리.to_pickle(os.path.join(self.folder_결과정리, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_결과정리.to_csv(os.path.join(self.folder_결과정리, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            n_수익률 = df_결과정리['수익률(%)'].values[0]
            s_수익률 = f'{n_수익률:.1f}%' if not pd.isna(n_수익률) else 'None'
            self.make_log(f'결과정리 완료({s_일자}, {int(df_결과정리["전체거래"].values[0]):,}건 매매,'
                          f' {int(df_결과정리["수익거래"].values[0]):,}건 성공,'
                          f' 수익률 {s_수익률})')

            # 백테스팅 리포트 생성
            folder_리포트 = os.path.join(self.folder_결과정리, '리포트')
            os.makedirs(folder_리포트, exist_ok=True)
            s_파일명_리포트 = f'백테스팅_리포트_{s_일자}.png'
            fig = self.make_리포트_백테스팅(df_결과정리=df_결과정리)
            fig.savefig(os.path.join(folder_리포트, s_파일명_리포트))
            plt.close(fig)

            # 리포트 복사 to 서버
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/sr분석_백테스팅'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡:
                import API_kakao
                k = API_kakao.KakaoAPI()
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 백테스팅 완료',
                                        s_button_title=f'[sr분석] 백테스팅 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'백테스팅 리포트 생성 완료({s_일자})')

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

    # noinspection PyTypeChecker
    def make_리포트_백테스팅(self, df_결과정리):
        """ 백테스팅 결과를 기반으로 daily 리포트 생성 후 fig 리턴 """
        # 데이터 설정
        df_결과정리 = df_결과정리.sort_values('일자')
        s_일자 = df_결과정리['일자'].max()

        # 그래프 설정
        fig = plt.figure(figsize=[16, 20])
        fig.suptitle(f'백테스팅 리포트 ({s_일자})', fontsize=16)
        ax_감시대상_일별 = fig.add_subplot(6, 2, 1)
        ax_상승예측_일별 = fig.add_subplot(6, 2, 2)
        ax_성공률_누적 = fig.add_subplot(6, 2, 3)
        ax_성공률_일별 = fig.add_subplot(6, 2, 4)
        ax_수익률_누적 = fig.add_subplot(6, 2, 5)
        ax_수익률_일별 = fig.add_subplot(6, 2, 6)
        ax_상세_학습정보_월별 = fig.add_subplot(6, 2, 7)
        ax_상세_학습정보_일별 = fig.add_subplot(6, 2, 8)
        ax_상세_백테스팅_월별 = fig.add_subplot(6, 2, 9)
        ax_상세_백테스팅_일별 = fig.add_subplot(6, 2, 10)
        ax_상세_매매실적_월별 = fig.add_subplot(6, 2, 11)
        ax_상세_매매실적_일별 = fig.add_subplot(6, 2, 12)

        # 일별 감시대상 건수
        li_파일명 = [파일명 for 파일명 in os.listdir(os.path.join(self.folder_종목선정))
                  if 'df_종목선정' in 파일명 and 'pkl' in 파일명 and 파일명 <= f'df_종목선정_{s_일자}.pkl']
        df_감시대상 = pd.DataFrame()
        df_감시대상['일자'] = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_파일명]
        df_감시대상['종목수'] = [len(pd.read_pickle(os.path.join(self.folder_종목선정, 파일명))) for 파일명 in li_파일명]
        df_감시대상 = df_감시대상.sort_values('일자')
        ary_x, ary_y = df_감시대상['일자'].values, df_감시대상['종목수'].values.astype(int)
        li_색깔 = ['C1' if 종목수 > 15 else 'C0' for 종목수 in ary_y]

        ax_감시대상_일별.set_title('[ 감시대상 종목수 (건, 일별) ]')
        ax_감시대상_일별.bar(ary_x, ary_y, color=li_색깔)
        ax_감시대상_일별.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_감시대상_일별.grid(linestyle='--', alpha=0.5)

        # 일별 상승예측 건수
        ary_x, ary_y = df_결과정리['일자'].values, df_결과정리['전체거래'].values.astype(int)
        li_색깔 = ['C3' if 예측건수 > 10 else 'C0' for 예측건수 in ary_y]

        ax_상승예측_일별.set_title(f'[ 상승예측 종목수 (건, 일별) ]')
        ax_상승예측_일별.bar(ary_x, ary_y, color=li_색깔)
        ax_상승예측_일별.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_상승예측_일별.set_yticks(range(0, max(ary_y) + 1, 1))
        ax_상승예측_일별.grid(linestyle='--', alpha=0.5)

        # 누적 예측 성공률
        df_결과정리['성공률_누적'] = df_결과정리['수익거래'].cumsum() / df_결과정리['전체거래'].cumsum() * 100
        ary_x, ary_y = df_결과정리['일자'].values, df_결과정리['성공률_누적'].values
        
        ax_성공률_누적.set_title(f'[ 성공률 (%, 누적, 일별) ]')
        ax_성공률_누적.plot(ary_x, ary_y)
        ax_성공률_누적.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_성공률_누적.set_yticks(range(0, 101, 20))
        ax_성공률_누적.grid(linestyle='--', alpha=0.5)
        ax_성공률_누적.axhline(100, color='C0', alpha=0)
        ax_성공률_누적.axhline(70, color='C1')

        # 일별 예측 성공률
        ary_x, ary_y = df_결과정리['일자'].values, df_결과정리['성공률(%)'].values
        li_색깔 = ['C0' if 성공률 > 70 else 'C3' for 성공률 in ary_y]

        ax_성공률_일별.set_title(f'[ 성공률 (%, 당일, 일별) ]')
        ax_성공률_일별.bar(ary_x, ary_y, color=li_색깔)
        ax_성공률_일별.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_성공률_일별.set_yticks(range(0, 101, 20))
        ax_성공률_일별.grid(linestyle='--', alpha=0.5)
        ax_성공률_일별.axhline(100, color='C0', alpha=0)
        ax_성공률_일별.axhline(70, color='C1')

        # 누적 수익률
        df_결과정리['수익률(%)'] = df_결과정리['수익률(%)'].apply(lambda x: 0 if pd.isna(x) else x)
        df_결과정리['수익률_누적'] = (1 + df_결과정리['수익률(%)'] / 100).cumprod() * 100
        ary_x, ary_y = df_결과정리['일자'].values, df_결과정리['수익률_누적'].values

        ax_수익률_누적.set_title(f'[ 수익률 (%, 누적, 일별, 100 기준) ]')
        ax_수익률_누적.plot(ary_x, ary_y)
        ax_수익률_누적.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_수익률_누적.set_yticks(range(40, 161, 20))
        ax_수익률_누적.grid(linestyle='--', alpha=0.5)
        ax_수익률_누적.axhline(160, color='C0', alpha=0)
        ax_수익률_누적.axhline(100, color='C1')
        ax_수익률_누적.axhline(40, color='C0', alpha=0)

        # 일별 수익률
        ary_x, ary_y = df_결과정리['일자'].values, df_결과정리['수익률(%)'].values
        li_색깔 = ['C0' if 수익률 > 0 else 'C3' for 수익률 in ary_y]

        ax_수익률_일별.set_title(f'[ 수익률 (%, 당일, 일별, 0 기준) ]')
        ax_수익률_일별.bar(ary_x, ary_y, color=li_색깔)
        ax_수익률_일별.set_xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        ax_수익률_일별.set_yticks(range(-10, 11, 2))
        ax_수익률_일별.grid(linestyle='--', alpha=0.5)
        ax_수익률_일별.axhline(10, color='C0', alpha=0)
        ax_수익률_일별.axhline(0, color='C1')
        ax_수익률_일별.axhline(-10, color='C0', alpha=0)

        # 월별 상세정보
        df_결과정리['년월'] = df_결과정리['일자'].apply(lambda x: f'{x[2:4]}-{x[4:6]}')
        df_gr = df_결과정리.groupby('년월')
        df_테이블_월별 = pd.DataFrame()
        df_테이블_월별['년월'] = df_gr['년월'].first()
        df_테이블_월별['매수(건)'] = df_gr['전체거래'].sum()
        df_테이블_월별['성공(건)'] = df_gr['수익거래'].sum()
        df_테이블_월별['성공률(%)'] = (df_테이블_월별['성공(건)'] / df_테이블_월별['매수(건)'] * 100).apply(lambda x: f'{x:.0f}')
        df_테이블_월별['수익률(%)'] = [((1 + df_gr.get_group(년월)['수익률(%)'] / 100).cumprod() * 100).values[-1]
                               for 년월 in df_gr.groups.keys()]
        df_테이블_월별['수익률(%)'] = df_테이블_월별['수익률(%)'].apply(lambda x: f'{x:.1f}')
        df_월별 = df_테이블_월별[-10:].T

        ax_상세_백테스팅_월별.set_title(f'[ 상세정보 - 백테스팅 (월별) ]')
        ax_상세_백테스팅_월별.axis('tight')
        ax_상세_백테스팅_월별.axis('off')
        obj_테이블 = ax_상세_백테스팅_월별.table(cellText=df_월별.values, rowLabels=df_월별.index, loc='center', cellLoc='center')
        obj_테이블.auto_set_font_size(False)
        obj_테이블.set_fontsize(12)
        obj_테이블.scale(1.0, 2.4)

        # 일별 상세정보
        df_테이블_일별 = pd.DataFrame()
        df_테이블_일별['월일'] = df_결과정리['일자'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        df_테이블_일별['매수(건)'] = df_결과정리['전체거래']
        df_테이블_일별['성공(건)'] = df_결과정리['수익거래']
        df_테이블_일별['성공률(%)'] = df_결과정리['성공률(%)'].apply(lambda x: f'{x:.0f}')
        df_테이블_일별['수익률(%)'] = df_결과정리['수익률(%)'].apply(lambda x: f'{x:.1f}')
        df_일별 = df_테이블_일별[-10:].T

        ax_상세_백테스팅_일별.set_title(f'[ 상세정보 - 백테스팅 (일별) ]')
        ax_상세_백테스팅_일별.axis('tight')
        ax_상세_백테스팅_일별.axis('off')
        obj_테이블 = ax_상세_백테스팅_일별.table(cellText=df_일별.values, rowLabels=df_일별.index, loc='center', cellLoc='center')
        obj_테이블.auto_set_font_size(False)
        obj_테이블.set_fontsize(12)
        obj_테이블.scale(1.0, 2.4)

        return fig


#######################################################################################################################
if __name__ == "__main__":
    a = Analyzer(n_분석일수=None)

    a.검증_매수매도(b_차트=False)
    a.검증_결과정리(b_카톡=False)

import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import analyzer_알고리즘 as Logic

# 그래프 한글 설정
import matplotlib.pyplot as plt
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
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_감시대상 = dic_폴더정보['분석1종목|50_종목_감시대상']
        self.folder_감시대상모델 = dic_폴더정보['분석1종목|60_종목_모델_감시대상']
        self.folder_종목상승예측 = dic_폴더정보['분석2공통|10_종목_상승예측']
        self.folder_종목수익검증 = dic_폴더정보['분석2공통|20_종목_수익검증']
        self.folder_공통데이터셋 = dic_폴더정보['분석2공통|30_공통_데이터셋']
        self.folder_공통모델 = dic_폴더정보['분석2공통|40_공통_모델']
        self.folder_공통성능평가 = dic_폴더정보['분석2공통|50_공통_성능평가']
        self.folder_공통수익검증 = dic_폴더정보['분석2공통|60_공통_수익검증']
        os.makedirs(self.folder_종목상승예측, exist_ok=True)
        os.makedirs(self.folder_종목수익검증, exist_ok=True)
        os.makedirs(self.folder_공통데이터셋, exist_ok=True)
        os.makedirs(self.folder_공통모델, exist_ok=True)
        os.makedirs(self.folder_공통성능평가, exist_ok=True)
        os.makedirs(self.folder_공통수익검증, exist_ok=True)

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 공통 분석 시작 ###')

    def 종목분석_상승예측(self, s_모델):
        """ 감시대상 종목 불러와서 10분봉 기준 상승여부 예측 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
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
                dic_조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
                li_감시대상 = list(df_감시대상['종목코드'].values)
            except KeyError:
                dic_조건 = dict()
                li_감시대상 = list()

            # 모델 불러오기 (전일 기준 - 전일 생성한 모델 불러와서 당일 적용)
            dic_모델 = pd.read_pickle(os.path.join(self.folder_감시대상모델, f'dic_모델_감시대상_{s_모델}_{s_일자_전일}.pkl'))

            # 10분봉 불러오기 (당일 기준)
            dic_df_10분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_df_10분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))

            # 종목별 상승 예측값 생성
            dic_df_상승예측 = dict()
            for s_종목코드 in tqdm(li_감시대상, desc=f'{s_모델} 상승 예측값 생성({s_일자})'):
                # 조건 설정
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이, n_예측성공, n_확률스펙 = dic_조건[s_종목코드]
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = int(n_대기봉수), int(n_학습일수), int(n_rf_트리), int(n_rf_깊이)
                s_케이스 = f'{n_대기봉수}_{n_학습일수}_{n_rf_트리}_{n_rf_깊이}'

                # 모델 설정 (데이터 부족 등 이유로 모델 미생성된 종목은 예외처리)
                try:
                    obj_모델 = dic_모델[s_종목코드][s_케이스]
                except KeyError:
                    continue

                # 데이터셋 준비 (추가 데이터 생성, 라벨 생성, 당일만 잘라내기, 입력용 ary로 변환)
                df_10분봉 = pd.concat([dic_df_10분봉_전일[s_종목코드], dic_df_10분봉_당일[s_종목코드]], axis=0).sort_index()
                df_데이터셋 = Logic.trd_make_추가데이터_종목모델_rf(df=df_10분봉)
                df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] == s_일자]
                if len(df_데이터셋) == 0 or len(df_데이터셋) != len(df_10분봉[df_10분봉['일자'] == s_일자]):
                    continue
                dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=1)
                ary_x_평가 = dic_데이터셋['ary_x_학습']
                ary_y_정답 = dic_데이터셋['ary_y_학습']

                # 상승확률 및 상승예측 생성 (10분봉 데이터를 당일 데이터만 잘라낸 후, 상승확률 및 상승예측 추가)
                df_10분봉 = df_10분봉[df_10분봉['일자'] == s_일자]
                try:
                    df_10분봉['상승확률(%)'] = obj_모델.predict_proba(ary_x_평가)[:, 1] * 100
                except IndexError:
                    df_10분봉['상승확률(%)'] = 0
                df_10분봉['상승예측'] = (df_10분봉['상승확률(%)'] > n_확률스펙) * 1
                df_10분봉['정답'] = ary_y_정답

                # 케이스 정보 추가
                df_10분봉['케이스'] = s_케이스

                # 평가 정보 추가
                df_10분봉['예측성공'] = int(n_예측성공)
                df_10분봉['확률스펙'] = n_확률스펙

                # dic_df에 입력
                dic_df_상승예측[s_종목코드] = df_10분봉

            # 상승예측 결과 하나의 df로 합치기
            li_df = [pd.DataFrame()] + [dic_df_상승예측[종목코드] for 종목코드 in dic_df_상승예측.keys()]
            df_상승예측 = pd.concat(li_df, axis=0)

            # 상승예측 없을 시 빈 df로 설정
            if len(df_상승예측) == 0:
                li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_종목상승예측)
                          if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
                s_참고파일 = max(li_파일명)
                df_상승예측 = pd.read_pickle(os.path.join(self.folder_종목상승예측, s_참고파일))
                df_상승예측 = df_상승예측.drop(df_상승예측.index)
                df_상승예측.loc[0] = None

            # 결과 저장
            df_상승예측.to_pickle(os.path.join(self.folder_종목상승예측, f'df_상승예측_{s_모델}_{s_일자}.pkl'))
            df_상승예측.to_csv(os.path.join(self.folder_종목상승예측, f'상승예측_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목별 상승예측 완료({s_일자}, {len(dic_df_상승예측):,}개 종목, {s_모델})')

    def 종목분석_수익검증(self, s_모델, b_카톡=False):
        """ 상승여부 예측한 결과를 바탕으로 종목선정 조건에 따른 결과 확인 (예측 엑셀, 리포트 저장) """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목수익검증)
                    if f'df_수익검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 상승예측 데이터 불러오기
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_종목상승예측)
                      if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            li_파일명 = [파일명 for 파일명 in li_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]

            # 예측한 값만 잘라내기 (예측값 없을 시 해당 날짜는 None 반환)
            li_df_수익검증 = list()
            for s_파일명 in li_파일명:
                df_상승예측_일별 = pd.read_pickle(os.path.join(self.folder_종목상승예측, s_파일명))
                df_상승예측_일별['상승예측'] = (df_상승예측_일별['상승확률(%)'] >= 50) * 1
                df_수익검증_일별 = df_상승예측_일별[df_상승예측_일별['상승예측'] == 1].drop_duplicates()
                if len(df_수익검증_일별) == 0:
                    df_수익검증_일별.loc[0] = None
                    df_수익검증_일별['일자'] = re.findall(r'\d{8}', s_파일명)[0]
                    df_수익검증_일별.index = df_수익검증_일별['일자'].astype('datetime64[ns]')
                li_df_수익검증.append(df_수익검증_일별)
            df_수익검증 = pd.concat(li_df_수익검증, axis=0)

            # 예측 엑셀 저장 (pkl 포함)
            df_수익검증.to_pickle(os.path.join(self.folder_종목수익검증, f'df_수익검증_{s_모델}_{s_일자}.pkl'))
            df_수익검증.to_csv(os.path.join(self.folder_종목수익검증, f'수익검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목별 수익검증 완료({s_일자}, {s_모델})')

            # 감시대상 종목 정보 생성
            li_감시대상_파일명 = [파일명 for 파일명 in os.listdir(self.folder_감시대상)
                           if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            s_시작일자 = min([re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_파일명])
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] >= s_시작일자]
            df_감시대상 = pd.DataFrame()
            df_감시대상['일자'] = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_감시대상_파일명]
            df_감시대상['종목수'] = [len(pd.read_pickle(os.path.join(self.folder_감시대상, 파일명)))
                              for 파일명 in li_감시대상_파일명]

            # 리포트 생성
            folder_리포트 = self.folder_종목수익검증
            s_파일명_리포트 = f'수익검증_리포트_{s_모델}_{s_일자}.png'
            self.make_리포트_종목(df_감시대상=df_감시대상, df_수익검증=df_수익검증)
            plt.savefig(os.path.join(folder_리포트, s_파일명_리포트))
            plt.close()

            # 리포트 복사 to 서버
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/수익검증_종목'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡:
                import API_kakao
                k = API_kakao.KakaoAPI()
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 종목분석검증 완료',
                                        s_button_title=f'[종목] 수익검증 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'종목모델 수익검증 리포트 생성 완료({s_일자}, {s_모델})')

    def 공통분석_데이터셋(self, s_모델, b_이전데이터수집=False):
        """ 종목분석 수익검증 결과를 바탕으로 공통 분석을 위한 데이터셋 df 생성 후 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_종목수익검증)
                    if f'df_수익검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통데이터셋)
                    if f'df_데이터셋_전체_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자 선정 보완 (이전 데이터 수집용)
        if b_이전데이터수집:
            s_최소일자_수익검증 = min(li_일자_전체)
            df_수익검증 = pd.read_pickle(
                os.path.join(self.folder_종목수익검증, f'df_수익검증_{s_모델}_{s_최소일자_수익검증}.pkl'))
            li_전체일자_대상 = list(
                pd.concat([df_수익검증['일자'], pd.Series(li_일자_전체)]).drop_duplicates().sort_values())

            if len(li_일자_완료) == 0:
                li_전체일자_완료 = list()
            else:
                s_최소일자_데이터셋 = min(li_일자_완료)
                df_데이터셋 = pd.read_pickle(
                    os.path.join(self.folder_공통데이터셋, f'df_데이터셋_전체_{s_모델}_{s_최소일자_데이터셋}.pkl'))
                li_전체일자_완료 = list(
                    pd.concat([df_데이터셋['일자'], pd.Series(li_일자_완료)]).drop_duplicates().sort_values())

            li_일자_전체 = li_전체일자_대상
            li_일자_완료 = li_전체일자_완료
            li_일자_대상 = [s_일자 for s_일자 in li_전체일자_대상 if s_일자 not in li_전체일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 수익검증 파일 불러오기
            try:
                df_수익검증 = pd.read_pickle(os.path.join(self.folder_종목수익검증, f'df_수익검증_rf_{s_일자}.pkl'))
            except FileNotFoundError:
                li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_종목수익검증)
                          if f'df_수익검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
                s_파일명 = min(li_파일명)
                df_수익검증 = pd.read_pickle(os.path.join(self.folder_종목수익검증, s_파일명))
            df_수익검증_일별 = df_수익검증[df_수익검증['일자'] == s_일자].copy()

            # 10분봉 불러오기
            dic_df_10분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_df_10분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))

            # 일별 데이터셋 생성
            li_df_데이터셋 = list()
            for ary_수익검증 in df_수익검증_일별.values:
                [s_검증일자, s_종목코드, s_종목명, s_시간, n_시가, n_고가, n_저가, n_종가, n_거래량, n_전일종가, n_전일대비,
                 n_종가ma5, n_종가ma10, n_종가ma20, n_종가ma60, n_종가ma120,
                 n_거래량ma5, n_거래량ma20, n_거래량ma60, n_거래량ma120,
                 n_상승확률, n_상승예측, n_정답, n_케이스, n_예측성공, n_확률스펙] = ary_수익검증

                # 10분봉 확인 (이전 봉, tr 동일)
                li_df_10분봉 = [dic_df_10분봉_당일[s_종목코드], dic_df_10분봉_전일[s_종목코드]]
                df_10분봉 = pd.concat(li_df_10분봉, axis=0).drop_duplicates().sort_index(ascending=True)
                dt_시점 = pd.Timestamp(f'{s_검증일자} {s_시간}')
                df_10분봉_tr = df_10분봉[df_10분봉.index < dt_시점].sort_index(ascending=False)

                # 데이터 변환
                dic_전일종가 = df_10분봉.set_index('일자').to_dict()['전일종가']
                df_10분봉_ma = Logic.trd_make_이동평균_분봉(df_분봉=df_10분봉_tr, dic_전일종가=dic_전일종가)
                df_데이터셋_공통 = Logic.trd_make_추가데이터_공통모델_rf(df=df_10분봉_ma,
                                                          n_상승확률_종목=n_상승확률, n_확률스펙=n_확률스펙)
                df_데이터셋_공통['정답'] = int(n_정답)
                li_df_데이터셋.append(df_데이터셋_공통)

            if len(li_df_데이터셋) == 0:
                df_데이터셋_일별 = pd.DataFrame()
            else:
                df_데이터셋_일별 = pd.concat(li_df_데이터셋, axis=0).drop_duplicates().sort_index(ascending=True)

            # 전체 데이터셋 생성 (일별 데이터셋 합치기)
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_공통데이터셋)
                      if f'df_데이터셋_전체_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            if len(li_파일명) == 0:
                df_데이터셋_전체_기존 = pd.DataFrame()
            else:
                s_파일명_기존 = max(li_파일명)
                df_데이터셋_전체_기존 = pd.read_pickle(os.path.join(self.folder_공통데이터셋, s_파일명_기존))
            df_데이터셋_전체 = pd.concat([df_데이터셋_전체_기존, df_데이터셋_일별], axis=0).drop_duplicates()
            df_데이터셋_전체 = df_데이터셋_전체.sort_index(ascending=True)

            # 전체 데이터 기간 제한 (rotator 동일 기간)
            with open('config.json', mode='rt', encoding='utf-8') as file:
                dic_config = json.load(file)
            n_보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])
            dt_기준일자 = pd.Timestamp(s_일자) - pd.DateOffset(days=n_보관기간_analyzer + 1)
            df_데이터셋_전체 = df_데이터셋_전체[df_데이터셋_전체.index >= dt_기준일자]

            # 데이터셋 저장
            pd.to_pickle(df_데이터셋_일별, os.path.join(self.folder_공통데이터셋, f'df_데이터셋_일별_{s_모델}_{s_일자}.pkl'))
            df_데이터셋_일별.to_csv(os.path.join(self.folder_공통데이터셋, f'df_데이터셋_일별_{s_모델}_{s_일자}.csv'),
                              index=False, encoding='cp949')
            pd.to_pickle(df_데이터셋_전체, os.path.join(self.folder_공통데이터셋, f'df_데이터셋_전체_{s_모델}_{s_일자}.pkl'))
            df_데이터셋_전체.to_csv(os.path.join(self.folder_공통데이터셋, f'df_데이터셋_전체_{s_모델}_{s_일자}.csv'),
                              index=False, encoding='cp949')

            # log 기록
            self.make_log(f'데이터셋 준비 완료({s_일자}, {s_모델})')

    def 공통분석_모델생성(self, s_모델):
        """ 데이터셋을 바탕으로 일자별 공통분석 모델 생성 후 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통데이터셋)
                    if f'df_데이터셋_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통모델)
                    if f'obj_공통모델_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 데이터셋 불러오기
            df_데이터 = pd.read_pickle(os.path.join(self.folder_공통데이터셋, f'df_데이터셋_{s_모델}_{s_일자}.pkl'))
            df_데이터 = df_데이터.dropna()

            # 공통모델 생성
            if s_모델 == 'rf':
                # 입력용 xy 생성
                s_라벨 = '정답'
                li_인자 = [컬럼명 for 컬럼명 in df_데이터.columns if 컬럼명 not in ['일자', '종목코드', '종목명', '시간', s_라벨]]
                ary_x_학습 = df_데이터.loc[:, li_인자].values
                ary_y_학습 = df_데이터[s_라벨].values

                dic_데이터셋 = dict()
                dic_데이터셋['ary_x_학습'] = ary_x_학습
                dic_데이터셋['ary_y_학습'] = ary_y_학습

                # 모델 생성
                obj_모델 = Logic.make_모델_rf(dic_데이터셋=dic_데이터셋, n_rf_트리=100, n_rf_깊이=10)

                # 모델 저장
                pd.to_pickle(obj_모델, os.path.join(self.folder_공통모델, f'obj_공통모델_{s_모델}_{s_일자}.pkl'))
                self.make_log(f'공통모델 생성 완료({s_일자}, {s_모델})')

    def 공통분석_성능평가(self, s_모델):
        """ 전일 생성된 종목 및 공통 모델을 기반으로 금일 데이터로 예측 결과 확인하여 평가결과 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통모델)
                    if f'obj_공통모델_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통성능평가)
                    if f'df_성능평가_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 데이터셋 및 모델 불러오기 (전일 모델로 당일 데이터 검증)
            df_데이터셋 = pd.read_pickle(os.path.join(self.folder_공통데이터셋, f'df_데이터셋_{s_모델}_{s_일자}.pkl'))
            obj_공통모델_전일 = pd.read_pickle(os.path.join(self.folder_공통모델, f'obj_공통모델_{s_모델}_{s_일자_전일}.pkl'))
            df_데이터셋_당일 = df_데이터셋[df_데이터셋['일자'] == s_일자]

            # 입력용 xy 생성
            s_라벨 = '정답'
            li_인자 = [컬럼 for 컬럼 in df_데이터셋_당일.columns if 컬럼 not in ['일자', '종목코드', '종목명', '시간', s_라벨]]
            ary_x_평가 = df_데이터셋_당일.loc[:, li_인자].values
            ary_y_정답 = df_데이터셋_당일[s_라벨].values

            # 상승확률 산출 (확률 0일 시 IndexError 처리, 당일 데이터 미존재 시 ValueError 처리)
            df_성능평가 = df_데이터셋_당일.copy()
            s_col_확률 = f'공통확률(%)'
            try:
                df_성능평가[s_col_확률] = obj_공통모델_전일.predict_proba(ary_x_평가)[:, 1] * 100
            except IndexError:
                df_성능평가[s_col_확률] = 0
            except ValueError:
                df_성능평가[s_col_확률] = None

            # 예측결과 입력 (50% 초과)
            df_성능평가['예측'] = (df_성능평가[s_col_확률] > 50) * 1

            # 컬럼 재정리
            li_컬럼명 = [컬럼 for 컬럼 in df_성능평가.columns if 컬럼 not in ['정답']] + ['정답']
            df_성능평가 = df_성능평가.loc[:, li_컬럼명]

            # 평가 결과 저장
            df_성능평가.to_pickle(os.path.join(self.folder_공통성능평가, f'df_성능평가_{s_모델}_{s_일자}.pkl'))
            df_성능평가.to_csv(os.path.join(self.folder_공통성능평가, f'df_성능평가_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'공통모델 성능평가 완료({s_일자}, {s_모델})')

    def 공통분석_수익검증(self, s_모델, b_카톡=False):
        """ 공통모델 사용하여 예측한 결과를 바탕으로 수익관점 결과 확인 (엑셀, 리포트 저장) """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통성능평가)
                    if f'df_성능평가_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_공통수익검증)
                    if f'df_공통수익검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 상승예측 데이터 불러오기
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_공통성능평가)
                      if f'df_성능평가_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            li_파일명 = [파일명 for 파일명 in li_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]

            # 예측한 값만 잘라내기 (예측값 없을 시 해당 날짜는 None 반환)
            li_df_수익검증 = list()
            for s_파일명 in li_파일명:
                df_상승예측_일별 = pd.read_pickle(os.path.join(self.folder_공통성능평가, s_파일명))
                df_상승예측_일별['예측'] = (df_상승예측_일별['공통확률(%)'] >= 50) * 1
                df_수익검증_일별 = df_상승예측_일별[df_상승예측_일별['예측'] == 1].drop_duplicates()
                if len(df_수익검증_일별) == 0:
                    df_수익검증_일별.loc[0] = None
                    df_수익검증_일별['일자'] = re.findall(r'\d{8}', s_파일명)[0]
                    df_수익검증_일별.index = df_수익검증_일별['일자'].astype('datetime64[ns]')
                li_df_수익검증.append(df_수익검증_일별)
            df_수익검증 = pd.concat(li_df_수익검증, axis=0)

            # 예측 엑셀 저장 (pkl 포함)
            df_수익검증.to_pickle(os.path.join(self.folder_공통수익검증, f'df_공통수익검증_{s_모델}_{s_일자}.pkl'))
            df_수익검증.to_csv(os.path.join(self.folder_공통수익검증, f'df_공통수익검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'공통모델 수익검증 완료({s_일자}, {s_모델})')

            # 감시대상 종목 정보 생성
            li_감시대상_파일명 = [파일명 for 파일명 in os.listdir(self.folder_감시대상)
                           if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            s_시작일자 = min([re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_파일명])
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] >= s_시작일자]
            df_감시대상 = pd.DataFrame()
            df_감시대상['일자'] = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_감시대상_파일명]
            df_감시대상['종목수'] = [len(pd.read_pickle(os.path.join(self.folder_감시대상, 파일명)))
                              for 파일명 in li_감시대상_파일명]

            # 종목모델 수익검증 결과 불러오기
            df_수익검증_종목 = pd.read_pickle(os.path.join(self.folder_종목수익검증, f'df_수익검증_rf_{s_일자}.pkl'))

            # 리포트 생성
            folder_리포트 = self.folder_공통수익검증
            s_파일명_리포트 = f'수익검증_리포트_{s_모델}_{s_일자}.png'
            self.make_리포트_공통(df_감시대상=df_감시대상, df_수익검증_종목=df_수익검증_종목, df_수익검증_공통=df_수익검증)
            plt.savefig(os.path.join(folder_리포트, s_파일명_리포트))
            plt.close()

            # 리포트 복사 to 서버
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/수익검증_공통'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡:
                import API_kakao
                k = API_kakao.KakaoAPI()
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 공통분석검증 완료',
                                        s_button_title=f'[공통] 수익검증 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'공통모델 수익검증 리포트 생성 완료({s_일자}, {s_모델})')

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

    @staticmethod
    def make_리포트_종목(df_감시대상, df_수익검증):
        """ 수익검증 데이터를 기반으로 daily 리포트 생성 및 png 파일로 저장 """
        # 그래프 설정
        plt.figure(figsize=[16, 9])

        # 일별 감시대상 종목수
        plt.subplot(2, 2, 1)
        plt.title('[ 감시대상 종목수 ]')
        ary_x, ary_y = df_감시대상['일자'].values, df_감시대상['종목수'].values
        li_색깔 = ['C1' if 종목수 > 15 else 'C0' for 종목수 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.grid(linestyle='--', alpha=0.5)

        # 일별 상승예측건수
        plt.subplot(2, 2, 2)
        plt.title('[ 상승예측 건수 ]')
        sri_상승예측건수 = df_수익검증.groupby('일자')['상승예측'].sum()
        ary_x, ary_y = sri_상승예측건수.index.values, sri_상승예측건수.values
        li_색깔 = ['C3' if 예측건수 > 10 else 'C0' for 예측건수 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.grid(linestyle='--', alpha=0.5)

        # 일별 예측성공률
        plt.subplot(2, 2, 4)
        plt.title('[ 예측 성공률 (%) ]')
        sri_예측성공건수 = df_수익검증.groupby('일자')['정답'].sum()
        sri_예측성공률 = sri_예측성공건수 / sri_상승예측건수 * 100
        ary_x, ary_y = sri_예측성공률.index.values, sri_예측성공률.values
        li_색깔 = ['C0' if 성공률 > 70 else 'C3' for 성공률 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.grid(linestyle='--', alpha=0.5)
        plt.axhline(100, color='C0', alpha=0)
        plt.axhline(70, color='C1')

        # 누적 예측성공률
        plt.subplot(2, 2, 3)
        plt.title('[ 예측 성공률 (%, 누적) ]')
        sri_예측성공률_누적 = sri_예측성공건수.cumsum() / sri_상승예측건수.cumsum() * 100
        ary_x, ary_y = sri_예측성공률_누적.index.values, sri_예측성공률_누적.values
        plt.plot(ary_x, ary_y)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.grid(linestyle='--', alpha=0.5)
        plt.axhline(100, color='C0', alpha=0)
        plt.axhline(70, color='C1')

    @staticmethod
    def make_리포트_공통(df_감시대상, df_수익검증_종목, df_수익검증_공통):
        """ 수익검증 데이터를 기반으로 daily 리포트 생성 및 png 파일로 저장 """
        # 월별 데이터 생성 - 공통검증
        df_수익검증_공통['년월'] = [f'{년월일[2:4]}-{년월일[4:6]}' for 년월일 in df_수익검증_공통['일자']]
        sri_년월 = df_수익검증_공통.groupby('년월')['년월'].first()

        dic_df_월별테이블_공통 = dict()
        for n_확률스펙 in [50, 55, 60]:
            li_월별테이블 = [sri_년월]
            df_수익검증_공통_확률스펙 = df_수익검증_공통[(df_수익검증_공통['공통확률(%)'].isna())
                                         | (df_수익검증_공통['공통확률(%)'] >= n_확률스펙)]
            li_월별테이블.append(df_수익검증_공통_확률스펙.groupby('년월')['예측'].sum())
            li_월별테이블.append(df_수익검증_공통_확률스펙.groupby('년월')['정답'].sum())
            df_월별테이블 = pd.concat(li_월별테이블, axis=1)
            df_월별테이블['성공률'] = df_월별테이블['정답'] / df_월별테이블['예측'] * 100
            df_월별테이블['기대수익'] = df_월별테이블['정답'] * 2.5 - (df_월별테이블['예측'] - df_월별테이블['정답']) * 3.5
            for 컬럼명 in ['예측', '정답', '성공률']:
                df_월별테이블[컬럼명] = df_월별테이블[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
            df_월별테이블['기대수익'] = df_월별테이블['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
            df_월별테이블_T = df_월별테이블[-8:].T
            df_월별테이블_T.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
            dic_df_월별테이블_공통[f'{n_확률스펙}퍼'] = df_월별테이블
            dic_df_월별테이블_공통[f'{n_확률스펙}퍼T'] = df_월별테이블_T

        # 월별 데이터 생성 - 종목검증
        df_수익검증_종목['년월'] = [f'{년월일[2:4]}-{년월일[4:6]}' for 년월일 in df_수익검증_종목['일자']]
        sri_년월 = df_수익검증_종목.groupby('년월')['년월'].first()

        dic_df_월별테이블_종목 = dict()
        for n_확률스펙 in [50, 55, 60]:
            li_월별테이블 = [sri_년월]
            df_수익검증_종목_확률스펙 = df_수익검증_종목[(df_수익검증_종목['상승확률(%)'].isna())
                                         | (df_수익검증_종목['상승확률(%)'] >= n_확률스펙)]
            li_월별테이블.append(df_수익검증_종목_확률스펙.groupby('년월')['상승예측'].sum())
            li_월별테이블.append(df_수익검증_종목_확률스펙.groupby('년월')['정답'].sum())
            df_월별테이블 = pd.concat(li_월별테이블, axis=1)
            df_월별테이블['성공률'] = df_월별테이블['정답'] / df_월별테이블['상승예측'] * 100
            df_월별테이블['기대수익'] = df_월별테이블['정답'] * 2.5 - (df_월별테이블['상승예측'] - df_월별테이블['정답']) * 3.5
            for 컬럼명 in ['상승예측', '정답', '성공률']:
                df_월별테이블[컬럼명] = df_월별테이블[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
            df_월별테이블['기대수익'] = df_월별테이블['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
            df_월별테이블_T = df_월별테이블[-8:].T
            df_월별테이블_T.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
            dic_df_월별테이블_종목[f'{n_확률스펙}퍼'] = df_월별테이블
            dic_df_월별테이블_종목[f'{n_확률스펙}퍼T'] = df_월별테이블_T

        # 일별 데이터 생성 - 공통검증
        df_수익검증_공통['년월일'] = df_수익검증_공통['일자']
        sri_일자 = df_수익검증_공통.groupby('년월일')['일자'].first()

        dic_df_일별테이블_공통 = dict()
        for n_확률스펙 in [50, 55, 60]:
            li_일별테이블 = [sri_일자]
            df_수익검증_공통_확률스펙 = df_수익검증_공통[(df_수익검증_공통['공통확률(%)'].isna())
                                         | (df_수익검증_공통['공통확률(%)'] >= n_확률스펙)]
            li_일별테이블.append(df_수익검증_공통_확률스펙.groupby('년월일')['예측'].sum())
            li_일별테이블.append(df_수익검증_공통_확률스펙.groupby('년월일')['정답'].sum())
            df_일별테이블 = pd.concat(li_일별테이블, axis=1)
            df_일별테이블['일자'] = df_일별테이블['일자'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
            df_일별테이블['성공률'] = df_일별테이블['정답'] / df_일별테이블['예측'] * 100
            df_일별테이블['기대수익'] = df_일별테이블['정답'] * 2.5 - (df_일별테이블['예측'] - df_일별테이블['정답']) * 3.5
            for 컬럼명 in ['예측', '정답', '성공률']:
                df_일별테이블[컬럼명] = df_일별테이블[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
            df_일별테이블['기대수익'] = df_일별테이블['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
            df_일별테이블_T = df_일별테이블[-10:].T
            df_일별테이블_T.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
            dic_df_일별테이블_공통[f'{n_확률스펙}퍼'] = df_일별테이블
            dic_df_일별테이블_공통[f'{n_확률스펙}퍼T'] = df_일별테이블_T

        # 일별 데이터 생성 - 종목검증
        df_수익검증_종목['년월일'] = df_수익검증_종목['일자']
        sri_일자 = df_수익검증_종목.groupby('년월일')['일자'].first()

        dic_df_일별테이블_종목 = dict()
        for n_확률스펙 in [50, 55, 60]:
            li_일별테이블 = [sri_일자]
            df_수익검증_종목_확률스펙 = df_수익검증_종목[(df_수익검증_종목['상승확률(%)'].isna())
                                         | (df_수익검증_종목['상승확률(%)'] >= n_확률스펙)]
            li_일별테이블.append(df_수익검증_종목_확률스펙.groupby('년월일')['상승예측'].sum())
            li_일별테이블.append(df_수익검증_종목_확률스펙.groupby('년월일')['정답'].sum())
            df_일별테이블 = pd.concat(li_일별테이블, axis=1)
            df_일별테이블['일자'] = df_일별테이블['일자'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
            df_일별테이블['성공률'] = df_일별테이블['정답'] / df_일별테이블['상승예측'] * 100
            df_일별테이블['기대수익'] = df_일별테이블['정답'] * 2.5 - (df_일별테이블['상승예측'] - df_일별테이블['정답']) * 3.5
            for 컬럼명 in ['상승예측', '정답', '성공률']:
                df_일별테이블[컬럼명] = df_일별테이블[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
            df_일별테이블['기대수익'] = df_일별테이블['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
            df_일별테이블_T = df_일별테이블[-10:].T
            df_일별테이블_T.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
            dic_df_일별테이블_종목[f'{n_확률스펙}퍼'] = df_일별테이블
            dic_df_일별테이블_종목[f'{n_확률스펙}퍼T'] = df_일별테이블_T

        # 그래프 표시할 확률스펙 설정
        n_확률스펙 = 55
        df_수익검증_공통_확률스펙 = df_수익검증_공통[(df_수익검증_공통['공통확률(%)'].isna())
                                     | (df_수익검증_공통['공통확률(%)'] >= n_확률스펙)]

        # 그래프 설정
        plt.figure(figsize=[16, 20])

        # 일별 감시대상 종목수
        plt.subplot(6, 2, 1)
        plt.title('[ 감시대상 종목수 ]')
        ary_x, ary_y = df_감시대상['일자'].values, df_감시대상['종목수'].values.astype(int)
        li_색깔 = ['C1' if 종목수 > 15 else 'C0' for 종목수 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.grid(linestyle='--', alpha=0.5)

        # 일별 상승예측건수
        plt.subplot(6, 2, 2)
        plt.title(f'[ 상승예측 건수 (확률스펙 {n_확률스펙}%) ]')
        sri_상승예측건수 = df_수익검증_공통_확률스펙.groupby('년월일')['예측'].sum()
        ary_x, ary_y = sri_상승예측건수.index.values, sri_상승예측건수.values.astype(int)
        li_색깔 = ['C3' if 예측건수 > 10 else 'C0' for 예측건수 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.yticks(range(0, max(ary_y) + 1, 1))
        plt.grid(linestyle='--', alpha=0.5)

        # 일별 예측성공률
        plt.subplot(6, 2, 4)
        plt.title(f'[ 예측 성공률 (%, 확률스펙 {n_확률스펙}%) ]')
        sri_예측성공건수 = df_수익검증_공통_확률스펙.groupby('년월일')['정답'].sum()
        sri_예측성공률 = sri_예측성공건수 / sri_상승예측건수 * 100
        ary_x, ary_y = sri_예측성공률.index.values, sri_예측성공률.values
        li_색깔 = ['C0' if 성공률 > 70 else 'C3' for 성공률 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.yticks(range(0, 101, 20))
        plt.grid(linestyle='--', alpha=0.5)
        plt.axhline(100, color='C0', alpha=0)
        plt.axhline(70, color='C1')

        # 누적 예측성공률
        plt.subplot(6, 2, 3)
        plt.title(f'[ 예측 성공률 (%, 누적, 확률스펙 {n_확률스펙}%) ]')
        sri_예측성공률_누적 = sri_예측성공건수.cumsum() / sri_상승예측건수.cumsum() * 100
        ary_x, ary_y = sri_예측성공률_누적.index.values, sri_예측성공률_누적.values
        plt.plot(ary_x, ary_y)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.yticks(range(0, 101, 20))
        plt.grid(linestyle='--', alpha=0.5)
        plt.axhline(100, color='C0', alpha=0)
        plt.axhline(70, color='C1')

        # 월별 성공률 - 종목검증
        plt.subplot(6, 2, 5)
        n_확률스펙 = 50
        plt.title(f'[ 월별 성공률 - 종목 (확률스펙 {n_확률스펙}%) ]')
        df = dic_df_월별테이블_종목[f'{n_확률스펙}퍼T']
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 일별 성공률 - 종목검증
        plt.subplot(6, 2, 6)
        n_확률스펙 = 50
        plt.title(f'[ 일별 성공률 - 종목 (확률스펙 {n_확률스펙}%) ]')
        df = dic_df_일별테이블_종목[f'{n_확률스펙}퍼T']
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 월별 성공률 - 공통검증
        for i, n_확률스펙 in enumerate([50, 55, 60]):
            plt.subplot(6, 2, 7 + 2 * i)
            plt.title(f'[ 월별 성공률 - 공통 (확률스펙 {n_확률스펙}%) ]')
            df = dic_df_월별테이블_공통[f'{n_확률스펙}퍼T']
            plt.axis('tight')
            plt.axis('off')
            테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
            테이블.auto_set_font_size(False)
            테이블.set_fontsize(12)
            테이블.scale(1.0, 2.4)

        # 일별 성공률 - 공통검증
        for i, n_확률스펙 in enumerate([50, 55, 60]):
            plt.subplot(6, 2, 8 + 2 * i)
            plt.title(f'[ 일별 성공률 - 공통 (확률스펙 {n_확률스펙}%) ]')
            df = dic_df_일별테이블_공통[f'{n_확률스펙}퍼T']
            plt.axis('tight')
            plt.axis('off')
            테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
            테이블.auto_set_font_size(False)
            테이블.set_fontsize(12)
            테이블.scale(1.0, 2.4)


#######################################################################################################################
if __name__ == "__main__":
    a = Analyzer()

    a.종목분석_상승예측(s_모델='rf')
    a.종목분석_수익검증(s_모델='rf', b_카톡=False)
    a.공통분석_데이터셋(s_모델='rf', b_이전데이터수집=True)
    a.공통분석_모델생성(s_모델='rf')
    a.공통분석_성능평가(s_모델='rf')
    a.공통분석_수익검증(s_모델='rf', b_카톡=True)

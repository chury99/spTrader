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
        self.folder_종목모델 = dic_폴더정보['분석1종목|60_종목_모델_감시대상']
        self.folder_수익검증_종목 = dic_폴더정보['분석2공통|20_종목_수익검증']
        self.folder_공통모델 = dic_폴더정보['분석2공통|40_공통_모델']
        self.folder_수익검증_공통 = dic_폴더정보['분석2공통|60_공통_수익검증']
        self.folder_데이터준비 = dic_폴더정보['백테스팅|10_데이터준비']
        self.folder_매수검증 = dic_폴더정보['백테스팅|20_매수검증']
        self.folder_매도검증 = dic_폴더정보['백테스팅|30_매도검증']
        self.folder_수익검증 = dic_폴더정보['백테스팅|40_수익검증']
        os.makedirs(self.folder_데이터준비, exist_ok=True)
        os.makedirs(self.folder_매수검증, exist_ok=True)
        os.makedirs(self.folder_매도검증, exist_ok=True)
        os.makedirs(self.folder_수익검증, exist_ok=True)

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 백테스팅 시작 ###')

    def 백테스팅_데이터준비(self, s_모델):
        """ 감시대상 종목 불러와서 종목조건, 분봉, 10분봉 데이터 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_데이터준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 감시대상 불러오기 (당일 기준 - 매수검증 시 전일 데이터 사용)
            df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_모델}_{s_일자}.pkl'))
            df_종목조건 = df_감시대상.copy()
            li_li_종목조건 = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': '확률스펙'].values]
            df_감시대상['li_조건'] = [[int(li_종목조건[i]) if i < 5 else li_종목조건[i] for i in range(len(li_종목조건))]
                                for li_종목조건 in li_li_종목조건]
            try:
                dic_종목조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
            except KeyError:
                dic_종목조건 = dict()

            # 데이터 기준 종목 정의
            df_감시대상_전일 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_모델}_{s_일자_전일}.pkl'))
            li_데이터종목 = df_감시대상_전일['종목코드'].values if len(df_감시대상_전일) > 0 else list()

            # 전일 종가 데이터 불러오기 (당일 기준 - 분봉 데이터 처리 시 사용)
            dic_전일종가 = Logic.trd_load_전일종가(s_일자=s_일자, li_데이터종목=li_데이터종목)

            # 분봉 데이터 불러오기 (당일 기준 - 매도 검증 시 사용)
            dic_분봉 = dict()
            dic_분봉_전체 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_일자}.pkl'))
            for s_종목코드 in li_데이터종목:
                df_분봉 = dic_분봉_전체[s_종목코드]
                df_분봉['일자시간'] = df_분봉['일자'] + ' ' + df_분봉['시간']
                df_분봉['일자시간'] = pd.to_datetime(df_분봉['일자시간'], format='%Y%m%d %H:%M:%S')
                df_분봉 = df_분봉.set_index(keys='일자시간').sort_index(ascending=True)
                dic_분봉[s_종목코드] = df_분봉

            # 10분봉 데이터 불러오기 (당일 + 전일 기준 - 실제는 10분봉 tr 조회 후 리턴값 사용)
            dic_10분봉 = dict()
            dic_10분봉_전체_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_10분봉_전체_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))
            for s_종목코드 in li_데이터종목:
                df_10분봉_당일 = dic_10분봉_전체_당일[s_종목코드]
                df_10분봉_전일 = dic_10분봉_전체_전일[s_종목코드]
                df_10분봉 = pd.concat([df_10분봉_전일, df_10분봉_당일], axis=0).sort_index()
                dic_10분봉[s_종목코드] = df_10분봉

            # 결과 저장
            pd.to_pickle(dic_종목조건, os.path.join(self.folder_데이터준비, f'dic_종목조건_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(df_종목조건, os.path.join(self.folder_데이터준비, f'df_종목조건_{s_모델}_{s_일자}.pkl'))
            df_종목조건.to_csv(os.path.join(self.folder_데이터준비, f'df_종목조건_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')
            pd.to_pickle(dic_전일종가, os.path.join(self.folder_데이터준비, f'dic_전일종가_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(dic_분봉, os.path.join(self.folder_데이터준비, f'dic_분봉_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(dic_10분봉, os.path.join(self.folder_데이터준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'데이터준비 완료({s_일자}, {len(dic_10분봉):,}개 종목, {s_모델})')

    def 백테스팅_매수검증(self, s_모델):
        """ 10분 단위로 종목모델, 공통모델 검증 진행 후 결과 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_데이터준비)
                    if f'dic_10분봉_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수검증)
                    if f'df_매수검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 데이터 파일 불러오기 (전일 종목, 모델 기준으로 당일 데이터 검증 => 종목-전일, ohlcv-당일, 모델-전일)
            dic_종목조건 = pd.read_pickle(os.path.join(self.folder_데이터준비, f'dic_종목조건_{s_모델}_{s_일자_전일}.pkl'))
            dic_전일종가_전체 = pd.read_pickle(os.path.join(self.folder_데이터준비, f'dic_전일종가_{s_모델}_{s_일자}.pkl'))
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_데이터준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))
            dic_종목모델 = pd.read_pickle(os.path.join(self.folder_종목모델, f'dic_모델_감시대상_{s_모델}_{s_일자_전일}.pkl'))
            obj_공통모델 = pd.read_pickle(os.path.join(self.folder_공통모델, f'obj_공통모델_{s_모델}_{s_일자_전일}.pkl'))

            # 종목별 케이스 정보 생성
            dic_케이스 = dict()
            for s_종목코드 in dic_종목조건.keys():
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이, n_예측성공, n_확률스펙 = dic_종목조건[s_종목코드]
                dic_케이스[s_종목코드] = f'{n_대기봉수}_{n_학습일수}_{n_rf_트리}_{n_rf_깊이}'

            # 기준시간 생성
            try:
                s_코드 = max(dic_10분봉, key=lambda x: len(dic_10분봉[x]))
                li_시간 = list(dic_10분봉[s_코드]['시간'].unique())
            except ValueError:
                li_시간 = []

            # 매수검증 진행
            li_df_매수검증 = []
            dic_데이터셋_종목 = dict()
            dic_데이터셋_공통 = dict()
            for s_시간 in tqdm(li_시간, desc=f'매수검증 ({s_일자})'):
                for s_종목코드 in dic_종목조건.keys():
                    # 데이터 불러오기
                    df_10분봉 = dic_10분봉[s_종목코드]
                    s_케이스 = dic_케이스[s_종목코드]
                    n_확률스펙 = dic_종목조건[s_종목코드][5]
                    obj_종목모델 = dic_종목모델[s_종목코드][s_케이스]
                    obj_공통모델 = obj_공통모델
                    s_종목명 = df_10분봉['종목명'].values[-1]
                    dic_전일종가 = dic_전일종가_전체[s_종목코드]

                    # 필요 데이터 골라내기 (tr 조회한 df와 동일) => trd
                    dt_시점 = pd.Timestamp(f'{s_일자} {s_시간}')
                    df_10분봉_tr = df_10분봉[df_10분봉.index < dt_시점].sort_index(ascending=False)

                    # 이동평균 데이터 생성 => trd
                    df_10분봉_ma = Logic.trd_make_이동평균_분봉(df_분봉=df_10분봉_tr, dic_전일종가=dic_전일종가)

                    # 종목모델 데이터셋 생성 => trd
                    df_데이터셋_종목 = Logic.trd_make_추가데이터_종목모델_rf(df=df_10분봉_ma)
                    ary_x = Logic.trd_make_x_1개검증용_rf(df=df_데이터셋_종목)

                    # 종목모델 상승확률 산출 => trd
                    try:
                        n_상승확률_종목 = obj_종목모델.predict_proba(ary_x)[:, 1][0] * 100
                    except (IndexError, ValueError):
                        n_상승확률_종목 = 0

                    # 공통모델 데이터셋 생성 => trd
                    df_데이터셋_공통 = Logic.trd_make_추가데이터_공통모델_rf(df=df_10분봉_ma,
                                                              n_상승확률_종목=n_상승확률_종목, n_확률스펙=n_확률스펙)
                    ary_x = Logic.trd_make_x_1개검증용_rf(df=df_데이터셋_공통)

                    # 공통모델 상승확률 산출 => trd
                    try:
                        n_상승확률_공통 = obj_공통모델.predict_proba(ary_x)[:, 1][0] * 100
                    except (IndexError, ValueError):
                        n_상승확률_공통 = 0

                    # 검증 데이터 생성
                    li_컬럼명 = ['일자', '종목코드', '종목명', '시간']
                    df_검증 = pd.DataFrame([[s_일자, s_종목코드, s_종목명, s_시간]], columns=li_컬럼명)
                    df_검증['일자시간'] = pd.to_datetime(df_검증['일자'] + ' ' + df_검증['시간'], format='%Y%m%d %H:%M:%S')
                    df_검증 = df_검증.set_index(keys='일자시간').sort_index(ascending=True)
                    df_검증['종목케이스'] = s_케이스
                    df_검증['확률스펙종목(%)'] = n_확률스펙
                    df_검증['종목확률(%)'] = n_상승확률_종목
                    df_검증['공통확률(%)'] = n_상승확률_공통

                    # 매수 신호 생성
                    df_검증['종목신호'] = (df_검증['종목확률(%)'] >= 50) * 1
                    df_검증['공통신호'] = (df_검증['공통확률(%)'] >= 55) * 1
                    df_검증['매수신호'] = df_검증['종목신호'] * df_검증['공통신호']
                    li_df_매수검증.append(df_검증)

                    # 종목 데이터셋 입력
                    try:
                        dic_데이터셋_종목[s_종목코드][dt_시점] = df_데이터셋_종목
                    except KeyError:
                        dic_데이터셋_종목[s_종목코드] = dict()
                        dic_데이터셋_종목[s_종목코드][dt_시점] = df_데이터셋_종목

                    # 공통 데이터셋 입력
                    try:
                        dic_데이터셋_공통[s_종목코드][dt_시점] = df_데이터셋_공통
                    except KeyError:
                        dic_데이터셋_공통[s_종목코드] = dict()
                        dic_데이터셋_공통[s_종목코드][dt_시점] = df_데이터셋_공통

            # df 정리 (데이터 없을 시 df에 None 출력)
            if len(li_df_매수검증) == 0:
                li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_매수검증)
                          if f'df_매수검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
                s_전일파일 = max(li_파일명)
                df_매수검증 = pd.read_pickle(os.path.join(self.folder_매수검증, s_전일파일))
                df_매수검증 = df_매수검증.drop(df_매수검증.index)
                df_매수검증.loc[0] = None
            else:
                df_매수검증 = pd.concat(li_df_매수검증, axis=0).drop_duplicates()

            # 결과 저장
            pd.to_pickle(dic_데이터셋_종목, os.path.join(self.folder_매수검증, f'dic_데이터셋_종목_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(dic_데이터셋_공통, os.path.join(self.folder_매수검증, f'dic_데이터셋_공통_{s_모델}_{s_일자}.pkl'))
            pd.to_pickle(df_매수검증, os.path.join(self.folder_매수검증, f'df_매수검증_{s_모델}_{s_일자}.pkl'))
            df_매수검증.to_csv(os.path.join(self.folder_매수검증, f'df_매수검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매수검증 완료({s_일자}, {s_모델})')

    def 백테스팅_매도검증(self, s_모델):
        """ 1분봉 기준 매도 조건 검증 후 결과 저장 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매수검증)
                    if f'df_매수검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도검증)
                    if f'df_매도검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 데이터 파일 불러오기 (전일 종목, 모델 기준으로 당일 데이터 검증 => 종목-전일, ohlcv-당일, 모델-전일)
            df_매수검증 = pd.read_pickle(os.path.join(self.folder_매수검증, f'df_매수검증_{s_모델}_{s_일자}.pkl'))
            dic_10분봉 = pd.read_pickle(os.path.join(self.folder_데이터준비, f'dic_10분봉_{s_모델}_{s_일자}.pkl'))
            dic_분봉 = pd.read_pickle(os.path.join(self.folder_데이터준비, f'dic_분봉_{s_모델}_{s_일자}.pkl'))

            # 매수신호 재점검
            df_매수검증['종목신호'] = (df_매수검증['종목확률(%)'] >= 50) * 1
            df_매수검증['공통신호'] = (df_매수검증['공통확률(%)'] >= 55) * 1
            df_매수검증['매수신호'] = df_매수검증['종목신호'] * df_매수검증['공통신호']

            # 매수신호 골라내기 (종목신호 전체)
            df_매수 = df_매수검증[df_매수검증['종목신호'] == 1]
            li_시간_매수 = list(df_매수['시간'].unique())

            # 매도 검증
            s_검증시간 = '08:59:00'
            li_df_매도검증 = []
            for s_시간 in tqdm(li_시간_매수, desc=f'매도검증 ({s_일자})'):
                # 마감시간 확인 (마감시간 전에는 다른 동작 불가)
                if s_시간 <= s_검증시간:
                    continue
                # 매수종목 선택 (먼저 나오는 종목)
                df_매수_시간 = df_매수[df_매수['시간'] == s_시간]
                s_종목코드 = df_매수_시간['종목코드'].values[0]
                s_종목명 = df_매수_시간['종목명'].values[0]
                s_종목케이스 = df_매수_시간['종목케이스'].values[0]
                s_대기봉수, s_학습일수, s_rf_트리, s_rf_깊이 = s_종목케이스.split('_')
                n_종목신호 = df_매수_시간['종목신호'].values[0]
                n_공통신호 = df_매수_시간['공통신호'].values[0]
                n_매수신호 = df_매수_시간['매수신호'].values[0]

                # 분봉 데이터 불러오기
                dt_시작 = pd.Timestamp(f'{s_일자} {s_시간}')
                dt_종료 = pd.Timestamp(f'{s_일자} {s_시간}') + pd.Timedelta(minutes=(int(s_대기봉수) * 10))
                df_분봉 = dic_분봉[s_종목코드]
                df_분봉 = df_분봉[df_분봉.index >= dt_시작]
                df_분봉 = df_분봉[df_분봉.index <= dt_종료]
                if len(df_분봉) == 0:
                    break

                # 매수 정보 산출
                s_시간_매수 = df_분봉['시간'].values[0]
                n_단가_매수 = int(df_분봉['고가'].values[0])

                # 기준가 불러오기 불러오기 (10분봉 기준 이전 분봉 종가)
                df_10분봉 = dic_10분봉[s_종목코드]
                df_10분봉_tr = df_10분봉[df_10분봉.index <= dt_시작].sort_index(ascending=False)
                n_단가_기준 = int(df_10분봉_tr['종가'].values[1])

                # 매도 검증 (분봉)
                li_li_매도검증 = []
                for s_검증시간 in df_분봉['시간']:
                    # 데이터 확인
                    df_검증 = df_분봉[df_분봉['시간'] == s_검증시간]
                    n_시가 = int(df_검증['시가'].values[0])
                    n_고가 = int(df_검증['고가'].values[0])
                    n_저가 = int(df_검증['저가'].values[0])
                    n_종가 = int(df_검증['종가'].values[0])
                    n_비율_고가 = (n_고가 / n_단가_기준 - 1) * 100
                    n_비율_저가 = (n_저가 / n_단가_기준 - 1) * 100

                    # 익절 확인
                    n_손익비율_익절 = 3.0
                    if n_비율_고가 >= n_손익비율_익절:
                        s_시간_매도 = s_검증시간
                        n_단가_매도 = int(n_단가_기준 * (1 + n_손익비율_익절 / 100))
                        li_매도검증 = [s_일자, s_종목코드, s_종목명, s_대기봉수, n_종목신호, n_공통신호, n_매수신호, n_단가_기준,
                                   s_시간_매수, n_단가_매수, s_시간_매도, n_단가_매도]
                        li_li_매도검증.append(li_매도검증)
                        break

                    # 손절 확인
                    n_손익비율_손절 = -3.0
                    if n_비율_저가 <= n_손익비율_손절:
                        s_시간_매도 = s_검증시간
                        n_단가_매도 = int(n_단가_기준 * (1 + n_손익비율_손절 / 100))
                        li_매도검증 = [s_일자, s_종목코드, s_종목명, s_대기봉수, n_종목신호, n_공통신호, n_매수신호, n_단가_기준,
                                   s_시간_매수, n_단가_매수, s_시간_매도, n_단가_매도]
                        li_li_매도검증.append(li_매도검증)
                        break

                    # 시간 종료 확인
                    if s_검증시간 == df_분봉['시간'].values[-1]:
                        s_시간_매도 = s_검증시간
                        n_단가_매도 = n_종가
                        li_매도검증 = [s_일자, s_종목코드, s_종목명, s_대기봉수, n_종목신호, n_공통신호, n_매수신호, n_단가_기준,
                                   s_시간_매수, n_단가_매수, s_시간_매도, n_단가_매도]
                        li_li_매도검증.append(li_매도검증)

                    # 운영 종료 확인
                    if s_검증시간 == '15:15:00':
                        s_시간_매도 = s_검증시간
                        n_단가_매도 = n_종가
                        li_매도검증 = [s_일자, s_종목코드, s_종목명, s_대기봉수, n_종목신호, n_공통신호, n_매수신호, n_단가_기준,
                                   s_시간_매수, n_단가_매수, s_시간_매도, n_단가_매도]
                        li_li_매도검증.append(li_매도검증)

                # 매도검증 df 생성
                li_컬럼명 = ['일자', '종목코드', '종목명', '대기봉수', '종목신호', '공통신호', '매수신호', '단가_기준',
                          '시간_매수', '단가_매수', '시간_매도', '단가_매도']
                df_매도검증_시간 = pd.DataFrame(li_li_매도검증, columns=li_컬럼명)
                li_df_매도검증.append(df_매도검증_시간)

            # 전체 매도검증 취합
            if len(li_df_매도검증) == 0:
                li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_매도검증)
                          if f'df_매도검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
                if len(li_파일명) == 0:
                    df_매도검증 = pd.DataFrame()
                else:
                    s_참고파일 = max(li_파일명)
                    df_매도검증 = pd.read_pickle(os.path.join(self.folder_매도검증, s_참고파일))
                    df_매도검증 = df_매도검증.drop(df_매도검증.index)
                    df_매도검증.loc[0] = None

            else:
                df_매도검증 = pd.concat(li_df_매도검증, axis=0).drop_duplicates()
                df_매도검증['일자시간'] = df_매도검증['일자'] + ' ' + df_매도검증['시간_매수']
                df_매도검증['일자시간'] = pd.to_datetime(df_매도검증['일자시간'], format='%Y%m%d %H:%M:%S')
                df_매도검증 = df_매도검증.set_index(keys='일자시간').sort_index(ascending=True)
                df_매도검증['기준대비(%)'] = (df_매도검증['단가_매도'] / df_매도검증['단가_기준'] - 1) * 100
                df_매도검증['매수대비(%)'] = (df_매도검증['단가_매도'] / df_매도검증['단가_매수'] - 1) * 100

            # 결과 저장
            pd.to_pickle(df_매도검증, os.path.join(self.folder_매도검증, f'df_매도검증_{s_모델}_{s_일자}.pkl'))
            df_매도검증.to_csv(os.path.join(self.folder_매도검증, f'df_매도검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'매도검증 완료({s_일자}, {s_모델})')

    def 백테스팅_수익검증(self, s_모델, b_카톡=False):
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_매도검증)
                    if f'df_매도검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익검증)
                    if f'df_수익검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 매도검증 데이터 불러오기
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_매도검증)
                      if f'df_매도검증_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            li_파일명 = [파일명 for 파일명 in li_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]

            # 전체 파일 합치기
            li_df_수익검증 = list()
            for s_파일명 in li_파일명:
                df_수익검증_일별 = pd.read_pickle(os.path.join(self.folder_매도검증, s_파일명)).dropna()
                if len(df_수익검증_일별) == 0:
                    df_수익검증_일별.loc[0] = None
                    df_수익검증_일별['일자'] = re.findall(r'\d{8}', s_파일명)[0]
                    df_수익검증_일별['일자시간'] = pd.to_datetime(df_수익검증_일별['일자'])
                    df_수익검증_일별 = df_수익검증_일별.set_index('일자시간')
                li_df_수익검증.append(df_수익검증_일별)
            df_수익검증 = pd.concat(li_df_수익검증, axis=0)

            # 수익률 생성 (세금+수수료 0.3% 반영)
            df_수익검증['수익률(%)'] = (df_수익검증['단가_매도'] / df_수익검증['단가_매수'] - 1) * 100 - 0.3

            # 결과 저장
            pd.to_pickle(df_수익검증, os.path.join(self.folder_수익검증, f'df_수익검증_{s_모델}_{s_일자}.pkl'))
            df_수익검증.to_csv(os.path.join(self.folder_수익검증, f'df_수익검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'수익검증 완료({s_일자}, {s_모델})')

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

            # 수익검증 데이터 불러오기
            df_수익검증_종목 = pd.read_pickle(os.path.join(self.folder_수익검증_종목, f'df_수익검증_{s_모델}_{s_일자}.pkl'))
            df_수익검증_공통 = pd.read_pickle(os.path.join(self.folder_수익검증_공통, f'df_공통수익검증_{s_모델}_{s_일자}.pkl'))

            # 리포트 생성
            folder_리포트 = self.folder_수익검증
            s_파일명_리포트 = f'수익검증_리포트_{s_모델}_{s_일자}.png'
            # noinspection PyBroadException
            try:
                self.make_리포트_백테스팅(df_감시대상=df_감시대상,
                                   df_수익검증_분석_종목=df_수익검증_종목, df_수익검증_분석_공통=df_수익검증_공통,
                                   df_수익검증_백테=df_수익검증)
                plt.savefig(os.path.join(folder_리포트, s_파일명_리포트))
                plt.close()
            except:
                continue

            # 리포트 복사 to 서버
            import UT_배치worker
            w = UT_배치worker.Worker()
            folder_서버 = 'kakao/백테스팅'
            w.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=folder_리포트, folder_서버=folder_서버)

            # 카톡 보내기
            if b_카톡:
                import API_kakao
                k = API_kakao.KakaoAPI()
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 백테스팅 완료',
                                        s_button_title=f'[백테] 수익검증 리포트 - {s_일자}',
                                        s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'수익검증 리포트 생성 완료({s_일자}, {s_모델})')
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

    @staticmethod
    def make_리포트_백테스팅(df_감시대상, df_수익검증_분석_종목, df_수익검증_분석_공통, df_수익검증_백테):
        """ 수익검증 데이터를 기반으로 daily 리포트 생성 및 png 파일로 저장 """
        # ### 수익검증 - 분석 - 종목 ### #
        # 데이터 추가 생성
        df_수익검증_분석_종목 = df_수익검증_분석_종목[(df_수익검증_분석_종목['상승확률(%)'].isna())
                                      | (df_수익검증_분석_종목['상승확률(%)'] >= 50)].copy()
        for s_일자 in df_감시대상['일자'].unique():
            if s_일자 not in df_수익검증_분석_종목['일자'].unique():
                df = df_수익검증_분석_종목.drop(df_수익검증_분석_종목.index).copy()
                df.loc[0] = None
                df['일자'] = s_일자
                df.index = pd.to_datetime(df['일자'])
                df_수익검증_분석_종목 = pd.concat([df_수익검증_분석_종목, df], axis=0).sort_index(ascending=True)
        df_수익검증_분석_종목['년월'] = df_수익검증_분석_종목['일자'].apply(lambda x: f'{x[2:4]}-{x[4:6]}')
        sri_년월 = df_수익검증_분석_종목.groupby('년월')['년월'].first()
        df_수익검증_분석_종목['년월일'] = df_수익검증_분석_종목['일자']
        sri_년월일 = df_수익검증_분석_종목.groupby('년월일')['년월일'].first()
        # 월별 데이터 생성
        li_월별테이블_분석_종목 = list()
        li_월별테이블_분석_종목.append(sri_년월)
        li_월별테이블_분석_종목.append(df_수익검증_분석_종목.groupby('년월')['상승예측'].sum())
        li_월별테이블_분석_종목.append(df_수익검증_분석_종목.groupby('년월')['정답'].sum())
        df_월별테이블_분석_종목 = pd.concat(li_월별테이블_분석_종목, axis=1)
        df_월별테이블_분석_종목['성공률'] = df_월별테이블_분석_종목['정답'] / df_월별테이블_분석_종목['상승예측'] * 100
        df_월별테이블_분석_종목['기대수익'] = df_월별테이블_분석_종목['정답'] * 2.5 \
                                 - (df_월별테이블_분석_종목['상승예측'] - df_월별테이블_분석_종목['정답']) * 3.5
        for 컬럼명 in ['상승예측', '정답', '성공률']:
            df_월별테이블_분석_종목[컬럼명] = df_월별테이블_분석_종목[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_월별테이블_분석_종목['기대수익'] = df_월별테이블_분석_종목['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_월별테이블_T_분석_종목 = df_월별테이블_분석_종목[-8:].T
        df_월별테이블_T_분석_종목.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
        # 일별 데이터 생성
        li_일별테이블_분석_종목 = list()
        li_일별테이블_분석_종목.append(sri_년월일)
        li_일별테이블_분석_종목.append(df_수익검증_분석_종목.groupby('년월일')['상승예측'].sum())
        li_일별테이블_분석_종목.append(df_수익검증_분석_종목.groupby('년월일')['정답'].sum())
        df_일별테이블_분석_종목 = pd.concat(li_일별테이블_분석_종목, axis=1)
        df_일별테이블_분석_종목['년월일'] = df_일별테이블_분석_종목['년월일'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        df_일별테이블_분석_종목['성공률'] = df_일별테이블_분석_종목['정답'] / df_일별테이블_분석_종목['상승예측'] * 100
        df_일별테이블_분석_종목['기대수익'] = df_일별테이블_분석_종목['정답'] * 2.5 \
                                 - (df_일별테이블_분석_종목['상승예측'] - df_일별테이블_분석_종목['정답']) * 3.5
        for 컬럼명 in ['상승예측', '정답', '성공률']:
            df_일별테이블_분석_종목[컬럼명] = df_일별테이블_분석_종목[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_일별테이블_분석_종목['기대수익'] = df_일별테이블_분석_종목['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_일별테이블_T_분석_종목 = df_일별테이블_분석_종목[-10:].T
        df_일별테이블_T_분석_종목.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']

        # ### 수익검증 - 분석 - 공통 ### #
        # 데이터 추가 생성
        df_수익검증_분석_공통 = df_수익검증_분석_공통[(df_수익검증_분석_공통['공통확률(%)'].isna())
                                      | (df_수익검증_분석_공통['공통확률(%)'] >= 55)].copy()
        for s_일자 in df_감시대상['일자'].unique():
            if s_일자 not in df_수익검증_분석_공통['일자'].unique():
                df = df_수익검증_분석_공통.drop(df_수익검증_분석_공통.index).copy()
                df.loc[0] = None
                df['일자'] = s_일자
                df.index = pd.to_datetime(df['일자'])
                df_수익검증_분석_공통 = pd.concat([df_수익검증_분석_공통, df], axis=0).sort_index(ascending=True)
        df_수익검증_분석_공통['년월'] = df_수익검증_분석_공통['일자'].apply(lambda x: f'{x[2:4]}-{x[4:6]}')
        sri_년월 = df_수익검증_분석_공통.groupby('년월')['년월'].first()
        df_수익검증_분석_공통['년월일'] = df_수익검증_분석_공통['일자']
        sri_년월일 = df_수익검증_분석_공통.groupby('년월일')['년월일'].first()
        # 월별 데이터 생성
        li_월별테이블_분석_공통 = list()
        li_월별테이블_분석_공통.append(sri_년월)
        li_월별테이블_분석_공통.append(df_수익검증_분석_공통.groupby('년월')['예측'].sum())
        li_월별테이블_분석_공통.append(df_수익검증_분석_공통.groupby('년월')['정답'].sum())
        df_월별테이블_분석_공통 = pd.concat(li_월별테이블_분석_공통, axis=1)
        df_월별테이블_분석_공통['성공률'] = df_월별테이블_분석_공통['정답'] / df_월별테이블_분석_공통['예측'] * 100
        df_월별테이블_분석_공통['기대수익'] = df_월별테이블_분석_공통['정답'] * 2.5 \
                                 - (df_월별테이블_분석_공통['예측'] - df_월별테이블_분석_공통['정답']) * 3.5
        for 컬럼명 in ['예측', '정답', '성공률']:
            df_월별테이블_분석_공통[컬럼명] = df_월별테이블_분석_공통[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_월별테이블_분석_공통['기대수익'] = df_월별테이블_분석_공통['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_월별테이블_T_분석_공통 = df_월별테이블_분석_공통[-8:].T
        df_월별테이블_T_분석_공통.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
        # 일별 데이터 생성
        li_일별테이블_분석_공통 = list()
        li_일별테이블_분석_공통.append(sri_년월일)
        li_일별테이블_분석_공통.append(df_수익검증_분석_공통.groupby('년월일')['예측'].sum())
        li_일별테이블_분석_공통.append(df_수익검증_분석_공통.groupby('년월일')['정답'].sum())
        df_일별테이블_분석_공통 = pd.concat(li_일별테이블_분석_공통, axis=1)
        df_일별테이블_분석_공통['년월일'] = df_일별테이블_분석_공통['년월일'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        df_일별테이블_분석_공통['성공률'] = df_일별테이블_분석_공통['정답'] / df_일별테이블_분석_공통['예측'] * 100
        df_일별테이블_분석_공통['기대수익'] = df_일별테이블_분석_공통['정답'] * 2.5 \
                                 - (df_일별테이블_분석_공통['예측'] - df_일별테이블_분석_공통['정답']) * 3.5
        for 컬럼명 in ['예측', '정답', '성공률']:
            df_일별테이블_분석_공통[컬럼명] = df_일별테이블_분석_공통[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_일별테이블_분석_공통['기대수익'] = df_일별테이블_분석_공통['기대수익'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_일별테이블_T_분석_공통 = df_일별테이블_분석_공통[-10:].T
        df_일별테이블_T_분석_공통.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']

        # ### 수익검증 - 백테 ### #
        # 데이터 추가 생성
        df_수익검증_백테['상승예측'] = df_수익검증_백테['종목코드'].apply(lambda x: 0 if pd.isna(x) else 1)
        df_수익검증_백테['예측성공'] = df_수익검증_백테['수익률(%)'].apply(lambda x: 1 if x > 0 else 0)
        df_수익검증_백테['년월'] = df_수익검증_백테['일자'].apply(lambda x: f'{x[2:4]}-{x[4:6]}')
        sri_년월 = df_수익검증_백테.groupby('년월')['년월'].first()
        df_수익검증_백테['년월일'] = df_수익검증_백테['일자']
        sri_년월일 = df_수익검증_백테.groupby('년월일')['년월일'].first()
        # 종목 데이터 생성
        df_수익검증_백테_종목 = df_수익검증_백테[df_수익검증_백테['종목신호'] == 1].copy()
        for s_일자 in df_감시대상['일자'].unique():
            if s_일자 not in df_수익검증_백테_종목['일자'].unique():
                df = df_수익검증_백테_종목.drop(df_수익검증_백테_종목.index).copy()
                df.loc[0] = None
                df['일자'] = s_일자
                df.index = pd.to_datetime(df['일자'])
                df_수익검증_백테_종목 = pd.concat([df_수익검증_백테_종목, df], axis=0).sort_index(ascending=True)
        # 공통 데이터 생성
        df_수익검증_백테_공통 = df_수익검증_백테[df_수익검증_백테['매수신호'] == 1].copy()
        for s_일자 in df_감시대상['일자'].unique():
            if s_일자 not in df_수익검증_백테_공통['일자'].unique():
                df = df_수익검증_백테_공통.drop(df_수익검증_백테_공통.index).copy()
                df.loc[0] = None
                df['일자'] = s_일자
                df.index = pd.to_datetime(df['일자'])
                df_수익검증_백테_공통 = pd.concat([df_수익검증_백테_공통, df], axis=0).sort_index(ascending=True)
        # 월별 데이터 생성 - 종목
        li_월별테이블_백테_종목 = list()
        li_월별테이블_백테_종목.append(sri_년월)
        li_월별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월')['상승예측'].sum())
        li_월별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월')['예측성공'].sum())
        li_월별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월')['수익률(%)'].sum())
        df_월별테이블_백테_종목 = pd.concat(li_월별테이블_백테_종목, axis=1)
        df_월별테이블_백테_종목['성공률'] = df_월별테이블_백테_종목['예측성공'] / df_월별테이블_백테_종목['상승예측'] * 100
        for 컬럼명 in ['상승예측', '예측성공', '성공률']:
            df_월별테이블_백테_종목[컬럼명] = df_월별테이블_백테_종목[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_월별테이블_백테_종목['수익률(%)'] = df_월별테이블_백테_종목['수익률(%)'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_월별테이블_백테_종목 = df_월별테이블_백테_종목.loc[:, ['년월', '상승예측', '예측성공', '성공률', '수익률(%)']]
        df_월별테이블_T_백테_종목 = df_월별테이블_백테_종목[-8:].T
        df_월별테이블_T_백테_종목.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
        # 월별 데이터 생성 - 공통
        li_월별테이블_백테_공통 = list()
        li_월별테이블_백테_공통.append(sri_년월)
        li_월별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월')['상승예측'].sum())
        li_월별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월')['예측성공'].sum())
        li_월별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월')['수익률(%)'].sum())
        df_월별테이블_백테_공통 = pd.concat(li_월별테이블_백테_공통, axis=1)
        df_월별테이블_백테_공통['성공률'] = df_월별테이블_백테_공통['예측성공'] / df_월별테이블_백테_공통['상승예측'] * 100
        for 컬럼명 in ['상승예측', '예측성공', '성공률']:
            df_월별테이블_백테_공통[컬럼명] = df_월별테이블_백테_공통[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_월별테이블_백테_공통['수익률(%)'] = df_월별테이블_백테_공통['수익률(%)'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_월별테이블_백테_공통 = df_월별테이블_백테_공통.loc[:, ['년월', '상승예측', '예측성공', '성공률', '수익률(%)']]
        df_월별테이블_T_백테_공통 = df_월별테이블_백테_공통[-8:].T
        df_월별테이블_T_백테_공통.index = ['년월', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
        # 일별 데이터 생성 - 백테스팅 - 종목
        li_일별테이블_백테_종목 = list()
        li_일별테이블_백테_종목.append(sri_년월일)
        li_일별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월일')['상승예측'].sum())
        li_일별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월일')['예측성공'].sum())
        li_일별테이블_백테_종목.append(df_수익검증_백테_종목.groupby('년월일')['수익률(%)'].sum())
        df_일별테이블_백테_종목 = pd.concat(li_일별테이블_백테_종목, axis=1)
        df_일별테이블_백테_종목['년월일'] = df_일별테이블_백테_종목['년월일'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        df_일별테이블_백테_종목['성공률'] = df_일별테이블_백테_종목['예측성공'] / df_일별테이블_백테_종목['상승예측'] * 100
        for 컬럼명 in ['상승예측', '예측성공', '성공률']:
            df_일별테이블_백테_종목[컬럼명] = df_일별테이블_백테_종목[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_일별테이블_백테_종목['수익률(%)'] = df_일별테이블_백테_종목['수익률(%)'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_일별테이블_백테_종목 = df_일별테이블_백테_종목.loc[:, ['년월일', '상승예측', '예측성공', '성공률', '수익률(%)']]
        df_일별테이블_T_백테_종목 = df_일별테이블_백테_종목[-10:].T
        df_일별테이블_T_백테_종목.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']
        # 일별 데이터 생성 - 백테스팅 - 공통
        li_일별테이블_백테_공통 = list()
        li_일별테이블_백테_공통.append(sri_년월일)
        li_일별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월일')['상승예측'].sum())
        li_일별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월일')['예측성공'].sum())
        li_일별테이블_백테_공통.append(df_수익검증_백테_공통.groupby('년월일')['수익률(%)'].sum())
        df_일별테이블_백테_공통 = pd.concat(li_일별테이블_백테_공통, axis=1)
        df_일별테이블_백테_공통['년월일'] = df_일별테이블_백테_공통['년월일'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        df_일별테이블_백테_공통['성공률'] = df_일별테이블_백테_공통['예측성공'] / df_일별테이블_백테_공통['상승예측'] * 100
        for 컬럼명 in ['상승예측', '예측성공', '성공률']:
            df_일별테이블_백테_공통[컬럼명] = df_일별테이블_백테_공통[컬럼명].apply(lambda x: x if pd.isna(x) else f'{x:.0f}')
        df_일별테이블_백테_공통['수익률(%)'] = df_일별테이블_백테_공통['수익률(%)'].apply(lambda x: x if pd.isna(x) else f'{x:.1f}')
        df_일별테이블_백테_공통 = df_일별테이블_백테_공통.loc[:, ['년월일', '상승예측', '예측성공', '성공률', '수익률(%)']]
        df_일별테이블_T_백테_공통 = df_일별테이블_백테_공통[-10:].T
        df_일별테이블_T_백테_공통.index = ['월일', '예측(건)', '성공(건)', '성공률(%)', '기대수익(%)']

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
        plt.title(f'[ 상승예측 건수 - 백테스팅 (종목50%/공통55%) ]')
        sri_상승예측건수 = df_수익검증_백테_공통.groupby('년월일')['상승예측'].sum()
        sri_상승예측건수 = sri_상승예측건수.apply(lambda x: 0 if pd.isna(x) else x)
        ary_x, ary_y = sri_상승예측건수.index.values, sri_상승예측건수.values.astype(int)
        li_색깔 = ['C3' if 예측건수 > 10 else 'C0' for 예측건수 in ary_y]
        plt.bar(ary_x, ary_y, color=li_색깔)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.yticks(range(0, max(ary_y) + 1, 1))
        plt.grid(linestyle='--', alpha=0.5)

        # 일별 예측성공률
        plt.subplot(6, 2, 4)
        plt.title(f'[ 예측 성공률 - 백테스팅 (%, 종목50%/공통55%) ]')
        sri_예측성공건수 = df_수익검증_백테_공통.groupby('년월일')['예측성공'].sum()
        sri_예측성공률 = sri_예측성공건수 / sri_상승예측건수 * 100
        sri_예측성공률 = sri_예측성공률.apply(lambda x: 0 if pd.isna(x) else x)
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
        plt.title(f'[ 예측 성공률 - 백테스팅 (%, 누적, 종목50%/공통55%) ]')
        sri_예측성공률_누적 = sri_예측성공건수.cumsum() / sri_상승예측건수.cumsum() * 100
        sri_예측성공률_누적 = sri_예측성공률_누적.apply(lambda x: 0 if pd.isna(x) else x)
        ary_x, ary_y = sri_예측성공률_누적.index.values, sri_예측성공률_누적.values
        plt.plot(ary_x, ary_y)
        plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
        plt.yticks(range(0, 101, 20))
        plt.grid(linestyle='--', alpha=0.5)
        plt.axhline(100, color='C0', alpha=0)
        plt.axhline(70, color='C1')

        # 월별 성공률 - 분석 (종목모델)
        plt.subplot(6, 2, 5)
        plt.title(f'[ 월별 성공률 - 분석 - 종목 (종목50%) ]')
        df = df_월별테이블_T_분석_종목
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 일별 성공률 - 분석 (종목모델)
        plt.subplot(6, 2, 6)
        plt.title(f'[ 일별 성공률 - 분석 - 종목 (종목50%) ]')
        df = df_일별테이블_T_분석_종목
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 월별 성공률 - 백테스팅 (종목)
        plt.subplot(6, 2, 7)
        plt.title(f'[ 월별 성공률 - 백테스팅 - 종목 (종목50%) ]')
        df = df_월별테이블_T_백테_종목
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 일별 성공률 - 백테스팅 (종목)
        plt.subplot(6, 2, 8)
        plt.title(f'[ 일별 성공률 - 백테스팅 - 종목 (종목50%) ]')
        df = df_일별테이블_T_백테_종목
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 월별 성공률 - 분석 (공통모델)
        plt.subplot(6, 2, 9)
        plt.title(f'[ 월별 성공률 - 분석 - 공통 (종목50%/공통55%) ]')
        df = df_월별테이블_T_분석_공통
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 일별 성공률 - 분석 (공통모델)
        plt.subplot(6, 2, 10)
        plt.title(f'[ 일별 성공률 - 분석 - 공통 (종목50%/공통55%) ]')
        df = df_일별테이블_T_분석_공통
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 월별 성공률 - 백테스팅 (공통)
        plt.subplot(6, 2, 11)
        plt.title(f'[ 월별 성공률 - 백테스팅 - 공통 (종목50%/공통55%) ]')
        df = df_월별테이블_T_백테_공통
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)

        # 일별 성공률 - 백테스팅 (공통)
        plt.subplot(6, 2, 12)
        plt.title(f'[ 일별 성공률 - 백테스팅 - 공통 (종목50%/공통55%) ]')
        df = df_일별테이블_T_백테_공통
        plt.axis('tight')
        plt.axis('off')
        테이블 = plt.table(cellText=df.values, rowLabels=df.index, loc='center', cellLoc='center')
        테이블.auto_set_font_size(False)
        테이블.set_fontsize(12)
        테이블.scale(1.0, 2.4)


#######################################################################################################################
if __name__ == "__main__":
    a = Analyzer()

    a.백테스팅_데이터준비(s_모델='rf')
    a.백테스팅_매수검증(s_모델='rf')
    a.백테스팅_매도검증(s_모델='rf')
    a.백테스팅_수익검증(s_모델='rf', b_카톡=True)

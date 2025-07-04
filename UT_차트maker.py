import os

import matplotlib.pyplot as plt
import pandas as pd
import re

# 그래프 한글 설정
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


def find_전일종가(df_ohlcv):
    """ 입력 받은 일봉 또는 분봉에 해당 종목의 전일 종가 생성 후 df 리턴 """
    # 전일종가 존재 시 리턴
    li_생성 = ['전일종가', '전일대비(%)']
    li_존재 = [1 if 컬럼 in df_ohlcv.columns else 0 for 컬럼 in li_생성]
    if 0 not in li_존재:
        return df_ohlcv

    # 폴더 정의
    import UT_폴더manager
    folder_캐시변환 = UT_폴더manager.dic_폴더정보['데이터|캐시변환']

    # 대상일 확인
    li_전체일 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(folder_캐시변환)
              if 'dic_코드별_분봉_' in 파일명 and '.pkl' in 파일명]
    ary_기준일 = df_ohlcv['일자'].unique()
    s_추가일 = max([일자 for 일자 in li_전체일 if 일자 < ary_기준일.min()])
    li_대상일 = [s_추가일] + list(ary_기준일)

    # 일봉 불러오기
    s_종목코드 = df_ohlcv['종목코드'].values[0]
    li_대상월 = list(pd.Series(li_대상일).apply(lambda x: x[:6]).unique())
    li_df_일봉 = [pd.read_pickle(os.path.join(folder_캐시변환, f'dic_코드별_일봉_{s_해당월}.pkl'))[s_종목코드]
                for s_해당월 in li_대상월]
    df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자').reset_index(drop=True)
    dic_일자2전일종가 = df_일봉.set_index('일자').to_dict()['전일종가']
    n_전일종가_마지막 = df_일봉['종가'].values[-1]

    # df_전일종가 인덱스 설정
    df_전일종가 = df_ohlcv.copy()
    if '시간' in df_전일종가.columns:
        df_전일종가['일자시간'] = df_전일종가['일자'] + ' ' + df_전일종가['시간']
        df_전일종가['일자시간'] = pd.to_datetime(df_전일종가['일자시간'], format='%Y%m%d %H:%M:%S')
        df_전일종가 = df_전일종가.set_index(keys='일자시간').sort_index(ascending=True)
    else:
        df_전일종가 = df_전일종가.sort_values('일자')

    # df 전일종가 데이터 생성
    df_전일종가['전일종가'] = df_전일종가['일자'].apply(lambda x:
                                          dic_일자2전일종가[x] if x in dic_일자2전일종가.keys() else n_전일종가_마지막)
    df_전일종가['전일대비(%)'] = (df_전일종가['종가'] / df_전일종가['전일종가'] - 1) * 100

    return df_전일종가


def make_이동평균(df_ohlcv):
    """ 입력 받은 일봉 또는 분봉에 각종 이동평균 데이터 생성 후 df 리턴 """
    # 이동평균 존재 시 리턴
    li_생성 = ['종가ma5', '종가ma10', '종가ma20', '종가ma60', '종가ma120',
             '거래량ma5', '거래량ma20', '거래량ma60', '거래량ma120']
    li_존재 = [1 if 컬럼 in df_ohlcv.columns else 0 for 컬럼 in li_생성]
    if 0 not in li_존재:
        return df_ohlcv

    # df_이동평균 인덱스 설정
    df_이동평균 = df_ohlcv.copy()
    s_구분 = '분봉' if '시간' in df_ohlcv.columns else '일봉'
    if s_구분 == '분봉':
        df_이동평균['일자시간'] = df_이동평균['일자'] + ' ' + df_이동평균['시간']
        df_이동평균['일자시간'] = pd.to_datetime(df_이동평균['일자시간'], format='%Y%m%d %H:%M:%S')
        df_이동평균 = df_이동평균.set_index(keys='일자시간').sort_index(ascending=True)
    else:
        df_이동평균 = df_이동평균.sort_values('일자')

    # df_이동평균 인덱스 설정
    df_이동평균['종가ma5'] = df_이동평균['종가'].rolling(5).mean()
    df_이동평균['종가ma10'] = df_이동평균['종가'].rolling(10).mean()
    df_이동평균['종가ma20'] = df_이동평균['종가'].rolling(20).mean()
    df_이동평균['종가ma60'] = df_이동평균['종가'].rolling(60).mean()
    df_이동평균['종가ma120'] = df_이동평균['종가'].rolling(120).mean()
    df_이동평균['거래량ma5'] = df_이동평균['거래량'].rolling(5).mean()
    df_이동평균['거래량ma20'] = df_이동평균['거래량'].rolling(20).mean()
    df_이동평균['거래량ma60'] = df_이동평균['거래량'].rolling(60).mean()
    df_이동평균['거래량ma120'] = df_이동평균['거래량'].rolling(120).mean()

    return df_이동평균


# noinspection PyPep8Naming,PyTypeChecker
def make_차트(df_ohlcv, n_봉수=None):
    """ 입력 받은 일봉 또는 분봉 기준으로 차트 생성 후 fig 리턴 """
    # 기본색상코드 정의
    dic_색상 = {'파랑': 'C0', '주황': 'C1', '녹색': 'C2', '빨강': 'C3', '보라': 'C4',
              '고동': 'C5', '분홍': 'C6', '회색': 'C7', '올리브': 'C8', '하늘': 'C9'}

    # 데이터 정렬
    s_구분 = '분봉' if '시간' in df_ohlcv.columns else '일봉'
    if s_구분 == '분봉':
        df_ohlcv['일시'] = df_ohlcv['일자'].apply(lambda x: f'{x[:4]}-{x[4:6]}-{x[6:]}') + ' ' + df_ohlcv['시간']
        df_ohlcv = df_ohlcv.sort_values('일시')
    else:
        df_ohlcv = df_ohlcv.sort_values('일자')

    # 데이터 잘라내기
    if n_봉수 is not None:
        df_ohlcv = df_ohlcv[-1 * n_봉수:]

    # 차트 정보 생성
    s_종목코드 = df_ohlcv['종목코드'].values[0]
    s_종목명 = df_ohlcv['종목명'].values[0]
    n_봉구분 = int((pd.Timestamp(df_ohlcv['일시'].values[1]) - pd.Timestamp(df_ohlcv['일시'].values[0])).seconds / 60)\
            if s_구분 == '분봉' else None
    s_봉구분 = f'{n_봉구분}분봉' if s_구분 == '분봉' else '일봉'

    # 추가 데이터 생성
    ary_x = df_ohlcv['일시'].values if s_구분 == '분봉' else df_ohlcv['일자'].values
    df_ohlcv['몸통'] = (df_ohlcv['종가'] - df_ohlcv['시가']).apply(lambda x: x if x != 0 else 0.1)
    li_색상_캔들 = df_ohlcv['몸통'].apply(lambda x: dic_색상['파랑'] if x < 0 else dic_색상['빨강'])
    li_색상_거래량 = (df_ohlcv['거래량'] - df_ohlcv['거래량'].shift(1)).apply(lambda x:
                                                                   dic_색상['파랑'] if x < 0 else dic_색상['빨강'])

    # 차트 레이아웃 설정
    fig = plt.Figure(figsize=(16, 9), tight_layout=True)
    fig.suptitle(f'[{s_봉구분}] {s_종목명}({s_종목코드})', fontsize=16)
    ax_캔들 = fig.add_subplot(3, 1, (1, 2))
    ax_거래량 = fig.add_subplot(3, 1, 3)

    # 캔들 차트 생성
    ax_캔들.bar(ary_x, height=df_ohlcv['몸통'], bottom=df_ohlcv['시가'], width=0.8, color=li_색상_캔들)
    ax_캔들.vlines(ary_x, df_ohlcv['저가'], df_ohlcv['고가'], lw=0.5, color=li_색상_캔들)
    ax_캔들.plot(ary_x, df_ohlcv['종가ma5'], lw=0.5, color=dic_색상['분홍'], label='ma5')
    ax_캔들.plot(ary_x, df_ohlcv['종가ma10'], lw=0.5, color=dic_색상['파랑'], label='ma10')
    ax_캔들.plot(ary_x, df_ohlcv['종가ma20'], lw=2, color=dic_색상['주황'], label='ma20', alpha=0.5)
    ax_캔들.plot(ary_x, df_ohlcv['종가ma60'], lw=0.5, color=dic_색상['녹색'], label='ma60')
    ax_캔들.plot(ary_x, df_ohlcv['종가ma120'], lw=2, color='black', label='ma120', alpha=0.5)
    ax_캔들.set_ylim(df_ohlcv['저가'].min() * 0.98, df_ohlcv['고가'].max() * 1.02)

    # 거래량 차트 생성
    ax_거래량.bar(ary_x, df_ohlcv['거래량'], width=0.8, color=li_색상_거래량)
    ax_거래량.plot(ary_x, df_ohlcv['거래량ma5'], lw=0.5, color=dic_색상['분홍'], label='ma5')
    ax_거래량.plot(ary_x, df_ohlcv['거래량ma20'], lw=2, color=dic_색상['주황'], label='ma20', alpha=0.5)
    ax_거래량.plot(ary_x, df_ohlcv['거래량ma60'], lw=0.5, color=dic_색상['녹색'], label='ma60')
    ax_거래량.plot(ary_x, df_ohlcv['거래량ma120'], lw=0.5, color='black', label='ma120')

    # 공통 설정
    for ax in fig.axes:
        df_틱 = df_ohlcv.copy().reset_index()
        if s_구분 == '분봉':
            df_틱['라벨'] = df_틱['일자'].apply(lambda x: f'{x[4:6]}-{x[6:8]}')
        else:
            df_틱['라벨'] = df_틱['일자'].apply(lambda x: f'{x[2:4]}-{x[4:6]}')
        df_틱['변화'] = df_틱['라벨'] == df_틱['라벨'].shift(1)
        df_틱 = df_틱[df_틱['변화'] == False]
        ax.set_xticks(df_틱.index, labels=df_틱['라벨'])
        ax.grid(linestyle='--', alpha=0.5)
        ax.legend(loc='upper left', fontsize=8)

    return fig


def make_수익리포트(s_대상, dic_수익정보):
    """ 수익요약 데이터 기준으로 리포트 생성하여 리턴 """
    # 기준정보 정의
    s_일자 = dic_수익정보['s_일자']
    df_수익요약 = dic_수익정보['df_수익요약']
    folder_대상종목 = dic_수익정보['folder_대상종목']
    folder_캐시변환 = dic_수익정보['folder_캐시변환']

    # 거래정보 생성 - 실거래
    n_초봉 = None
    df_거래정보 = pd.DataFrame()
    if s_대상 == '실거래':
        # 실거래 적용 초봉 및 선정기준 확인
        df_대상종목_매매 = pd.read_pickle(os.path.join(folder_대상종목, f'df_대상종목_{s_일자}_매매.pkl')) \
                            if f'df_대상종목_{s_일자}_매매.pkl' in os.listdir(folder_대상종목) else None
        n_초봉 = df_대상종목_매매['초봉'].values[-1] if df_대상종목_매매 is not None else 5
        s_선정사유 = df_대상종목_매매['선정사유'].values[-1] if df_대상종목_매매 is not None else 'vi발동'

        # df_거래정보 생성
        dic_수익정보['n_초봉'] = n_초봉
        dic_수익정보['s_선정사유'] = s_선정사유
        df_거래정보_실거래 = make_수익리포트_거래정보_실거래(dic_수익정보=dic_수익정보)
        df_거래정보_백테스팅 = make_수익리포트_거래정보_백테스팅(n_초봉=n_초봉, s_선정사유=s_선정사유, dic_수익정보=dic_수익정보)
        df_거래정보 = pd.concat([df_거래정보_실거래, df_거래정보_백테스팅], axis=0).sort_values(['매수시간'])

    # 거래정보 생성 - 백테스팅
    if s_대상 == '백테스팅':
        n_초봉 = dic_수익정보['n_초봉']
        pass

    # 초봉 읽어오기
    dic_초봉 = pd.read_pickle(os.path.join(folder_캐시변환, f'dic_코드별_{n_초봉}초봉_{s_일자}.pkl'))

    # 리포트 데이터 정의
    # df_거래정보_매수 = df_거래정보[df_거래정보['주문구분'] == '매수']
    # li_종목코드 = [종목코드.replace('A', '') for 종목코드 in df_거래정보_매수['종목코드'].unique()]
    li_종목코드 = list(df_거래정보['종목코드'].unique())
    dic_종목코드2종목명 = df_거래정보.set_index('종목코드')['종목명'].to_dict()
    n_테이블_세로 = 4
    n_차트_가로 = 4
    # li_차트_세로 = [(len(df_거래정보_매수[df_거래정보_매수['종목코드'] == 종목코드])
    #                 + (n_차트_가로 - 1)) // n_차트_가로 for 종목코드 in li_종목코드]
    li_차트_세로 = [(len(df_거래정보[df_거래정보['종목코드'] == 종목코드])
                    + (n_차트_가로 - 1)) // n_차트_가로 for 종목코드 in li_종목코드]
    n_차트_세로 = n_테이블_세로 + sum(li_차트_세로)
    fig = plt.Figure(figsize=(16, n_차트_세로 * 2), tight_layout=False)

    # 리포트 생성 - 수익요약
    ax_수익요약 = fig.add_subplot(n_차트_세로, n_차트_가로, (1, n_테이블_세로 * n_차트_가로))
    ax_수익요약 = make_수익리포트_ax_수익요약(ax=ax_수익요약, df_수익요약=df_수익요약)

    # 리포트 생성 - 매수매도
    li_종목별줄수 = list()
    for i_종목순번, s_종목코드 in enumerate(li_종목코드):
        # # 미대상 종목 제외
        # if s_종목코드 not in dic_초봉:
        #     continue

        # 그래프용 데이터 생성
        df_초봉 = dic_초봉[s_종목코드]
        s_종목명 = dic_종목코드2종목명[s_종목코드]
        dic_수익정보['s_종목코드'] = s_종목코드
        dic_수익정보['s_종목명'] = s_종목명
        dic_수익정보['df_초봉'] = df_초봉
        dic_수익정보['df_거래정보_종목'] = df_거래정보[df_거래정보['종목코드'] == s_종목코드]
        # dic_매수매도 = make_수익리포트_ax_매수매도_데이터(dic_수익정보=dic_수익정보)

        # 매수매도 그래프 설정
        n_거래수_종목 = len(df_거래정보[df_거래정보['종목코드'] == s_종목코드])
        for i_거래순번 in range(n_거래수_종목):
            # 위치 설정
            dic_수익정보['n_거래순번'] = i_거래순번
            n_종목줄수 = sum(li_종목별줄수)
            n_차트위치 = (n_테이블_세로 * n_차트_가로) + (n_종목줄수 * n_차트_가로) + i_거래순번 + 1

            # 차트 생성
            ax_매수매도 = fig.add_subplot(n_차트_세로, n_차트_가로, n_차트위치)
            ax_매수매도 = make_수익리포트_ax_매수매도(ax=ax_매수매도, dic_수익정보=dic_수익정보)

            # 위치 카운트용 정보 업데이트
            if i_거래순번 + 1 == n_거래수_종목:
                li_종목별줄수.append((n_거래수_종목 + (n_차트_가로 - 1)) // n_차트_가로)

    return fig


def make_수익리포트_거래정보_실거래(dic_수익정보):
    """ 실거래 정보 조회하여 df_거래정보 생성 후 리턴"""
    # 기준정보 정의
    s_일자 = dic_수익정보['s_일자']
    n_초봉 = dic_수익정보['n_초봉']
    s_선정사유 = dic_수익정보['s_선정사유']
    folder_체결잔고 = dic_수익정보['folder_체결잔고']
    folder_주문정보 = dic_수익정보['folder_주문정보']

    # 체결잔고 데이터 불러오기 - 실거래 확인
    df_체결잔고 = pd.read_csv(os.path.join(folder_체결잔고, f'체결잔고_{s_일자}.csv'), encoding='cp949') \
                    if f'체결잔고_{s_일자}.csv' in os.listdir(folder_체결잔고)\
                    else pd.DataFrame(dict(주문상태=list(), 계좌번호=list()))
    df_체결잔고_체결 = df_체결잔고[df_체결잔고['주문상태'] == '체결'].copy()
    df_체결잔고_체결 = df_체결잔고_체결[df_체결잔고_체결['계좌번호'] == 5292685210]
    if len(df_체결잔고_체결) == 0:
        return pd.DataFrame()

    # df_거래정보 생성
    li_컬럼명 = ['종목코드', '종목명', '시간', '주문구분', '체결가']
    df_거래정보 = df_체결잔고_체결.loc[:, li_컬럼명].copy()
    df_거래정보['종목코드'] = df_거래정보['종목코드'].apply(lambda x: x.replace('A', ''))
    df_거래정보['종목명'] = df_거래정보['종목명'].apply(lambda x: x.replace(' ', ''))

    # 매도사유 추가
    df_주문정보 = pd.read_pickle(os.path.join(folder_주문정보, f'주문정보_{s_일자}.pkl')) \
                    if f'주문정보_{s_일자}.pkl' in os.listdir(folder_주문정보)\
                    else pd.DataFrame(dict(일자=list(), 주문시간=list()))
    df_주문정보['dt일시'] = pd.to_datetime(df_주문정보['일자'] + ' ' + df_주문정보['주문시간'])
    df_주문정보 = df_주문정보.set_index('dt일시')
    df_주문정보_매도 = df_주문정보[df_주문정보['주문구분'] == '매도'] if len(df_주문정보) > 0 else None
    li_매도사유 = list()
    for i in range(len(df_거래정보)):
        s_종목코드 = df_거래정보['종목코드'].values[i]
        dt_거래시간 = pd.Timestamp(f'{s_일자} {df_거래정보["시간"].values[i]}')
        s_주문구분 = df_거래정보['주문구분'].values[i]
        if s_주문구분 == '매수' or df_주문정보_매도 is None:
            li_매도사유.append(None)
            continue
        if s_주문구분 == '매도':
            df_주문정보_매도_종목 = df_주문정보_매도[df_주문정보_매도['종목코드'] == s_종목코드]
            df_주문정보_매도_종목 = df_주문정보_매도_종목[df_주문정보_매도_종목.index <= dt_거래시간]
            df_주문정보_매도_종목 = df_주문정보_매도_종목[df_주문정보_매도_종목.index >= dt_거래시간 - pd.Timedelta(seconds=1)]
            s_매도사유 = df_주문정보_매도_종목['매도사유'].values[-1] if len(df_주문정보_매도_종목) > 0 else None
            li_매도사유.append(s_매도사유)
            continue
    df_거래정보['매도사유'] = li_매도사유

    # 양식 변경 - 매수매도 한번에
    df_거래정보 = df_거래정보.sort_values(['종목코드', '시간'])
    dic_거래정보_매수 = dict()
    dic_거래정보_매도 = dict()
    li_컬럼명 = ['종목코드', '종목명', '매수시간', '매도시간', '매수가', '매도가', '매도사유']
    for i in range(len(df_거래정보)):
        s_주문구분 = df_거래정보['주문구분'].values[i]
        if s_주문구분 == '매수':
            dic_거래정보_매수.setdefault('종목코드', list()).append(df_거래정보['종목코드'].values[i])
            dic_거래정보_매수.setdefault('종목명', list()).append(df_거래정보['종목명'].values[i])
            dic_거래정보_매수.setdefault('매수시간', list()).append(df_거래정보['시간'].values[i])
            dic_거래정보_매수.setdefault('매수가', list()).append(df_거래정보['체결가'].values[i])
        if s_주문구분 == '매도':
            # dic_거래정보_매도.setdefault('종목코드', list()).append(df_거래정보['종목코드'].values[i])
            # dic_거래정보_매도.setdefault('종목명', list()).append(df_거래정보['종목명'].values[i])
            dic_거래정보_매도.setdefault('매도시간', list()).append(df_거래정보['시간'].values[i])
            dic_거래정보_매도.setdefault('매도가', list()).append(df_거래정보['체결가'].values[i])
            dic_거래정보_매도.setdefault('매도사유', list()).append(df_거래정보['매도사유'].values[i])
    df_거래정보_매수 = pd.DataFrame(dic_거래정보_매수)
    df_거래정보_매도 = pd.DataFrame(dic_거래정보_매도)
    df_거래정보_양식변경 = pd.concat([df_거래정보_매수, df_거래정보_매도], axis=1).loc[:, li_컬럼명] \
                            if len(df_거래정보_매수) == len(df_거래정보_매도) else pd.DataFrame()

    # 추가정보 생성
    df_거래정보_양식변경['초봉'] = n_초봉 if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경
    df_거래정보_양식변경['선정사유'] = s_선정사유 if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경
    df_거래정보_양식변경['거래구분'] = '실거래' if len(df_거래정보_양식변경) > 0 else df_거래정보_양식변경

    return df_거래정보_양식변경


def make_수익리포트_거래정보_백테스팅(n_초봉, s_선정사유, dic_수익정보):
    """ 백테스팅 정보 조회하여 df_거래정보 생성 후 리턴"""
    # 기준정보 정의
    s_일자 = dic_수익정보['s_일자']
    folder_결과정리 = dic_수익정보['folder_결과정리']

    # 백테스팅 결과 불러오기
    df_백테결과 = pd.read_pickle(os.path.join(folder_결과정리, f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl')) \
                    if f'df_결과정리_{s_일자}_{n_초봉}초봉.pkl' in os.listdir(folder_결과정리) else pd.DataFrame()
    df_백테결과_선정사유 = df_백테결과[df_백테결과['선정사유'] == s_선정사유]

    # df_거래정보 생성
    df_거래정보 = pd.DataFrame()
    df_거래정보['종목코드'] = df_백테결과_선정사유['종목코드'].values
    df_거래정보['종목명'] = df_백테결과_선정사유['종목명'].values
    df_거래정보['매수시간'] = df_백테결과_선정사유['매수시간'].values
    df_거래정보['매도시간'] = df_백테결과_선정사유['매도시간'].values
    df_거래정보['매수가'] = df_백테결과_선정사유['매수가'].values
    df_거래정보['매도가'] = df_백테결과_선정사유['매도가'].values
    df_거래정보['매도사유'] = df_백테결과_선정사유['매도사유'].values
    df_거래정보['초봉'] = n_초봉
    df_거래정보['선정사유'] = s_선정사유
    df_거래정보['거래구분'] = '백테스팅'

    return df_거래정보


def make_수익리포트_ax_수익요약(ax, df_수익요약):
    """ df_수익요약 기준으로 리포트 테이블 변환 후 ax 리턴 """
    # 기준정보 정의
    # df_수익요약 = dic_기준정보['df_수익요약']

    # df_리포트 생성
    df_리포트 = pd.DataFrame()
    df_리포트['일자'] = df_수익요약['일자']
    li_컬럼명 = [컬럼명 for 컬럼명 in df_수익요약.columns if '일자' not in 컬럼명]
    for s_컬럼명 in li_컬럼명:
        df_리포트[s_컬럼명] = df_수익요약[s_컬럼명].apply(lambda x: f'{x:.1f}' if x is not None else x)

    # ax 생성
    ax.set_title('[ 수익요약 ]', loc='center', fontsize=10, fontweight='bold')
    ax.axis('tight')
    ax.axis('off')
    obj_테이블 = ax.table(cellText=df_리포트.values, colLabels=df_리포트.columns, cellLoc='center', loc='center')
    obj_테이블.set_fontsize(8)
    obj_테이블.scale(1.0, 1.6)

    # 선정 조건에 배경색 표기
    try:
        for n_col in range(len(df_수익요약.columns)):
            obj_테이블[2, n_col].set_facecolor('lightgrey')
        n_max위치 = df_수익요약.set_index('일자').T['10성능%'].argmax() + 1
        for n_row in range(len(df_수익요약) + 1):
            obj_테이블[n_row, n_max위치].set_facecolor('yellow')
        obj_테이블[0, n_max위치].set_text_props(fontweight='bold')
        obj_테이블[2, n_max위치].set_text_props(fontweight='bold')
    except TypeError:
        pass

    return ax


# def make_수익리포트_ax_매수매도_데이터(dic_수익정보):
#     """" dic_기준정보를 입력받아 매수매도 그래프용 데이터 생성 후 dic_매수매도 리턴 """
#     # 기준정보 정의
#     folder_대상종목 = dic_수익정보['folder_대상종목']
#     folder_캐시변환 = dic_수익정보['folder_캐시변환']
#     folder_체결잔고 = dic_수익정보['folder_체결잔고']
#     folder_주문정보 = dic_수익정보['folder_주문정보']
#     df_초봉 = dic_수익정보['df_초봉']
#     df_거래정보 = dic_수익정보['df_거래정보']
#
#
#
#
#
#     # dic_매수매도 생성
#     if s_대상 == "실거래":
#         df_대상종목_매매 = pd.read_pickle(os.path.join(folder_대상종목, f'df_대상종목_{s_일자}_매매.pkl'))\
#                             if f'df_대상종목_{s_일자}_매매.pkl' in os.listdir(folder_대상종목) else None
#         n_초봉 = df_대상종목_매매['초봉'].values[-1] if df_대상종목_매매 is not None else 5
#         s_선정사유 = df_대상종목_매매['선정사유'].values[-1] if df_대상종목_매매 is not None else 'vi발동'
#         dic_초봉 = pd.read_pickle(os.path.join(folder_캐시변환, f'dic_코드별_{n_초봉}초봉_{s_일자}.pkl'))
#         try:
#             df_체결잔고 = pd.read_csv(os.path.join(folder_체결잔고, f'체결잔고_{s_일자}.csv'), encoding='cp949')
#         except FileNotFoundError:
#             li_파일명 = [파일명 for 파일명 in os.listdir(folder_체결잔고) if '체결잔고' in 파일명 and '.csv' in 파일명]
#             df_체결잔고 = pd.read_csv(os.path.join(folder_체결잔고, max(li_파일명)), encoding='cp949')[:0]
#         df_체결잔고 = df_체결잔고[df_체결잔고['주문상태'] == '체결']
#         df_주문정보 = pd.read_pickle(os.path.join(folder_주문정보, f'주문정보_{s_일자}.pkl'))\
#                             if f'주문정보_{s_일자}.pkl' in os.listdir(folder_주문정보) else None
#         # li_매도컬럼 = ['매도1매도우세', '매도2매수피크', '매도3하락한계', '매도4타임아웃']
#         # df_주문정보['매도사유'] = df_주문정보[li_매도컬럼].apply(lambda row: li_매도컬럼[row.tolist().index(True)][-4:]
#         #                                                     if True in row.tolist() else None, axis=1)\
#         #                         if li_매도컬럼[0] in df_주문정보.columns else None
#     if s_대상 == "백테스팅":
#         pass
#     # dic_기준정보 = dict(s_일자=s_일자, n_초봉=n_초봉, s_선정사유=s_선정사유,
#     #                 df_수익요약=df_수익요약,
#     #                 df_체결잔고=df_체결잔고, df_주문정보=df_주문정보)
#     dic_매수매도 = dict(s_일자=s_일자, n_초봉=n_초봉, s_선정사유=s_선정사유,
#                     df_체결잔고=df_체결잔고, df_주문정보=df_주문정보)
#
#     # 스케일 설정
#     df_초봉 = dic_초봉[s_종목코드]
#     dic_매수매도['df_초봉'] = df_초봉
#     dic_매수매도['s_종목코드'] = s_종목코드
#     dic_매수매도['n_시세_max'] = df_초봉['고가'].max()
#     dic_매수매도['n_시세_min'] = df_초봉['저가'].min()
#     dic_매수매도['n_거래량_max'] = max(df_초봉['매수량'].max(), df_초봉['매도량'].max())
#     dic_매수매도['n_거래량_min'] = min(df_초봉['매수량'].min(), df_초봉['매도량'].min())
#     dic_매수매도['n_거래횟수_max'] = max(df_초봉['매수횟수'].max(), df_초봉['매도횟수'].max(), df_초봉['체결횟수'].max())
#     dic_매수매도['n_거래횟수_min'] = min(df_초봉['매수횟수'].min(), df_초봉['매도횟수'].min(), df_초봉['체결횟수'].min())
#
#     return dic_매수매도


def make_수익리포트_ax_매수매도(ax, dic_수익정보):
    """ df_체결잔고 기준으로 주가 변화 및 매수매도 시점 표기 후 ax 리턴 """
    # 기준정보 정의
    s_일자 = dic_수익정보['s_일자']
    s_종목코드 = dic_수익정보['s_종목코드']
    s_종목명 = dic_수익정보['s_종목명']
    n_거래순번 = dic_수익정보['n_거래순번']
    df_초봉 = dic_수익정보['df_초봉']
    df_거래정보_종목 = dic_수익정보['df_거래정보_종목']
    # s_일자 = df_초봉.index[-1].strftime('%Y%m%d')

    # 매수매도 정보 정의
    dt_매수시점 = pd.Timestamp(f'{s_일자} {df_거래정보_종목["매수시간"].values[n_거래순번]}')
    dt_매도시점 = pd.Timestamp(f'{s_일자} {df_거래정보_종목["매도시간"].values[n_거래순번]}')
    dt_타임아웃 = dt_매수시점 + pd.Timedelta(minutes=5)
    n_매수가 = df_거래정보_종목['매수가'].values[n_거래순번]
    n_매도가 = df_거래정보_종목['매도가'].values[n_거래순번]
    s_매도사유 = df_거래정보_종목['매도사유'].values[n_거래순번]
    n_초봉 = df_거래정보_종목['초봉'].values[n_거래순번]
    s_선정사유 = df_거래정보_종목['선정사유'].values[n_거래순번]
    s_거래구분 = df_거래정보_종목['거래구분'].values[n_거래순번]

    # df_체결잔고 = dic_수익정보['df_체결잔고']
    # df_체결잔고['종목코드'] = df_체결잔고['종목코드'].apply(lambda x: x.replace('A', ''))
    # df_체결잔고_종목 = df_체결잔고[df_체결잔고['종목코드'] == s_종목코드].copy()
    # df_체결잔고_종목['dt일시'] = df_체결잔고_종목['시간'].apply(lambda x: pd.Timestamp(f'{s_일자} {x}'))
    # df_체결잔고_종목 = df_체결잔고_종목.set_index('dt일시')
    # df_주문정보 = dic_수익정보['df_주문정보']
    # df_주문정보_종목 = df_주문정보[df_주문정보['종목코드'] == s_종목코드].copy()
    # df_주문정보_종목['dt일시'] = pd.to_datetime(df_주문정보_종목['일자'] + ' ' + df_주문정보_종목['주문시간'])
    # df_주문정보_종목 = df_주문정보_종목.set_index('dt일시')
    # s_종목명 = df_체결잔고_종목['종목명'].values[-1].strip()

    # 차트 정보 정의
    df_초봉_차트 = df_초봉[df_초봉.index >= dt_매수시점 - pd.Timedelta(minutes=2)]
    df_초봉_차트 = df_초봉_차트[df_초봉_차트.index <= dt_매수시점 + pd.Timedelta(minutes=8)]

    # 스케일 정보 정의
    n_시세_max = df_초봉['고가'].max()
    n_시세_min = df_초봉['저가'].min()
    n_거래량_max = max(df_초봉['매수량'].max(), df_초봉['매도량'].max())
    n_거래량_min = min(df_초봉['매수량'].min(), df_초봉['매도량'].min())
    n_거래횟수_max = max(df_초봉['매수횟수'].max(), df_초봉['매도횟수'].max(), df_초봉['체결횟수'].max())
    n_거래횟수_min = min(df_초봉['매수횟수'].min(), df_초봉['매도횟수'].min(), df_초봉['체결횟수'].min())
    # n_시세_max = dic_수익정보['n_시세_max']
    # n_시세_min = dic_수익정보['n_시세_min']
    # n_거래량_max = dic_수익정보['n_거래량_max']
    # n_거래량_min = dic_수익정보['n_거래량_min']
    # n_거래횟수_max = dic_수익정보['n_거래횟수_max']

    # 기본색상코드 정의
    dic_색상 = {'파랑': 'C0', '주황': 'C1', '녹색': 'C2', '빨강': 'C3', '보라': 'C4',
              '고동': 'C5', '분홍': 'C6', '회색': 'C7', '올리브': 'C8', '하늘': 'C9'}

    # 데이터 정리
    # df_체결잔고_종목_매수 = df_체결잔고_종목[df_체결잔고_종목['주문구분'] == '매수']
    # df_체결잔고_종목_매도 = df_체결잔고_종목[df_체결잔고_종목['주문구분'] == '매도']
    # dt_매수시점 = df_체결잔고_종목_매수.index[n_거래순번]
    # dt_매도시점 = df_체결잔고_종목_매도[df_체결잔고_종목_매도.index >= dt_매수시점].index[0]\
    #                 if len(df_체결잔고_종목_매도) > 0 else None
    # dt_타임아웃 = dt_매수시점 + pd.Timedelta(minutes=5) if dt_매수시점 is not None else None
    # n_매수가 = df_체결잔고_종목_매수['체결가'][dt_매수시점]\
    #             if len(df_체결잔고_종목_매수[df_체결잔고_종목_매수.index == dt_매수시점]) == 1\
    #             else max(df_체결잔고_종목_매수['체결가'][dt_매수시점])
    # if dt_매도시점 is not None:
    #     n_매도가 = df_체결잔고_종목_매도['체결가'][dt_매도시점]\
    #                 if len(df_체결잔고_종목_매도[df_체결잔고_종목_매도.index == dt_매도시점]) == 1\
    #                 else max(df_체결잔고_종목_매도['체결가'][dt_매도시점])
    # else:
    #     n_매도가 = None
    # df_주문정보_종목_매도 = df_주문정보_종목[df_주문정보_종목.index <= dt_매도시점]
    # df_주문정보_종목_매도 = df_주문정보_종목_매도[df_주문정보_종목_매도.index >= dt_매도시점 - pd.Timedelta(seconds=1)]
    # s_매도사유 = df_주문정보_종목_매도['매도사유'].values[-1] if len(df_주문정보_종목_매도) > 0 else None
    # df_초봉_차트 = df_초봉[df_초봉.index >= dt_매수시점 - pd.Timedelta(minutes=2)]
    # df_초봉_차트 = df_초봉_차트[df_초봉_차트.index <= dt_매수시점 + pd.Timedelta(minutes=8)]

    # ax 생성
    s_차트명 = f'[ {s_종목명}({s_종목코드}) ] {n_초봉}초봉' if n_거래순번 == 0 else ''
    # s_차트명 = f'[ {s_종목명}({s_종목코드}) ] {n_초봉}초봉|{s_선정사유}' if n_거래순번 == 0 else ''
    ax.set_title(s_차트명, loc='left', fontsize=10, fontweight='bold')
    ax.vlines(df_초봉_차트.index, df_초봉_차트['저가'], df_초봉_차트['고가'], lw=0.5, color='black')
    ax_거래량 = ax.twinx()
    ax_거래량.plot(df_초봉_차트.index, df_초봉_차트['매수량'], lw=0.5, alpha=0.7, color=dic_색상['빨강'])
    ax_거래량.plot(df_초봉_차트.index, df_초봉_차트['매도량'], lw=0.5, alpha=0.7, color=dic_색상['파랑'])
    ax_체결횟수 = ax.twinx()
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['매수횟수'], lw=1, alpha=0.2, color=dic_색상['빨강'])
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['매도횟수'], lw=1, alpha=0.2, color=dic_색상['파랑'])
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['체결횟수'], lw=1, alpha=0.3, color='black')

    # 매수매도 시점 표기
    ax.axvline(dt_매수시점, lw=1, alpha=0.5, color=dic_색상['녹색']) if dt_매수시점 is not None else None
    ax.axvline(dt_매도시점, lw=1, alpha=0.5, color=dic_색상['보라']) if dt_매도시점 is not None else None
    ax.axhline(n_매수가, lw=1, alpha=0.5, color=dic_색상['녹색']) if n_매수가 is not None else None
    ax.axhline(n_매도가, lw=1, alpha=0.5, color=dic_색상['보라']) if n_매도가 is not None else None
    ax.axvspan(df_초봉_차트.index[0], dt_매수시점, alpha=0.3, color=dic_색상['회색']) if dt_매수시점 is not None else None
    ax.axvspan(dt_타임아웃, df_초봉_차트.index[-1], alpha=0.3, color=dic_색상['회색']) if dt_타임아웃 is not None else None

    # 스케일 설정
    ax.set_ylim(n_시세_min, n_시세_max)
    ax.set_yticks([])
    ax.set_xticks([dt_매수시점],
                    labels=[f'{s_거래구분}|{s_선정사유}-{dt_매수시점.strftime("%H:%M:%S")}-{s_매도사유}'], ha='center')
    ax_거래량.set_ylim(n_거래량_min, n_거래량_max * 1.1)
    ax_거래량.set_yticks([])
    ax_체결횟수.set_ylim(0, n_거래횟수_max * 1.1)
    ax_체결횟수.set_yticks([])

    return ax


#######################################################################################################################
if __name__ == "__main__":
    df_샘플 = pd.DataFrame({'일자': ['20240719', '20240722', '20240723'], '종목코드': ['000020', '000020', '000020']})
    df_샘플 = find_전일종가(df_ohlcv=df_샘플)
    df_샘플 = make_이동평균(df_ohlcv=df_샘플)

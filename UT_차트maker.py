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


#######################################################################################################################
if __name__ == "__main__":
    df_샘플 = pd.DataFrame({'일자': ['20240719', '20240722', '20240723'], '종목코드': ['000020', '000020', '000020']})
    df_샘플 = find_전일종가(df_ohlcv=df_샘플)
    df_샘플 = make_이동평균(df_ohlcv=df_샘플)

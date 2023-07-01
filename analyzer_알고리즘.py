import os
import sys
import pandas as pd

import numpy as np
# noinspection PyUnresolvedReferences
from sklearn.model_selection import train_test_split

from tqdm import tqdm


# noinspection PyArgumentList
def make_추가데이터_lstm(df):
    """ ohlcv 데이터 (ma 포함, na 삭제) 받아서 분석을 위한 추가 데이터 처리 후 ary 데이터를 dic에 담아서 리턴 """
    # df_추가 생성
    # 정리 (date, code, name, time, ohlcv 만 남기고 삭제)
    df_추가 = df.loc[:, ['일자', '종목코드', '종목명', '시간', '시가', '고가', '저가', '종가', '거래량']].copy()

    # 상승률 생성
    df_추가['종가_1봉'] = df['종가'].shift(1)
    df_추가['상승률(%)'] = (df['종가'] / df_추가['종가_1봉'] - 1) * 100

    # ma값 가져오기
    df_추가['종가ma5'] = df['종가ma5'].values
    df_추가['종가ma10'] = df['종가ma5'].values
    df_추가['종가ma20'] = df['종가ma5'].values
    df_추가['종가ma60'] = df['종가ma5'].values
    df_추가['종가ma120'] = df['종가ma5'].values

    # 인자 값을 상승률로 변경
    li_인자 = ['시가', '고가', '저가', '종가', '거래량', '종가ma5', '종가ma10', '종가ma20']
    for s_인자 in li_인자:
        if s_인자 == '상승률(%)':
            pass
        elif s_인자 == '거래량':
            # 거래량은 이전 1봉 ~ 3봉까지 데이터도 변환 (단, 거래량 0인 경우 상승률도 0으로 설정)
            df_추가.loc[df_추가['거래량'] == 0, '거래량'] = 0.1
            for i in range(3):
                n_이전봉 = i + 1
                df_추가[f'상승률(%)_거래량_{n_이전봉}봉'] = (df_추가['거래량'] / df_추가['거래량'].shift(n_이전봉)) * 100
                df_추가.loc[df_추가['거래량'] == 0.1, f'상승률(%)_거래량_{n_이전봉}봉'] = 0
            df_추가.loc[df_추가['거래량'] == 0.1, '거래량'] = 0
        else:
            # 나머지 항목은 종가_1봉 대비 상승률 산출
            df_추가[f'상승률(%)_{s_인자}'] = (df_추가[s_인자] / df_추가['종가_1봉'] - 1) * 100

    # 상승률 대상으로 one-hot encoding 변환
    li_인자_ohe = [f'상승률(%)_{인자}' for 인자 in li_인자 if 인자 not in ['거래량', '상승률(%)']]
    for s_인자 in li_인자_ohe:
        li_구간 = [-float('inf'), -5, -3, -2, -1, -0.5, 0.5, 1, 2, 3, 5, float('inf')]
        li_구간명 = ['-5', '-3', '-2', '-1', '-05', '0', '+05', '+1', '+2', '+3', '+5']
        for s_구간명, n_구간_시작, n_구간_종료 in zip(li_구간명, li_구간[:-1], li_구간[1:]):
            sri_컬럼 = pd.Series((df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료), name=f'ohe_{s_인자}_{s_구간명}')
            df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    li_인자_ohe = [f'상승률(%)_거래량_{i + 1}봉' for i in range(3)]
    for s_인자 in li_인자_ohe:
        li_구간 = [-float('inf'), 30, 50, 100, 150, 200, 300, float('inf')]
        li_구간명 = ['0', '50', '100', '150', '200', '300', '500']
        for s_구간명, n_구간_시작, n_구간_종료 in zip(li_구간명, li_구간[:-1], li_구간[1:]):
            sri_컬럼 = pd.Series((df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료), name=f'ohe_{s_인자}_{s_구간명}')
            df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    # 라벨 데이터 생성
    s_컬럼명_라벨 = '상승률(%)_고가'
    n_스펙_라벨 = 3

    sri_컬럼 = pd.Series(df_추가[s_컬럼명_라벨] > n_스펙_라벨, name=f'라벨_{s_컬럼명_라벨}')
    df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    # 데이터 잘라내기
    df_추가 = df_추가.dropna()[-1000:]

    # x, y 데이터 생성
    n_윈도우 = 20
    li_인자_ohe = [컬럼명 for 컬럼명 in df_추가.columns if 'ohe_' in 컬럼명]
    s_라벨컬럼 = f'라벨_{s_컬럼명_라벨}'

    # 데이터 길이가 윈도우 크기보다 작으면 종료
    if len(df_추가) <= n_윈도우:
        return None

    else:
        ary_데이터_x = df_추가.loc[:, li_인자_ohe].values
        ary_데이터_y = df_추가[s_라벨컬럼].values

        li_x = list()
        li_y = list()
        for i in range(len(ary_데이터_x) - n_윈도우):
            # 참고할 과거 데이터 생성
            li_x.append(ary_데이터_x[i:i + n_윈도우])
            # 라벨 데이터 생성
            li_y.append(ary_데이터_y[i + n_윈도우])

        ary_x = np.array(li_x)
        ary_x = ary_x.reshape(-1, n_윈도우, len(li_인자_ohe))
        ary_y = np.array(li_y)

        # train, test 데이터 분리
        ary_x_학습, ary_x_검증, ary_y_학습, ary_y_검증 = train_test_split(ary_x, ary_y, test_size=0.2, random_state=42)

        # dic에 저장
        dic_데이터셋 = dict()
        dic_데이터셋['ary_x'] = ary_x
        dic_데이터셋['ary_y'] = ary_y
        dic_데이터셋['ary_x_학습'] = ary_x_학습
        dic_데이터셋['ary_y_학습'] = ary_y_학습
        dic_데이터셋['ary_x_검증'] = ary_x_검증
        dic_데이터셋['ary_y_검증'] = ary_y_검증

        return dic_데이터셋

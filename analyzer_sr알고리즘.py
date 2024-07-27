import os
import sys
import pandas as pd
import numpy as np

from scipy import stats
from tqdm import tqdm


def find_지지저항(df_ohlcv, n_윈도우=60):
    """ 입력 받은 일봉, 분봉 기준으로 지지저항 값 찾아서 df 형태로 리턴 """
    # 데이터 정렬
    df_지지저항 = df_ohlcv.copy()
    s_구분 = '분봉' if '시간' in df_ohlcv.columns else '일봉'
    if s_구분 == '분봉':
        df_지지저항['일자시간'] = df_지지저항['일자'] + ' ' + df_지지저항['시간']
        df_지지저항['일자시간'] = pd.to_datetime(df_지지저항['일자시간'], format='%Y%m%d %H:%M:%S')
        df_지지저항 = df_지지저항.set_index(keys='일자시간').sort_index(ascending=True)
    else:
        df_지지저항['년월일'] = df_지지저항['일자'].values
        df_지지저항['년월일'] = pd.to_datetime(df_지지저항['년월일'], format='%Y%m%d')
        df_지지저항 = df_지지저항.set_index(keys='년월일').sort_index(ascending=True)

    # 지지저항 값 찾기 (z-score 3 초과)
    df_지지저항[f'z값_거래량{n_윈도우}'] = df_지지저항['거래량'].rolling(n_윈도우).apply(lambda x: stats.zscore(x)[-1])
    df_지지저항 = df_지지저항[df_지지저항[f'z값_거래량{n_윈도우}'] > 3]

    # 중복값 제거 (1% 이내 => 거래량 큰 값 선택)
    df_지지저항 = df_지지저항.sort_values('고가')
    df_지지저항['고가temp'] = df_지지저항['고가'].values
    df_지지저항['거래량temp'] = df_지지저항['거래량'].values
    li_df_고가그룹 = list()
    while len(df_지지저항) > 0:
        n_고가기준 = df_지지저항['고가'].values[0] * 1.01
        li_df_고가그룹.append(df_지지저항[df_지지저항['고가'] < n_고가기준])
        df_지지저항 = df_지지저항[df_지지저항['고가'] >= n_고가기준]
    li_df_지지저항 = list()
    for df_고가그룹 in li_df_고가그룹:
        n_거래량max = df_고가그룹['거래량'].values.max()
        li_df_지지저항.append(df_고가그룹[df_고가그룹['거래량'] == n_거래량max])
    df_지지저항 = pd.concat(li_df_지지저항, axis=0)

    return df_지지저항

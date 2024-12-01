import os
import sys
import pandas as pd
import numpy as np


def cal_z스코어(데이터):
    """ 입력 받은 데이터 기준으로 마지막 값의 z스코어 계산 후 리턴 """
    # 데이터 계산
    ary_데이터 = np.array(데이터)
    n_평균값 = ary_데이터.mean()
    n_표준편차 = ary_데이터.std()
    n_데이터 = ary_데이터[-1]

    # z스코어 계산
    n_z스코어 = (n_데이터 - n_평균값) / n_표준편차 if n_표준편차 != 0 else 0

    return n_z스코어


def make_초봉데이터(li_체결정보, s_오늘, n_초봉):
    """ list 형태의 체결정보 데이터를 초봉 데이터로 변환 후 df 리턴 """
    # 체결정보 정리
    df_체결정보 = pd.DataFrame(li_체결정보,
                           columns=['종목코드', '체결시간', '체결단가', '전일대비(%)', '체결량', '매수매도', '체결금액'])
    df_체결정보 = df_체결정보.dropna().sort_values('체결시간')
    df_체결정보['dt일시'] = pd.to_datetime(s_오늘 + ' ' + df_체결정보['체결시간'])
    df_체결정보 = df_체결정보.set_index('dt일시')

    # 매수매도 분리
    df_체결정보_매수 = df_체결정보[df_체결정보['매수매도'] == '매수']
    df_체결정보_매도 = df_체결정보[df_체결정보['매수매도'] == '매도']

    # 리샘플 생성
    df_리샘플 = df_체결정보.resample(f'{n_초봉}s')
    df_리샘플_매수 = df_체결정보_매수.resample(f'{n_초봉}s')
    df_리샘플_매도 = df_체결정보_매도.resample(f'{n_초봉}s')

    # 초봉 생성
    df_초봉 = df_리샘플.first().loc[:, '종목코드':'체결시간']
    df_초봉['체결시간'] = df_초봉.index.strftime('%H:%M:%S')
    df_초봉['시가'] = df_리샘플['체결단가'].first()
    df_초봉['고가'] = df_리샘플['체결단가'].max()
    df_초봉['저가'] = df_리샘플['체결단가'].min()
    df_초봉['종가'] = df_리샘플['체결단가'].last()
    df_초봉['거래량'] = df_리샘플['체결량'].sum()
    df_초봉['매수량'] = df_리샘플_매수['체결량'].sum()
    df_초봉['매도량'] = df_리샘플_매도['체결량'].sum()
    df_초봉['매수량'] = df_초봉['매수량'].fillna(0).astype(int)
    df_초봉['매도량'] = df_초봉['매도량'].fillna(0).astype(int)

    return df_초봉


def make_매수신호(df_초봉, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 현재봉 제외
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉

    # 데이터 길이 검증
    if len(df_초봉) < 30:
        return [False] * 3, dict()

    # 초봉 확인
    n_초봉 = (df_초봉.index[1] - df_초봉.index[0]).seconds

    # 매수신호 검증
    li_매수신호 = list()

    # 1) z스코어 검증
    n_z매수 = cal_z스코어(df_초봉['z_매수'].values[-30:])
    n_z매도 = cal_z스코어(df_초봉['z_매도'].values[-30:])
    b_z스코어 = n_z매수 > 3 and n_z매도 < 1
    li_매수신호.append(b_z스코어)

    # 2) 거래금액 검증
    n_매수금액 = df_초봉['만원_매수'].values[-1]
    b_거래금액 = n_매수금액 > 10000 * n_초봉
    li_매수신호.append(b_거래금액)

    # 3) 체결강도 검증
    n_체결강도 = df_초봉['체결강도'].values[-1]
    b_체결강도 = n_체결강도 > 500
    li_매수신호.append(b_체결강도)

    # 정보 전달용 dic 생성
    dic_신호상세 = dict(n_초봉=n_초봉,
                    n_z매수=n_z매수, n_z매도=n_z매도,
                    n_매수금액=n_매수금액,
                    n_체결강도=n_체결강도)

    return li_매수신호, dic_신호상세


def make_매도신호(df_초봉, n_매수가, s_매수시간, n_현재가=None, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 현재봉 제외
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉

    # 데이터 길이 검증
    if len(df_초봉) < 30:
        return [False] * 4, dict()

    # 초봉 확인
    n_초봉 = (df_초봉.index[1] - df_초봉.index[0]).seconds

    # 매도신호 검증
    li_매도신호 = list()

    # 1) 매도우세 검증
    n_z매수 = cal_z스코어(df_초봉['z_매수'].values[-30:])
    n_z매도 = cal_z스코어(df_초봉['z_매도'].values[-30:])
    n_매도금액 = df_초봉['만원_매도'].values[-1]
    n_체결강도 = df_초봉['체결강도'].values[-1]
    b_매도우세 = n_z매수 < 1 and n_z매도 > 3 and n_매도금액 > 7000 * n_초봉 and n_체결강도 < 100
    li_매도신호.append(b_매도우세)

    # 2) 하락한계 검증
    if n_현재가 is None:
        ary_종가 = df_초봉['종가'].dropna().values
        n_현재가 = ary_종가[-1] if len(ary_종가) > 0 else None
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 if n_현재가 is not None else None
    b_하락한계 = n_수익률 < -0.5 if n_수익률 is not None else False
    li_매도신호.append(b_하락한계)

    # 3) 타임아웃 검증
    n_타임아웃초 = 60 * 5
    dt_현재 = pd.Timestamp('now') if dt_일자시간 is None else dt_일자시간
    s_일자 = dt_현재.strftime('%Y%m%d')
    dt_매수시간 = pd.Timestamp(f'{s_일자} {s_매수시간}')
    n_경과초 = (dt_현재 - dt_매수시간).seconds
    b_타임아웃 = n_경과초 > n_타임아웃초
    li_매도신호.append(b_타임아웃)

    # 4) 시장종료 검증
    b_시장종료 = dt_현재.strftime('%H:%M:%S') >= '15:15:00'
    li_매도신호.append(b_시장종료)

    # 정보 전달용 dic 생성
    dic_신호상세 = dict(n_초봉=n_초봉,
                    n_z매수=n_z매수, n_z매도=n_z매도, n_매도금액=n_매도금액, n_체결강도=n_체결강도,
                    n_현재가=n_현재가, n_수익률=n_수익률,
                    n_경과초=n_경과초)

    return li_매도신호, dic_신호상세

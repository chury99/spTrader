import os
import sys
import pandas as pd
import numpy as np


def cal_zscore(data):
    # 데이터 계산
    ary_데이터 = np.array(data)
    n_mean = ary_데이터.mean()
    n_std = ary_데이터.std()

    # zscore 계산
    ary_zscore = (ary_데이터 - n_mean) / n_std if n_std != 0 else [0]

    return ary_zscore


def find_일봉변동_거래량(df_일봉, n_윈도우, n_z값):
    """ 입력 받은 일봉 기준으로 거래량 변동 확인해서 조건 만족 시 당일 df 리턴 (불만족 시 빈 df 리턴) """
    # 데이터만 골라내기
    df_일봉 = df_일봉.sort_values('일자')
    if len(df_일봉) >= n_윈도우:
        df_일봉변동 = df_일봉[n_윈도우 * -1:].copy()
    else:
        df_일봉변동 = df_일봉[:0].copy()
        return df_일봉변동

    # 태그 설정
    df_일봉변동['방법론'] = '거래량'

    # 변동 확인 (z-score 초과, 거래대금 100억 초과)
    df_일봉변동['z값_거래량'] = cal_zscore(df_일봉변동['거래량'])
    if df_일봉변동['z값_거래량'].values[-1] > n_z값 and df_일봉변동['거래대금(백만)'].values[-1] > 10000:
        df_일봉변동 = df_일봉변동[-1:]
    else:
        df_일봉변동 = df_일봉변동[:0]

    return df_일봉변동


def find_지지저항_거래량(df_ohlcv, n_윈도우):
    """ 입력 받은 일봉, 분봉 기준으로 지지저항 값 찾아서 df 형태로 리턴
        # n_윈도우: 이상치 기준으로 잡을 봉 수
        # n_통합범위: 유사한 값 통합할 범위 (% 단위) """
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
    df_지지저항[f'z값_거래량{n_윈도우}'] = df_지지저항['거래량'].rolling(n_윈도우).apply(lambda x: cal_zscore(x)[-1])
    df_지지저항 = df_지지저항[df_지지저항[f'z값_거래량{n_윈도우}'] > 3]

    # 방법론 표시
    df_지지저항['방법론'] = '거래량'

    return df_지지저항


def find_지지저항_피크값(df_ohlcv):
    """ 입력 받은 일봉, 분봉 기준으로 피크값 찾아서 df 형태로 리턴 """
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

    # 지지저항 값 찾기 (범위 내 피크치 확인)
    n_범위 = 5
    n_편측 = int(n_범위 / 2)
    ary_고가 = df_지지저항['고가'].values
    li_idx_피크 = list()
    for i in range(len(ary_고가)):
        if i < n_편측:
            continue
        if ary_고가[i] == max(ary_고가[i - n_편측: i + n_편측 + 1]):
            li_idx_피크.append(i)

    # df_지지저항 생성
    df_지지저항 = df_지지저항.iloc[li_idx_피크, :]

    # 방법론 표시
    df_지지저항['방법론'] = '피크값'

    return df_지지저항


def find_지지저항_라인통합(df_지지저항, n_퍼센트범위):
    """ 입력 받은 df_지지저항 기준으로 n_퍼센트범위 내 값들을 거래량 기준으로 통합 후 df 형태로 리턴 """
    # 빈 데이터 입력되면 그대로 출력
    if len(df_지지저항) == 0:
        return df_지지저항

    # 종목별 라인 통합
    li_df_라인통합 = list()
    for s_종목코드 in df_지지저항['종목코드'].unique():
        # 고가비율 확인
        df_라인통합_종목 = df_지지저항[df_지지저항['종목코드'] == s_종목코드].copy()
        df_라인통합_종목 = df_라인통합_종목.sort_values('고가')
        df_라인통합_종목['고가비율(%)'] = (df_라인통합_종목['고가'] / df_라인통합_종목['고가'].shift(1) - 1) * 100
        df_라인통합_종목['잔존'] = df_라인통합_종목['고가비율(%)'] > n_퍼센트범위

        # n_퍼센트범위 이내 값 없을 때까지 계속 반복 확인 (단, 첫번째 row 값은 None 이라 False 값 하나 일때 마무리)
        while sum(df_라인통합_종목['잔존'] == False) > 1:
            # 그룹별 분리 (n_퍼센트범위 기준)
            li_df_고가그룹 = list()
            while len(df_라인통합_종목) > 0:
                n_고가기준 = df_라인통합_종목['고가'].values[0] * (100 + n_퍼센트범위) / 100
                li_df_고가그룹.append(df_라인통합_종목[df_라인통합_종목['고가'] <= n_고가기준])
                df_라인통합_종목 = df_라인통합_종목[df_라인통합_종목['고가'] > n_고가기준]

            # 그룹별 거래량 max 확인
            li_df_라인통합_종목 = list()
            for df_고가그룹 in li_df_고가그룹:
                n_거래량max = df_고가그룹['거래량'].values.max()
                df_거래량max = df_고가그룹[df_고가그룹['거래량'] == n_거래량max]
                df_거래량max = df_거래량max[-1:] if len(df_거래량max) > 1 else df_거래량max
                li_df_라인통합_종목.append(df_거래량max)

            # df_라인통합_종목 재생성
            df_라인통합_종목 = pd.concat(li_df_라인통합_종목, axis=0)

            # 고가 비율 재확인 (while 종료 조건 확인)
            df_라인통합_종목['고가비율(%)'] = (df_라인통합_종목['고가'] / df_라인통합_종목['고가'].shift(1) - 1) * 100
            df_라인통합_종목['잔존'] = df_라인통합_종목['고가비율(%)'] > n_퍼센트범위

        # li_df_라인통합 추가
        li_df_라인통합.append(df_라인통합_종목)

    # df_라인통합 생성
    try:
        df_라인통합 = pd.concat(li_df_라인통합, axis=0)
    except ValueError:
        df_라인통합 = pd.DataFrame()

    return df_라인통합


def find_지지저항_추가통합(df_지지저항_기존, df_ohlcv_신규):
    """ 신규 지지저항 산출 후 기존 지지저항과 통합 후 df 리턴 """
    # 기존 지지저항 확인
    if len(df_지지저항_기존['종목코드'].unique()) > 1:
        return '[error] df_지지저항_기존 : 1개 종목만 입력 필요'

    # 신규 지지저항 생성
    df_지지저항_신규_거래량 = find_지지저항_거래량(df_ohlcv=df_ohlcv_신규, n_윈도우=120)
    df_지지저항_신규_피크값 = find_지지저항_피크값(df_ohlcv=df_ohlcv_신규)

    # 지지저항 통합
    df_지지저항_통합 = pd.concat([df_지지저항_기존, df_지지저항_신규_거래량, df_지지저항_신규_피크값], axis=0).drop_duplicates()
    df_지지저항 = find_지지저항_라인통합(df_지지저항=df_지지저항_통합, n_퍼센트범위=1.5)

    return df_지지저항


# noinspection PyUnresolvedReferences
def find_매수신호(df_ohlcv, li_지지저항, dt_일자시간=None):
    """ tr 조회된 df_ohlcv 받아서 매수 신호 확인 후 list 형태로 리턴 """
    # False return 값 정의 (조건 수만큼)
    li_false = [False] * 5

    # 현재봉 제외 (tr 조회 시 현재봉 값이 포함됨)
    if dt_일자시간 is None:
        s_일자 = df_ohlcv['일자'].max()
        li_dt_3분봉 = [pd.Timestamp(f'{s_일자} 09:00:00') + pd.Timedelta(minutes=n*3) for n in range(131)]
        dt_일자시간 = max(dt for dt in li_dt_3분봉 if dt < pd.Timestamp('now'))
    df_분봉 = df_ohlcv[df_ohlcv.index < dt_일자시간].copy()

    # df 값 미존재 시 False return
    if len(df_분봉) < 2 or len(li_지지저항) == 0:
        return li_false

    # 변수 정의
    df_분봉 = df_분봉.sort_values(['일자', '시간'], ascending=True).reset_index()
    df_분봉10 = df_분봉[-10:].copy().reset_index(drop=True)
    df_분봉20 = df_분봉[-20:].copy().reset_index(drop=True)
    n_거래량z값_1 = cal_zscore(df_분봉20['거래량'].values)[-1]
    n_추세10ma10_1 = np.polyfit(df_분봉10.index, df_분봉10['종가ma10'], 1)[0]
    n_추세10ma20_1 = np.polyfit(df_분봉10.index, df_분봉10['종가ma20'], 1)[0]
    n_추세10ma60_1 = np.polyfit(df_분봉10.index, df_분봉10['종가ma60'], 1)[0]
    n_추세10ma120_1 = np.polyfit(df_분봉10.index, df_분봉10['종가ma120'], 1)[0]
    n_추세10종가_1 = np.polyfit(df_분봉10.index, df_분봉10['종가'], 1)[0]
    n_추세20ma10_1 = np.polyfit(df_분봉20.index, df_분봉20['종가ma10'], 1)[0]
    n_추세20ma20_1 = np.polyfit(df_분봉20.index, df_분봉20['종가ma20'], 1)[0]
    n_추세20ma60_1 = np.polyfit(df_분봉20.index, df_분봉20['종가ma60'], 1)[0]
    n_추세20ma120_1 = np.polyfit(df_분봉20.index, df_분봉20['종가ma120'], 1)[0]
    n_추세20종가_1 = np.polyfit(df_분봉20.index, df_분봉20['종가'], 1)[0]
    n_ma10_1 = df_분봉['종가ma10'].values[-1]
    n_ma20_1 = df_분봉['종가ma20'].values[-1]
    n_ma60_1 = df_분봉['종가ma60'].values[-1]
    n_ma120_1 = df_분봉['종가ma120'].values[-1]
    n_시가_1 = df_분봉['시가'].values[-1]
    n_저가_1 = df_분봉['저가'].values[-1]
    n_종가_1 = df_분봉['종가'].values[-1]
    n_종가_2 = df_분봉['종가'].values[-2]

    # 매수신호 생성 - 자리검증|추세검증|배열검증|sr검증|시간검증
    li_매수신호 = list()

    # 1) [자리검증] 거래량 + 이평선 통합
    # 거래량 z값이 3 초과
    b_매수신호1_1 = n_거래량z값_1 > 3
    # 이평선 터치 (0.3% 이내)
    b_매수신호1_2_10 = abs(n_저가_1 / n_ma10_1 - 1) * 100 < 0.3
    b_매수신호1_2_20 = abs(n_저가_1 / n_ma20_1 - 1) * 100 < 0.3
    b_매수신호1_2_60 = abs(n_저가_1 / n_ma60_1 - 1) * 100 < 0.3
    b_매수신호1_2 = b_매수신호1_2_10 or b_매수신호1_2_20 or b_매수신호1_2_60
    # 몸통이 +1% 초과
    b_매수신호1_3 = (n_종가_1 / n_시가_1 - 1) * 100 > 1

    b_매수신호1 = b_매수신호1_1 or (b_매수신호1_2 and b_매수신호1_3)
    li_매수신호.append(b_매수신호1)

    # 2) [추세검증] 추세가 1 초과 (종가, ma20, ma60, ma120)
    b_매수신호2_1 = n_추세10종가_1 > 1
    b_매수신호2_2 = n_추세20ma20_1 > 1
    b_매수신호2_3 = n_추세20ma60_1 > 1
    b_매수신호2_4 = n_추세20ma120_1 > 1

    b_매수신호2 = b_매수신호2_1 and b_매수신호2_2 and b_매수신호2_3 and b_매수신호2_4
    li_매수신호.append(b_매수신호2)

    # 3) [배열검증] 이평선 정배열 (종가, ma20)
    b_매수신호3_1 = n_종가_1 > n_ma20_1
    b_매수신호3_2 = n_ma20_1 > n_ma60_1
    b_매수신호3_2 = n_ma60_1 > n_ma120_1

    b_매수신호3 = b_매수신호3_1
    li_매수신호.append(b_매수신호3)

    # 4) [sr검증] 지지저항 위치
    # 종가가 지지저항 내부에 존재
    b_매수신호4_1 = max(li_지지저항) > n_종가_1 > min(li_지지저항)
    # 저항선과 1.5% 이상 갭 존재
    n_저항선 = min(저항 for 저항 in li_지지저항 if 저항 > n_종가_1) if b_매수신호4_1 else None
    b_매수신호4_2 = (n_저항선 / n_종가_1 - 1) * 100 > 1.5 if n_저항선 is not None else False

    b_매수신호4 = b_매수신호4_1 and b_매수신호4_2
    li_매수신호.append(b_매수신호4)

    # 5) [시간검증] 15시 이후 매수 금지
    s_시간 = dt_일자시간.strftime('%H:%M:%S')
    b_매수신호5_1 = s_시간 < '15:00:00'

    b_매수신호5 = b_매수신호5_1
    li_매수신호.append(b_매수신호5)

    return li_매수신호


def find_매도신호(n_현재가, dic_지지저항, s_현재시간=None):
    """ df_ohlcv 받아서 매도 신호 확인 후 list 형태로 리턴 """
    # False return 값 정의
    li_false = [False] * 5
    n_매도단가 = None

    # 기준정보 미존재 시 false return
    if None in dic_지지저항.values():
        return li_false, n_매도단가

    # 기준정보 정의
    n_매수단가 = dic_지지저항['n_매수단가']
    n_지지선 = dic_지지저항['n_지지선']
    n_저항선 = dic_지지저항['n_저항선']

    # 매도신호 생성 ['저항터치', '지지붕괴', '추세이탈', '하락한계', '장종료']
    li_매도신호 = list()

    # 1) 저항선 터치
    b_매도신호 = True if n_현재가 >= n_저항선 else False
    li_매도신호.append(b_매도신호)
    n_매도단가 = n_저항선 if b_매도신호 else n_매도단가

    # 2) 지지선 붕괴 (1% 마진)
    n_지지선_마진 = int(n_지지선 * (1 - 0.01))
    b_매도신호 = True if n_현재가 < n_지지선_마진 else False
    li_매도신호.append(b_매도신호)
    n_매도단가 = n_지지선_마진 if b_매도신호 else n_매도단가

    # 3) 추세 이탈 (이전 5개봉 종가)
    b_매도신호 = False
    li_매도신호.append(b_매도신호)
    n_매도단가 = None if b_매도신호 else n_매도단가

    # 4) 하락 한계 (매수가 대비 5% 하락)
    n_하락한계 = int(n_매수단가 * (1 - 0.05))
    b_매도신호 = True if n_현재가 < n_하락한계 else False
    li_매도신호.append(b_매도신호)
    n_매도단가 = n_하락한계 if b_매도신호 else n_매도단가

    # 5) 장 종료
    s_현재시간 = pd.Timestamp('now').strftime('%H:%M:%S') if s_현재시간 is None else s_현재시간
    b_매도신호 = True if s_현재시간 > '15:18:40' else False
    li_매도신호.append(b_매도신호)
    n_매도단가 = None if b_매도신호 else n_매도단가

    return li_매도신호, n_매도단가

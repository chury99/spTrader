import pandas as pd
import numpy as np


def cal_z스코어(ary_데이터, n_윈도우):
    """ 입력 받은 데이터 기준으로 마지막 값의 z스코어 계산 후 리턴 """
    # nan 제거 (nan : 자기 자신과 같지 않음)
    ary_데이터 = np.array([x for x in ary_데이터 if x == x])

    # 데이터 길이 검증 - 윈도우별 별도 기준 적용
    dic_길이제한 = dict(윈도우_30=20, 윈도우_10=2)
    if len(ary_데이터) < dic_길이제한[f'윈도우_{n_윈도우}']:
        return np.nan

    # 데이터 잘라내기 - 윈도우 기준
    ary_데이터 = ary_데이터[-1 * n_윈도우:]

    # 데이터 계산
    n_평균값 = ary_데이터.mean()
    n_표준편차 = ary_데이터.std()
    n_데이터 = ary_데이터[-1]

    # z스코어 계산
    n_z스코어 = (n_데이터 - n_평균값) / n_표준편차 if n_표준편차 != 0 else 0

    return n_z스코어


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

    # z-score 생성
    try:
        df_일봉변동['z값_거래량'] = cal_z스코어(데이터=df_일봉변동['거래량'], n_윈도우=30)
    except ValueError:
        df_일봉변동['z값_거래량'] = 0

    # 변동 확인 (z-score 초과, 거래대금 100억 초과)
    if df_일봉변동['z값_거래량'].values[-1] > n_z값 and df_일봉변동['거래대금(백만)'].values[-1] > 10000:
        df_일봉변동 = df_일봉변동[-1:]
    else:
        df_일봉변동 = df_일봉변동[:0]

    # 양봉 확인
    if len(df_일봉변동) > 0:
        n_시가 = df_일봉변동['시가'].values[-1]
        n_고가 = df_일봉변동['고가'].values[-1]
        n_저가 = df_일봉변동['저가'].values[-1]
        n_종가 = df_일봉변동['종가'].values[-1]
        if n_종가 - n_시가 > 0 and n_종가 - n_시가 > n_고가 - n_종가:
            df_일봉변동 = df_일봉변동[-1:]
        else:
            df_일봉변동 = df_일봉변동[:0]

    return df_일봉변동


def make_초봉데이터_trader(li_체결정보, s_오늘, n_초봉, s_종목코드):
    """ list 형태의 체결정보 데이터를 초봉 데이터로 변환 후 df 리턴 """
    # 체결정보 정리
    df_체결정보 = pd.DataFrame(li_체결정보,
                           columns=['종목코드', '체결시간', '체결단가', '전일대비(%)', '체결량', '매수매도', '체결금액'])
    df_체결정보 = df_체결정보.dropna().sort_values('체결시간')
    df_체결정보['dt일시'] = pd.to_datetime(s_오늘 + ' ' + df_체결정보['체결시간'])
    df_체결정보 = df_체결정보.set_index('dt일시')

    # 초봉 데이터 생성
    df_초봉 = make_초봉데이터(df_체결정보=df_체결정보, n_초봉=n_초봉, s_종목코드=s_종목코드)

    return df_초봉


def make_초봉데이터(df_체결정보, n_초봉, s_종목코드):
    """ df 형태의 체결정보 데이터를 초봉 데이터로 변환 후 df 리턴 """
    # 체결량이 20 이하면 제외
    df_체결정보 = df_체결정보[df_체결정보['체결량'] > 20]

    # 매수매도 분리
    df_체결정보_매수 = df_체결정보[df_체결정보['매수매도'] == '매수']
    df_체결정보_매도 = df_체결정보[df_체결정보['매수매도'] == '매도']

    # 리샘플 생성
    df_리샘플 = df_체결정보.resample(f'{n_초봉}s')
    df_리샘플_매수 = df_체결정보_매수.resample(f'{n_초봉}s')
    df_리샘플_매도 = df_체결정보_매도.resample(f'{n_초봉}s')

    # 초봉 생성
    if len(df_체결정보) > 0:
        df_초봉 = df_리샘플.first().loc[:, '종목코드':'체결시간']
        df_초봉['종목코드'] = s_종목코드
        df_초봉['체결시간'] = df_초봉.index.strftime('%H:%M:%S')
        df_초봉['시가'] = df_리샘플['체결단가'].first()
        df_초봉['고가'] = df_리샘플['체결단가'].max()
        df_초봉['저가'] = df_리샘플['체결단가'].min()
        df_초봉['종가'] = df_리샘플['체결단가'].last()
        df_초봉['거래량'] = df_리샘플['체결량'].sum()
        df_초봉['매수량'] = df_리샘플_매수['체결량'].sum()
        df_초봉['매도량'] = df_리샘플_매도['체결량'].sum()
        df_초봉['체결횟수'] = df_리샘플['체결량'].count()
        df_초봉['매수횟수'] = df_리샘플_매수['체결량'].count()
        df_초봉['매도횟수'] = df_리샘플_매도['체결량'].count()
        for 컬럼명 in ['거래량', '매수량', '매도량', '체결횟수', '매수횟수', '매도횟수']:
            df_초봉[컬럼명] = df_초봉[컬럼명].fillna(0).astype(int)
    else:
        df_초봉 = pd.DataFrame(dict(종목코드=[], 체결시간=[], 시가=[], 고가=[], 저가=[], 종가=[],
                             거래량=[], 매수량=[], 매도량=[], 체결횟수=[], 매수횟수=[], 매도횟수=[]))

    return df_초봉


# noinspection PyTypeChecker
def make_매수신호(dic_매개변수, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 변수 정의
    li_신호종류 = ['z값', '금액', '강도']
    df_초봉 = dic_매개변수['df_초봉_매수봇']
    n_초봉 = dic_매개변수['n_초봉']
    dic_매수신호 = dict(li_신호종류_매수봇=li_신호종류,
                    b_매수신호_매수봇=None, li_매수신호_매수봇=None, n_z매수량_매수봇=None, n_z매도량_매수봇=None,
                    n_매수금액_매수봇=None, n_체결강도_매수봇=None, n_매수횟수_매수봇=None, n_매도횟수_매수봇=None,
                    n_체결횟수_0카운트=None, df_초봉_기준봉_매수봇=None)

    # 현재봉 제외
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉

    # 데이터 길이 검증
    if len(df_초봉) == 0:
        li_매수신호 = [False] * len(li_신호종류)
        dic_매수신호.update(b_매수신호_매수봇=False, li_매수신호_매수봇=li_매수신호, df_초봉_기준봉_매수봇=df_초봉[-1:])
        return dic_매수신호

    # 데이터 정의
    ary_매수량 = df_초봉['매수량'].values
    ary_매도량 = df_초봉['매도량'].values
    ary_체결횟수 = df_초봉['체결횟수'].values
    n_매수량 = df_초봉['매수량'].values[-1]
    n_매도량 = df_초봉['매도량'].values[-1]
    n_체결횟수 = df_초봉['체결횟수'].values[-1]
    n_매수횟수 = df_초봉['매수횟수'].values[-1]
    n_매도횟수 = df_초봉['매도횟수'].values[-1]
    n_종가 = df_초봉['종가'].values[-1]
    df_초봉_기준봉 = df_초봉[-1:]

    # 매수신호 검증
    li_매수신호 = list()

    # 1) z스코어 검증
    n_z매수량 = cal_z스코어(ary_데이터=ary_매수량, n_윈도우=30)
    n_z매도량 = cal_z스코어(ary_데이터=ary_매도량, n_윈도우=30)
    b_z스코어 = (n_z매수량 > 3 and n_z매도량 < 1) if (n_z매수량 is not None and n_z매도량 is not None) else False
    li_매수신호.append(b_z스코어)
    dic_매수신호.update(n_z매수량_매수봇=n_z매수량, n_z매도량_매수봇=n_z매도량)

    # 2) 거래금액 검증
    n_매수금액 = n_종가 * n_매수량 / 10000
    b_거래금액 = n_매수금액 > 10000 * n_초봉
    li_매수신호.append(b_거래금액)
    dic_매수신호.update(n_매수금액_매수봇=n_매수금액)

    # 3) 체결강도 검증
    n_체결강도 = (n_매수량 / n_매도량 * 100) if n_매도량 != 0 else 99999
    n_체결횟수_0카운트 = sum(0 == ary_체결횟수[-5:])
    b_체결강도 = n_체결강도 > 500 and n_매수횟수 > 40 * n_초봉 and n_체결횟수_0카운트 == 0
    # b_체결강도 = n_체결강도 > 500 and n_매수횟수 > 30 * n_초봉 and n_체결횟수_0카운트 == 0
    # b_체결강도 = n_체결강도 > 500 and n_체결횟수_0카운트 == 0
    li_매수신호.append(b_체결강도)
    dic_매수신호.update(n_체결강도_매수봇=n_체결강도,
                    n_매수횟수_매수봇=n_매수횟수, n_매도횟수_매수봇=n_매도횟수, n_체결횟수_0카운트=n_체결횟수_0카운트)

    # 정보 전달용 dic 생성
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호.update(b_매수신호_매수봇=b_매수신호, li_매수신호_매수봇=li_매수신호, df_초봉_기준봉_매수봇=df_초봉_기준봉)

    return dic_매수신호


# noinspection PyTypeChecker
def make_매도신호(dic_매개변수, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 변수 정의
    li_신호종류 = ['매수피크', '매수변동', '하락한계', '타임아웃']
    df_초봉_기준봉 = dic_매개변수['df_초봉_기준봉_매수봇']
    df_초봉 = dic_매개변수['df_초봉_매도봇']
    s_매수시간 = dic_매개변수['s_주문시간_매수봇']
    n_매수가 = dic_매개변수['n_매수단가_매도봇']
    n_현재가 = dic_매개변수['n_현재가_매도봇']
    dic_매도신호 = dict(li_신호종류_매도봇=li_신호종류,
                    b_매도신호_매도봇=None, li_매도신호_매도봇=None, n_현재가_매도봇=None, n_수익률_매도봇=None,
                    n_경과초_매도봇=None, n_매도량_누적_매도봇=None, n_매수량_누적_매도봇=None,
                    n_z매수량_매도봇=None, n_매수비율_매도봇=None, n_체결강도_매도봇=None,
                    n_매수량max_매도봇=None, n_매수량_기준봉_매도봇=None, n_매수량_매도봇=None,
                    li_매도신호_수치_매도봇=None, s_매도사유_매도봇=None)

    # 기준봉 생성 (미입력 시)
    if df_초봉_기준봉 is None:
        df_초봉_기준봉 = df_초봉[df_초봉['체결시간'] < s_매수시간][-1:]

    # 현재봉 제외, 기준봉 추가
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉
    df_초봉 = pd.concat([df_초봉, df_초봉_기준봉], axis=0).drop_duplicates('체결시간').sort_index()

    # 데이터 길이 검증
    if len(df_초봉) == 0:
        li_매도신호 = [False] * len(li_신호종류)
        li_매도신호_수치 = [None] * len(li_신호종류)
        dic_매도신호.update(b_매도신호_매도봇=False, li_매도신호_매도봇=li_매도신호, li_매도신호_수치_매도봇=li_매도신호_수치)
        return dic_매도신호

    # 데이터 정의
    n_매수량 = df_초봉['매수량'].values[-1]
    n_매도량 = df_초봉['매도량'].values[-1]
    n_매수량_1 = df_초봉['매수량'].values[-2]
    n_매수량_1 = 0 if pd.isna(n_매수량_1) else n_매수량_1
    if n_현재가 is None:
        ary_종가 = df_초봉['종가'].dropna().values
        n_현재가 = ary_종가[-1] if len(ary_종가) > 0 else None
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_현재가 is not None else None
    df_초봉_매수이후 = df_초봉[df_초봉.index >= df_초봉_기준봉.index[0]] if len(df_초봉_기준봉) > 0 else df_초봉
    n_매수량_누적 = df_초봉_매수이후['매수량'].sum()
    n_매도량_누적 = df_초봉_매수이후['매도량'].sum()
    n_매수량max = df_초봉_매수이후[:-1]['매수량'].max()
    n_매수량_기준봉 = df_초봉_기준봉['매수량'].values[-1]
    ary_매수량 = df_초봉['매수량'].values
    n_z매수량 = cal_z스코어(ary_데이터=ary_매수량, n_윈도우=10)
    n_z매수량_1 = cal_z스코어(ary_데이터=ary_매수량[:-1], n_윈도우=10)
    s_이전봉시간 = df_초봉['체결시간'].values[-1]
    n_매수비율 = n_매수량 / n_매수량_기준봉 * 100 if n_매수량_기준봉 != 0 else 0
    n_체결강도 = (n_매수량 / n_매도량 * 100) if n_매도량 != 0 else 99999

    # 매도신호 검증
    li_매도신호 = list()
    li_매도신호_수치 = list()

    # # 1) 매도우세 검증
    # n_매도강도 = n_매도량_누적 / n_매수량_누적 * 100 if n_매수량_누적 != 0 else 0
    # # b_매도우세 = n_매도강도 > 100
    # b_매도우세 = False
    # li_매도신호.append(b_매도우세)
    # li_매도신호_수치.append(f'{n_매도강도:.0f}%')
    # dic_매도신호.update(n_매도량_누적_매도봇=n_매도량_누적, n_매수량_누적_매도봇=n_매수량_누적)
    # dic_매도신호.update(n_z매수량_매도봇=n_z매수량)

    # 1) 매수피크 검증
    # n_매수비율 = n_매수량 / n_매수량max * 100 if n_매수량max != 0 else 0
    # n_매수비율 = n_매수량 / n_매수량_기준봉 * 100 if n_매수량_기준봉 != 0 else 0
    # b_매수피크 = 70 < n_매수비율 < 100 and n_매수량 > n_매수량_1
    # b_매수피크 = n_매수비율 > 70 and n_매수량 > n_매수량_1 and s_이전봉시간 > s_매수시간
    # b_매수피크 = n_매수비율 > 50 and n_매수량 > n_매수량_1 and s_이전봉시간 > s_매수시간
    b_매수피크 = n_매수비율 > 70 and n_매수량 > n_매수량_1 and s_이전봉시간 > s_매수시간
    # b_매수피크 = (n_매수비율 > 70 or n_z매수량 > 2.0) and n_매수량 > n_매수량_1 and s_이전봉시간 > s_매수시간
    li_매도신호.append(b_매수피크)
    li_매도신호_수치.append(f'{n_매수비율:.0f}%')
    dic_매도신호.update(n_매수량max_매도봇=n_매수량max, n_매수량_기준봉_매도봇=n_매수량_기준봉, n_매수량_매도봇=n_매수량)

    # 2) 매수변동 검증
    # b_z매수량 = n_z매수량 > 2.0 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    # b_z매수량 = n_z매수량 > 2.0 and n_매수비율 > 10 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    # b_z매수량 = n_z매수량 > 2.0 and n_매수비율 > 20 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    # b_z매수량 = n_z매수량 > 2.5 and n_매수비율 > 10 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    # b_z매수량 = n_z매수량 > 3.0 and n_매수비율 > 10 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    # b_매수변동 = n_z매수량 > 2.0 and n_매수비율 > 10 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    b_매수변동 = n_z매수량 > 2.0 and n_매수비율 > 10 and n_체결강도 > 100 and n_z매수량 > n_z매수량_1 and s_이전봉시간 > s_매수시간
    li_매도신호.append(b_매수변동)
    li_매도신호_수치.append(f'{n_z매수량:.2f}')
    dic_매도신호.update(n_z매수량_매도봇=n_z매수량, n_매수비율_매도봇=n_매수비율, n_체결강도_매도봇=n_체결강도)
    dic_매도신호.update(n_매도량_누적_매도봇=n_매도량_누적, n_매수량_누적_매도봇=n_매수량_누적)

    # 3) 하락한계 검증
    b_하락한계 = n_수익률 < -3.2 if n_수익률 is not None else False
    li_매도신호.append(b_하락한계)
    li_매도신호_수치.append(f'{n_수익률:.1f}%')
    dic_매도신호.update(n_현재가_매도봇=n_현재가, n_수익률_매도봇=n_수익률)

    # 4) 타임아웃 검증
    n_타임아웃초 = 60 * 5
    dt_현재 = pd.Timestamp('now') if dt_일자시간 is None else dt_일자시간
    s_일자 = dt_현재.strftime('%Y%m%d')
    dt_매수시간 = pd.Timestamp(f'{s_일자} {s_매수시간}')
    n_경과초 = (dt_현재 - dt_매수시간).seconds
    b_타임아웃 = n_경과초 > n_타임아웃초 or dt_현재.strftime('%H:%M:%S') >= '15:15:00'
    li_매도신호.append(b_타임아웃)
    li_매도신호_수치.append(f'{n_경과초:.0f}초')
    dic_매도신호.update(n_경과초_매도봇=n_경과초)

    # 정보 전달용 dic 생성
    b_매도신호 = sum(li_매도신호) > 0
    s_매도사유 = li_신호종류[li_매도신호.index(True)] if b_매도신호 else None
    dic_매도신호.update(b_매도신호_매도봇=b_매도신호, li_매도신호_매도봇=li_매도신호,
                    li_매도신호_수치_매도봇=li_매도신호_수치, s_매도사유_매도봇=s_매도사유)

    return dic_매도신호

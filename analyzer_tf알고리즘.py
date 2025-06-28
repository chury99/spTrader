import os
import sys
import pandas as pd
import numpy as np

import UT_차트maker as Chart

# 그래프 한글 설정
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


def cal_z스코어(데이터):
    """ 입력 받은 데이터 기준으로 마지막 값의 z스코어 계산 후 리턴 """
    # nan 제거 (nan : 자기 자신과 같지 않음)
    ary_데이터 = np.array([x for x in 데이터 if x == x])
    if len(ary_데이터) < 20:
        return np.nan

    # 데이터 계산
    n_평균값 = ary_데이터.mean()
    n_표준편차 = ary_데이터.std()
    n_데이터 = ary_데이터[-1]

    # z스코어 계산
    n_z스코어 = (n_데이터 - n_평균값) / n_표준편차 if n_표준편차 != 0 else 0

    return n_z스코어


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
        # df_초봉['종목코드'] = [코드 for 코드 in df_초봉['종목코드'].unique() if 코드 is not None][0]
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


def make_매수신호(dic_매개변수, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 변수 정의
    li_신호종류 = ['z값', '금액', '강도']
    df_초봉 = dic_매개변수['df_초봉_매수봇']
    n_초봉 = dic_매개변수['n_초봉']

    # 현재봉 제외
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉

    # 데이터 길이 검증
    if len(df_초봉) == 0:
        li_매수신호 = [False] * len(li_신호종류)
        dic_매수신호 = dict(b_매수신호_매수봇=False, li_매수신호_매수봇=li_매수신호,
                        n_z매수량_매수봇='', n_z매도량_매수봇='', n_매수금액_매수봇='', n_체결강도_매수봇='',
                        li_신호종류_매수봇=li_신호종류, df_초봉_기준봉_매수봇=df_초봉[-1:])
        return dic_매수신호

    # 데이터 정의
    ary_매수량 = df_초봉['매수량'].values[-30:]
    ary_매도량 = df_초봉['매도량'].values[-30:]
    n_매수량 = df_초봉['매수량'].values[-1]
    n_매도량 = df_초봉['매도량'].values[-1]
    n_매수횟수 = df_초봉['매수횟수'].values[-1]
    n_매도횟수 = df_초봉['매도횟수'].values[-1]
    n_종가 = df_초봉['종가'].values[-1]
    df_초봉_기준봉 = df_초봉[-1:]

    # 매수신호 검증
    li_매수신호 = list()

    # 1) z스코어 검증
    n_z매수량 = cal_z스코어(ary_매수량)
    n_z매도량 = cal_z스코어(ary_매도량)
    # b_z스코어 = (n_z매수량 > 3 and n_z매도량 < 1)\
    #             if len(df_초봉) >= 30 and n_z매수량 is not None and n_z매도량 is not None else False
    b_z스코어 = (n_z매수량 > 3 and n_z매도량 < 1) if (n_z매수량 is not None and n_z매도량 is not None) else False
    li_매수신호.append(b_z스코어)

    # 2) 거래금액 검증
    n_매수금액 = n_종가 * n_매수량 / 10000
    b_거래금액 = n_매수금액 > 10000 * n_초봉
    li_매수신호.append(b_거래금액)

    # 3) 체결강도 검증
    n_체결강도 = (n_매수량 / n_매도량 * 100) if n_매도량 != 0 else 99999
    b_체결강도 = n_체결강도 > 500
    li_매수신호.append(b_체결강도)

    # 정보 전달용 dic 생성
    b_매수신호 = sum(li_매수신호) == len(li_매수신호)
    dic_매수신호 = dict(b_매수신호_매수봇=b_매수신호, li_매수신호_매수봇=li_매수신호,
                    n_z매수량_매수봇=n_z매수량, n_z매도량_매수봇=n_z매도량,
                    n_매수금액_매수봇=n_매수금액,
                    n_체결강도_매수봇=n_체결강도,
                    n_매수횟수_매수봇=n_매수횟수, n_매도횟수_매수봇=n_매도횟수,
                    li_신호종류_매수봇=li_신호종류, df_초봉_기준봉_매수봇=df_초봉_기준봉)

    return dic_매수신호


def make_매도신호(dic_매개변수, dt_일자시간=None):
    """ 입력 받은 초봉 데이터 기준으로 매수신호 생성 후 리턴 """
    # 변수 정의
    li_신호종류 = ['매도우세', '매수피크', '하락한계', '타임아웃']
    df_초봉_기준봉 = dic_매개변수['df_초봉_기준봉_매수봇']
    df_초봉 = dic_매개변수['df_초봉_매도봇']
    s_매수시간 = dic_매개변수['s_주문시간_매수봇']
    n_매수가 = dic_매개변수['n_매수단가_매도봇']
    n_현재가 = dic_매개변수['n_현재가_매도봇']

    # 기준봉 생성 (미입력 시)
    if df_초봉_기준봉 is None:
        df_초봉_기준봉 = df_초봉[df_초봉['체결시간'] < s_매수시간][-1:]

    # 현재봉 제외, 기준봉 추가
    df_초봉 = df_초봉[df_초봉.index < dt_일자시간].copy() if dt_일자시간 is not None else df_초봉
    df_초봉 = pd.concat([df_초봉, df_초봉_기준봉], axis=0).drop_duplicates('체결시간').sort_index()

    # 데이터 길이 검증
    if len(df_초봉) == 0:
        li_매도신호 = [False] * len(li_신호종류)
        dic_매도신호 = dict(b_매도신호_매도봇=False, li_매도신호_매도봇=li_매도신호,
                        n_현재가_매도봇='', n_수익률_매도봇='', n_경과초_매도봇='', n_매도량_누적_매도봇='', n_매수량_누적_매도봇='',
                        n_매수량max_매도봇='', n_매수량_매도봇='', li_신호종류_매도봇='', li_매도신호_수치_매도봇='')
        return dic_매도신호

    # 데이터 정의
    n_매수량 = df_초봉['매수량'].values[-1]
    if n_현재가 is None:
        ary_종가 = df_초봉['종가'].dropna().values
        n_현재가 = ary_종가[-1] if len(ary_종가) > 0 else None
    n_수익률 = (n_현재가 / n_매수가 - 1) * 100 - 0.2 if n_현재가 is not None else None
    df_초봉_매수이후 = df_초봉[df_초봉.index >= df_초봉_기준봉.index[0]] if len(df_초봉_기준봉) > 0 else df_초봉
    n_매수량_누적 = df_초봉_매수이후['매수량'].sum()
    n_매도량_누적 = df_초봉_매수이후['매도량'].sum()
    n_매수량max = df_초봉_매수이후[:-1]['매수량'].max()

    # 매도신호 검증
    li_매도신호 = list()
    li_매도신호_수치 = list()

    # 1) 매도우세 검증
    n_매도강도 = n_매도량_누적 / n_매수량_누적 * 100 if n_매수량_누적 != 0 else 0
    b_매도우세 = n_매도강도 > 100
    li_매도신호.append(b_매도우세)
    li_매도신호_수치.append(f'{n_매도강도:.0f}%')

    # 2) 매수피크 검증
    n_매수비율 = n_매수량 / n_매수량max * 100 if n_매수량max != 0 else 0
    # b_매수피크 = 50 < n_매수비율 < 100
    b_매수피크 = 70 < n_매수비율 < 100
    li_매도신호.append(b_매수피크)
    li_매도신호_수치.append(f'{n_매수비율:.0f}%')

    # 3) 하락한계 검증
    b_하락한계 = n_수익률 < -1.2 if n_수익률 is not None else False
    li_매도신호.append(b_하락한계)
    li_매도신호_수치.append(f'{n_수익률:.1f}%')

    # 4) 타임아웃 검증
    n_타임아웃초 = 60 * 5
    dt_현재 = pd.Timestamp('now') if dt_일자시간 is None else dt_일자시간
    s_일자 = dt_현재.strftime('%Y%m%d')
    dt_매수시간 = pd.Timestamp(f'{s_일자} {s_매수시간}')
    n_경과초 = (dt_현재 - dt_매수시간).seconds
    b_타임아웃 = n_경과초 > n_타임아웃초 or dt_현재.strftime('%H:%M:%S') >= '15:15:00'
    li_매도신호.append(b_타임아웃)
    li_매도신호_수치.append(f'{n_경과초:.0f}초')

    # 정보 전달용 dic 생성
    b_매도신호 = sum(li_매도신호) > 0
    s_매도사유 = li_신호종류[li_매도신호.index(True)] if b_매도신호 else None
    dic_매도신호 = dict(b_매도신호_매도봇=b_매도신호, li_매도신호_매도봇=li_매도신호,
                    n_현재가_매도봇=n_현재가, n_수익률_매도봇=n_수익률,
                    n_경과초_매도봇=n_경과초,
                    n_매도량_누적_매도봇=n_매도량_누적, n_매수량_누적_매도봇=n_매수량_누적,
                    n_매수량max_매도봇=n_매수량max, n_매수량_매도봇=n_매수량,
                    li_신호종류_매도봇=li_신호종류, li_매도신호_수치_매도봇=li_매도신호_수치,
                    s_매도사유_매도봇=s_매도사유)

    return dic_매도신호


def make_수익리포트(s_대상, dic_수익정보):
    """ 수익요약 데이터 기준으로 리포트 생성하여 리턴 """
    # 기준정보 정의
    s_일자 = dic_수익정보['s_일자']
    df_수익요약 = dic_수익정보['df_수익요약']
    folder_대상종목 = dic_수익정보['folder_대상종목']
    folder_캐시변환 = dic_수익정보['folder_캐시변환']
    folder_체결잔고 = dic_수익정보['folder_체결잔고']
    folder_주문정보 = dic_수익정보['folder_주문정보']

    # 리포트 생성 기준정보 생성
    if s_대상 == "실거래":
        df_대상종목_매매 = pd.read_pickle(os.path.join(folder_대상종목, f'df_대상종목_{s_일자}_매매.pkl'))\
                            if f'df_대상종목_{s_일자}_매매.pkl' in os.listdir(folder_대상종목) else None
        n_초봉 = df_대상종목_매매['초봉'].values[-1] if df_대상종목_매매 is not None else 5
        s_선정사유 = df_대상종목_매매['선정사유'].values[-1] if df_대상종목_매매 is not None else 'vi발동'
        dic_초봉 = pd.read_pickle(os.path.join(folder_캐시변환, f'dic_코드별_{n_초봉}초봉_{s_일자}.pkl'))
        try:
            df_체결잔고 = pd.read_csv(os.path.join(folder_체결잔고, f'체결잔고_{s_일자}.csv'), encoding='cp949')
        except FileNotFoundError:
            li_파일명 = [파일명 for 파일명 in os.listdir(folder_체결잔고) if '체결잔고' in 파일명 and '.csv' in 파일명]
            df_체결잔고 = pd.read_csv(os.path.join(folder_체결잔고, max(li_파일명)), encoding='cp949')[:0]
        df_체결잔고 = df_체결잔고[df_체결잔고['주문상태'] == '체결']
        df_주문정보 = pd.read_pickle(os.path.join(folder_주문정보, f'주문정보_{s_일자}.pkl'))\
                            if f'주문정보_{s_일자}.pkl' in os.listdir(folder_주문정보) else None
        # li_매도컬럼 = ['매도1매도우세', '매도2매수피크', '매도3하락한계', '매도4타임아웃']
        # df_주문정보['매도사유'] = df_주문정보[li_매도컬럼].apply(lambda row: li_매도컬럼[row.tolist().index(True)][-4:]
        #                                                     if True in row.tolist() else None, axis=1)\
        #                         if li_매도컬럼[0] in df_주문정보.columns else None
    if s_대상 == "백테스팅":
        pass
    dic_기준정보 = dict(df_수익요약=df_수익요약, n_초봉=n_초봉, s_선정사유=s_선정사유,
                    df_체결잔고=df_체결잔고, df_주문정보=df_주문정보)

    # 리포트 데이터 정의
    df_체결잔고_매수 = df_체결잔고[df_체결잔고['주문구분'] == '매수']
    li_종목코드 = [종목코드.replace('A', '') for 종목코드 in df_체결잔고_매수['종목코드'].unique()]
    n_종목수 = len(li_종목코드)
    n_max거래 = max(df_체결잔고_매수.groupby('종목코드')['주문구분'].count()) if len(df_체결잔고_매수) > 0 else 0
    n_테이블_세로 = 4
    # n_차트_세로 = n_테이블_세로 + n_종목수
    # n_차트_가로 = n_max거래 if n_max거래 > 0 else 1
    n_차트_가로 = 4
    li_차트_세로 = [(len(df_체결잔고_매수[df_체결잔고_매수['종목코드'] == f'A{종목코드}'])
                 + (n_차트_가로 - 1)) // n_차트_가로 for 종목코드 in li_종목코드]
    n_차트_세로 = n_테이블_세로 + sum(li_차트_세로)
    fig = plt.Figure(figsize=(16, n_차트_세로 * 2), tight_layout=False)

    # 리포트 생성 - 수익요약
    ax_수익요약 = fig.add_subplot(n_차트_세로, n_차트_가로, (1, n_테이블_세로 * n_차트_가로))
    ax_수익요약 = make_수익리포트_ax_수익요약(ax=ax_수익요약, dic_기준정보=dic_기준정보)

    # 리포트 생성 - 매수매도
    li_종목별줄수 = list()
    for i_종목순번, s_종목코드 in enumerate(li_종목코드):
        # 스케일 설정
        df_초봉 = dic_초봉[s_종목코드]
        dic_기준정보['df_초봉'] = df_초봉
        dic_기준정보['s_종목코드'] = s_종목코드
        dic_기준정보['n_시세_max'] = df_초봉['고가'].max()
        dic_기준정보['n_시세_min'] = df_초봉['저가'].min()
        dic_기준정보['n_거래량_max'] = max(df_초봉['매수량'].max(), df_초봉['매도량'].max())
        dic_기준정보['n_거래량_min'] = min(df_초봉['매수량'].min(), df_초봉['매도량'].min())
        dic_기준정보['n_거래횟수_max'] = max(df_초봉['매수횟수'].max(), df_초봉['매도횟수'].max(), df_초봉['체결횟수'].max())
        dic_기준정보['n_거래횟수_min'] = min(df_초봉['매수횟수'].min(), df_초봉['매도횟수'].min(), df_초봉['체결횟수'].min())

        # 매수매도 그래프 설정
        n_거래수_종목 = len(df_체결잔고_매수[df_체결잔고_매수['종목코드'] == f'A{s_종목코드}'])
        for i_거래순번 in range(n_거래수_종목):
            # 위치 설정
            dic_기준정보['n_거래순번'] = i_거래순번
            n_종목줄수 = sum(li_종목별줄수)
            n_차트위치 = (n_테이블_세로 * n_차트_가로) + (n_종목줄수 * n_차트_가로) + i_거래순번 + 1
            # 차트 생성
            ax_매수매도 = fig.add_subplot(n_차트_세로, n_차트_가로, n_차트위치)
            ax_매수매도 = make_수익리포트_ax_매수매도(ax=ax_매수매도, dic_기준정보=dic_기준정보)
            # 위치 카운트용 정보 업데이트
            if i_거래순번 + 1 == n_거래수_종목:
                li_종목별줄수.append((n_거래수_종목 + (n_차트_가로 - 1)) // n_차트_가로)

        # for i_가로 in range(n_max거래):
        #     if i_가로 >= len(df_체결잔고_매수[df_체결잔고_매수['종목코드'] == f'A{s_종목코드}']):
        #         continue
        #     dic_기준정보['n_거래순번'] = i_가로
        #     # n_차트위치 = n_테이블_세로 * n_차트_가로 + i_종목순번 * n_차트_가로 + i_가로 + 1
        #     n_차트위치 = (n_테이블_세로 * n_차트_가로) + i_종목순번 * n_차트_가로 + i_가로 + 1
        #     ax_매수매도 = fig.add_subplot(n_차트_세로, n_차트_가로, n_차트위치)
        #     ax_매수매도 = self.make_ax_매수매도(ax=ax_매수매도, dic_기준정보=dic_기준정보)

    return fig


def make_수익리포트_ax_수익요약(ax, dic_기준정보):
    """ df_수익요약 기준으로 리포트 테이블 변환 후 ax 리턴 """
    # 기준정보 정의
    df_수익요약 = dic_기준정보['df_수익요약']

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


def make_수익리포트_ax_매수매도(ax, dic_기준정보):
    """ df_체결잔고 기준으로 주가 변화 및 매수매도 시점 표기 후 ax 리턴 """
    # 기준정보 정의
    n_초봉 = dic_기준정보['n_초봉']
    s_선정사유 = dic_기준정보['s_선정사유']
    s_종목코드 = dic_기준정보['s_종목코드']
    n_거래순번 = dic_기준정보['n_거래순번']
    df_초봉 = dic_기준정보['df_초봉']
    s_일자 = df_초봉.index[-1].strftime('%Y%m%d')
    df_체결잔고 = dic_기준정보['df_체결잔고']
    df_체결잔고['종목코드'] = df_체결잔고['종목코드'].apply(lambda x: x.replace('A', ''))
    df_체결잔고_종목 = df_체결잔고[df_체결잔고['종목코드'] == s_종목코드].copy()
    df_체결잔고_종목['dt일시'] = df_체결잔고_종목['시간'].apply(lambda x: pd.Timestamp(f'{s_일자} {x}'))
    df_체결잔고_종목 = df_체결잔고_종목.set_index('dt일시')
    df_주문정보 = dic_기준정보['df_주문정보']
    df_주문정보_종목 = df_주문정보[df_주문정보['종목코드'] == s_종목코드].copy()
    df_주문정보_종목['dt일시'] = pd.to_datetime(df_주문정보_종목['일자'] + ' ' + df_주문정보_종목['주문시간'])
    df_주문정보_종목 = df_주문정보_종목.set_index('dt일시')
    s_종목명 = df_체결잔고_종목['종목명'].values[-1].strip()
    n_시세_max = dic_기준정보['n_시세_max']
    n_시세_min = dic_기준정보['n_시세_min']
    n_거래량_max = dic_기준정보['n_거래량_max']
    n_거래량_min = dic_기준정보['n_거래량_min']
    n_거래횟수_max = dic_기준정보['n_거래횟수_max']

    # 기본색상코드 정의
    dic_색상 = {'파랑': 'C0', '주황': 'C1', '녹색': 'C2', '빨강': 'C3', '보라': 'C4',
              '고동': 'C5', '분홍': 'C6', '회색': 'C7', '올리브': 'C8', '하늘': 'C9'}

    # 데이터 정리
    df_체결잔고_종목_매수 = df_체결잔고_종목[df_체결잔고_종목['주문구분'] == '매수']
    df_체결잔고_종목_매도 = df_체결잔고_종목[df_체결잔고_종목['주문구분'] == '매도']
    dt_매수시점 = df_체결잔고_종목_매수.index[n_거래순번]
    dt_매도시점 = df_체결잔고_종목_매도[df_체결잔고_종목_매도.index >= dt_매수시점].index[0]\
                    if len(df_체결잔고_종목_매도) > 0 else None
    dt_타임아웃 = dt_매수시점 + pd.Timedelta(minutes=5) if dt_매수시점 is not None else None
    n_매수가 = df_체결잔고_종목_매수['체결가'][dt_매수시점]\
                if len(df_체결잔고_종목_매수[df_체결잔고_종목_매수.index == dt_매수시점]) == 1\
                else max(df_체결잔고_종목_매수['체결가'][dt_매수시점])
    if dt_매도시점 is not None:
        n_매도가 = df_체결잔고_종목_매도['체결가'][dt_매도시점]\
                    if len(df_체결잔고_종목_매도[df_체결잔고_종목_매도.index == dt_매도시점]) == 1\
                    else max(df_체결잔고_종목_매도['체결가'][dt_매도시점])
    else:
        n_매도가 = None
    df_주문정보_종목_매도 = df_주문정보_종목[df_주문정보_종목.index <= dt_매도시점]
    df_주문정보_종목_매도 = df_주문정보_종목_매도[df_주문정보_종목_매도.index >= dt_매도시점 - pd.Timedelta(seconds=1)]
    s_매도사유 = df_주문정보_종목_매도['매도사유'].values[-1] if len(df_주문정보_종목_매도) > 0 else None
    df_초봉_차트 = df_초봉[df_초봉.index >= dt_매수시점 - pd.Timedelta(minutes=2)]
    df_초봉_차트 = df_초봉_차트[df_초봉_차트.index <= dt_매수시점 + pd.Timedelta(minutes=8)]

    # ax 생성
    s_차트명 = f'[ {s_종목명}({s_종목코드}) ] {n_초봉}초봉|{s_선정사유}' if n_거래순번 == 0 else ''
    ax.set_title(s_차트명, loc='left', fontsize=10, fontweight='bold')
    ax.vlines(df_초봉_차트.index, df_초봉_차트['저가'], df_초봉_차트['고가'], lw=0.5, color='black')
    ax_거래량 = ax.twinx()
    ax_거래량.plot(df_초봉_차트.index, df_초봉_차트['매수량'], lw=0.5, alpha=0.7, color=dic_색상['빨강'])
    ax_거래량.plot(df_초봉_차트.index, df_초봉_차트['매도량'], lw=0.5, alpha=0.7, color=dic_색상['파랑'])
    ax_체결횟수 = ax.twinx()
    # n_width = 2
    # ax_체결횟수.bar(df_초봉_차트.index - pd.Timedelta(seconds=n_width/2), df_초봉_차트['매수횟수'],
    #             width=pd.Timedelta(seconds=n_width), alpha=1, color=dic_색상['빨강'])
    # ax_체결횟수.bar(df_초봉_차트.index + pd.Timedelta(seconds=n_width/2), df_초봉_차트['매도횟수'],
    #             width=pd.Timedelta(seconds=n_width), alpha=1, color=dic_색상['파랑'])
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['매수횟수'], lw=1, alpha=0.2, color=dic_색상['빨강'])
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['매도횟수'], lw=1, alpha=0.2, color=dic_색상['파랑'])
    ax_체결횟수.plot(df_초봉_차트.index, df_초봉_차트['체결횟수'], lw=1, alpha=0.3, color='black')

    # 매수매도 시점 표기
    ax.axvline(dt_매수시점, lw=1, alpha=0.5, color=dic_색상['녹색']) if dt_매수시점 is not None else None
    ax.axvline(dt_매도시점, lw=1, alpha=0.5, color=dic_색상['보라']) if dt_매도시점 is not None else None
    # ax.axvline(dt_타임아웃, lw=1, alpha=0.5, color=dic_색상['회색']) if dt_타임아웃 is not None else None
    ax.axhline(n_매수가, lw=1, alpha=0.5, color=dic_색상['녹색']) if n_매수가 is not None else None
    ax.axhline(n_매도가, lw=1, alpha=0.5, color=dic_색상['보라']) if n_매도가 is not None else None
    ax.axvspan(df_초봉_차트.index[0], dt_매수시점, alpha=0.3, color=dic_색상['회색']) if dt_매수시점 is not None else None
    ax.axvspan(dt_타임아웃, df_초봉_차트.index[-1], alpha=0.3, color=dic_색상['회색']) if dt_타임아웃 is not None else None
    # ax.axvspan(dt_매수시점, dt_타임아웃, alpha=0.3, color=dic_색상['올리브']) if dt_타임아웃 is not None else None

    # 스케일 설정
    ax.set_ylim(n_시세_min, n_시세_max)
    ax.set_yticks([])
    ax.set_xticks([dt_매수시점], labels=[f'{dt_매수시점.strftime("%H:%M:%S")}-{s_매도사유}'], ha='center')
    ax_거래량.set_ylim(n_거래량_min, n_거래량_max * 1.1)
    ax_거래량.set_yticks([])
    ax_체결횟수.set_ylim(0, n_거래횟수_max * 1.1)
    ax_체결횟수.set_yticks([])

    return ax

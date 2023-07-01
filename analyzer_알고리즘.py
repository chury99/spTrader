import os
import sys
import pandas as pd

from tqdm import tqdm


def make_추가데이터_lstm(df):
    """ ohlcv 데이터 (ma 포함, na 삭제) 받아서 분석을 위한 추가 데이터 처리 후 ary_x, ary_y 데이터 리턴 """
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
            df_추가[f'ohe_{s_인자}_{s_구간명}'] = (df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료)

    li_인자_ohe = [f'상승률(%)_거래량_{i + 1}봉' for i in range(3)]
    for s_인자 in li_인자_ohe:
        li_구간 = [-float('inf'), 30, 50, 100, 150, 200, 300, float('inf')]
        li_구간명 = ['0', '50', '100', '150', '200', '300', '500']
        for s_구간명, n_구간_시작, n_구간_종료 in zip(li_구간명, li_구간[:-1], li_구간[1:]):
            df_추가[f'ohe_{s_인자}_{s_구간명}'] = (df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료)

    # 라벨 데이터 생성
    s_컬럼명_라벨 = '상승률(%)_고가'
    n_스펙_라벨 = 3
    df_추가[f'라벨_{s_컬럼명_라벨}'] = df_추가[s_컬럼명_라벨] > n_스펙_라벨

    # 데이터 잘라내기
    df_추가 = df_추가.dropna()[-1000:]

    # 상승률만 골라내기 ???
    li_컬럼명 = ['일자'] + [컬럼명 for 컬럼명 in df_추가.columns if '상승률(%)_' in 컬럼명 or '라벨_' in 컬럼명]
    df_상승률 = df_추가.loc[:, li_컬럼명].copy()

    # x, y 데이터 생성
    n_윈도우 = 20
    li_인자_ohe = [컬럼명 for 컬럼명 in df_상승률.columns if 'ohe_' in 컬럼명]
    s_라벨컬럼 = f'라벨_{s_컬럼명_라벨}'

    pass

    df = df_상승률
    n_window = n_윈도우
    li_col_x = li_인자_ohe
    col_y = s_라벨컬럼

    if len(df) <= n_window:
        print('*** len(ary_data) should be larger than n_window ***')
        dummy = np.nan
        return dummy, dummy, dummy

    ary_data_x = df.loc[:, li_col_x].values
    ary_data_y = df[col_y].values

    li_x = []
    li_y = []
    for i in range(len(df) - n_window):
        # 참고할 과거 데이터 생성
        li_x.append(ary_data_x[i:i + n_window])
        # 라벨 데이터 생성
        li_y.append(ary_data_y[i + n_window])

    # x, y array 설정
    ary_x = np.array(li_x)
    ary_x = ary_x.reshape(-1, n_window, len(li_col_x))  # 전체 데이터수 x 참고할 과거 데이터 수 x feature 수
    ary_y = np.array(li_y)

    # x array 마지막값 설정
    ary_x_last = np.array([ary_data_x[-1 * n_window:]])
    ary_x_last.reshape(-1, n_window, len(li_col_x))

    # print(ary_x.shape, ary_y.shape, ary_x_last.shape)
    return ary_x, ary_y, ary_x_last




    # x, y 데이터 생성
    n_window = 20
    li_features = [col for col in df_norm.columns if ('ohe_' in col)]
    s_label_label = f'label_{s_label}'
    ary_x, ary_y, ary_x_last = self.t.get_xy(df=df_norm,
                                             li_col_x=li_features, col_y=s_label_label, n_window=n_window)
    # ary_x값 이상 시 error 처리 (이상 시 3개 모두 np.nan으로 리턴)
    if (type(ary_x) == float) and np.isnan(ary_x):
        self.dic_args['LSTM_error'] = 1
        pd.to_pickle(self.dic_args, self.path_args)
        return

    # train, test 데이터 분리
    x_train, x_test, y_train, y_test = train_test_split(ary_x, ary_y, test_size=0.2, random_state=42)

    # train 데이터에 전체 데이터 지정
    # x_train, y_train = ary_x, ary_y

    # 변수값 저장
    self.dic_args['LSTM_n_window'] = n_window
    self.dic_args['LSTM_s_name'] = df['name'][0]
    self.dic_args['LSTM_li_features'] = li_features
    self.dic_args['LSTM_df'], self.dic_args['LSTM_df_norm'] = df, df_norm
    self.dic_args['LSTM_s_label'], self.dic_args['LSTM_n_spec_label'] = s_label, n_spec_label
    self.dic_args['LSTM_ary_x'], self.dic_args['LSTM_ary_y'] = ary_x, ary_y
    self.dic_args['LSTM_ary_x_last'] = ary_x_last
    self.dic_args['LSTM_x_train'], self.dic_args['LSTM_x_test'] = x_train, x_test
    self.dic_args['LSTM_y_train'], self.dic_args['LSTM_y_test'] = y_train, y_test
    pd.to_pickle(self.dic_args, self.path_args)

    pass

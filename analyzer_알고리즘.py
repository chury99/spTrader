import os
import sys
import pandas as pd
import numpy as np

# noinspection PyUnresolvedReferences
from sklearn.ensemble import RandomForestClassifier

# noinspection PyUnresolvedReferences
from sklearn.model_selection import train_test_split
# noinspection PyUnresolvedReferences
from tensorflow.keras.models import Sequential
# noinspection PyUnresolvedReferences
from tensorflow.keras.layers import LSTM, Dense
# noinspection PyUnresolvedReferences
from tensorflow.keras import optimizers
# noinspection PyUnresolvedReferences
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

from tqdm import tqdm


def make_추가데이터_rf(df):
    """ ohlcv 데이터 (ma 포함, na 삭제) 받아서 분석을 위한 추가 데이터 처리 후 df 리턴
    # 입력 df 컬럼 (20개) : 일자, 종목코드, 종목명, 시간, 시가, 고가, 저가, 종가, 거래량, 전일종가, 전일대비(%),
                          종가ma5, 종가ma10, 종가ma20, 종가ma60, 종가ma120, 거래량ma5, 거래량ma20, 거래량ma60, 거래량ma120 """
    # df_추가 생성
    df_추가 = df.loc[:, ['일자', '종목코드', '종목명', '시간', '시가', '고가', '저가', '종가', '거래량']].copy()

    # 상승률 생성
    df_추가['종가_1봉'] = df['종가'].shift(1)
    df_추가['상승률(%)'] = (df['종가'] / df_추가['종가_1봉'] - 1) * 100

    # 이평선 가져오기
    df_추가['종가ma5'] = df['종가ma5'].values
    df_추가['종가ma10'] = df['종가ma10'].values
    df_추가['종가ma20'] = df['종가ma20'].values
    df_추가['종가ma60'] = df['종가ma60'].values
    df_추가['종가ma120'] = df['종가ma120'].values

    df_추가['거래량ma5'] = df['거래량ma5'].values
    df_추가['거래량ma20'] = df['거래량ma20'].values
    df_추가['거래량ma60'] = df['거래량ma60'].values
    df_추가['거래량ma120'] = df['거래량ma120'].values

    # 상승률 생성
    df_추가['상승률(%)_시가'] = (df_추가['시가'] / df_추가['종가_1봉'] - 1) * 100
    df_추가['상승률(%)_고가'] = (df_추가['고가'] / df_추가['종가_1봉'] - 1) * 100
    df_추가['상승률(%)_저가'] = (df_추가['저가'] / df_추가['종가_1봉'] - 1) * 100
    df_추가['상승률(%)_종가'] = (df_추가['종가'] / df_추가['종가_1봉'] - 1) * 100
    df_추가['상승률(%)_거래량'] = (df_추가['거래량'] / df_추가['거래량'].shift(1)) * 100

    # 일 상승률 생성
    df_추가['전일대비(%)'] = df['전일대비(%)'].values

    # 차트 정보 생성
    df_추가['상승률(%)_몸통_상단'] = df_추가.loc[:, ['상승률(%)_시가', '상승률(%)_종가']].max(axis=1)
    df_추가['상승률(%)_몸통_하단'] = df_추가.loc[:, ['상승률(%)_시가', '상승률(%)_종가']].min(axis=1)
    df_추가['상승률(%)_꼬리_상단'] = df_추가['상승률(%)_고가'] - df_추가['상승률(%)_몸통_상단']
    df_추가['상승률(%)_꼬리_하단'] = df_추가['상승률(%)_저가'] - df_추가['상승률(%)_몸통_하단']

    # 이평선 배열 생성
    df_추가['종가ma_5v10'] = df_추가['종가ma5'] > df_추가['종가ma10']
    df_추가['종가ma_5v20'] = df_추가['종가ma5'] > df_추가['종가ma20']
    df_추가['종가ma_5v60'] = df_추가['종가ma5'] > df_추가['종가ma60']
    df_추가['종가ma_5v120'] = df_추가['종가ma5'] > df_추가['종가ma120']
    df_추가['종가ma_10v20'] = df_추가['종가ma10'] > df_추가['종가ma20']
    df_추가['종가ma_10v60'] = df_추가['종가ma10'] > df_추가['종가ma60']
    df_추가['종가ma_10v120'] = df_추가['종가ma10'] > df_추가['종가ma120']
    df_추가['종가ma_20v60'] = df_추가['종가ma20'] > df_추가['종가ma60']
    df_추가['종가ma_20v120'] = df_추가['종가ma20'] > df_추가['종가ma120']
    df_추가['종가ma_60v120'] = df_추가['종가ma60'] > df_추가['종가ma120']

    df_추가['종가ma_5v10v20'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma20'])
    df_추가['종가ma_5v10v60'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma60'])
    df_추가['종가ma_5v10v120'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma120'])
    df_추가['종가ma_5v20v60'] = (df_추가['종가ma5'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma60'])
    df_추가['종가ma_5v20v120'] = (df_추가['종가ma5'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma120'])
    df_추가['종가ma_5v60v120'] = (df_추가['종가ma5'] > df_추가['종가ma60']) & (df_추가['종가ma60'] > df_추가['종가ma120'])
    df_추가['종가ma_10v20v60'] = (df_추가['종가ma10'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma60'])
    df_추가['종가ma_10v20v120'] = (df_추가['종가ma10'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma120'])
    df_추가['종가ma_10v60v120'] = (df_추가['종가ma10'] > df_추가['종가ma60']) & (df_추가['종가ma60'] > df_추가['종가ma120'])
    df_추가['종가ma_20v60v120'] = (df_추가['종가ma20'] > df_추가['종가ma60']) & (df_추가['종가ma60'] > df_추가['종가ma120'])

    df_추가['종가ma_5v10v20v60'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma20']) \
                               & (df_추가['종가ma20'] > df_추가['종가ma60'])
    df_추가['종가ma_5v10v20v120'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma20'])\
                                & (df_추가['종가ma20'] > df_추가['종가ma120'])
    df_추가['종가ma_5v10v60v120'] = (df_추가['종가ma5'] > df_추가['종가ma10']) & (df_추가['종가ma10'] > df_추가['종가ma60'])\
                                & (df_추가['종가ma60'] > df_추가['종가ma120'])
    df_추가['종가ma_5v20v60v120'] = (df_추가['종가ma5'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma60'])\
                                & (df_추가['종가ma60'] > df_추가['종가ma120'])
    df_추가['종가ma_10v20v60v120'] = (df_추가['종가ma10'] > df_추가['종가ma20']) & (df_추가['종가ma20'] > df_추가['종가ma60'])\
                                 & (df_추가['종가ma60'] > df_추가['종가ma120'])

    df_추가['종가ma_5v10v20v60v120'] = (df_추가['종가ma5'] > df_추가['종가ma10']) \
                                   & (df_추가['종가ma10'] > df_추가['종가ma20']) \
                                   & (df_추가['종가ma20'] > df_추가['종가ma60']) \
                                   & (df_추가['종가ma60'] > df_추가['종가ma120'])

    # 이격도 생성
    df_추가['이격도(%)_종가vs종가ma5'] = (df_추가['종가'] / df['종가ma5'] - 1) * 100
    df_추가['이격도(%)_종가vs종가ma10'] = (df_추가['종가'] / df['종가ma10'] - 1) * 100
    df_추가['이격도(%)_종가vs종가ma20'] = (df_추가['종가'] / df['종가ma20'] - 1) * 100
    df_추가['이격도(%)_종가vs종가ma60'] = (df_추가['종가'] / df['종가ma60'] - 1) * 100
    df_추가['이격도(%)_종가vs종가ma120'] = (df_추가['종가'] / df['종가ma120'] - 1) * 100

    # 시간 구간 생성
    df_추가['타임존_A'] = (df_추가['시간'] >= '09:00:00') & (df_추가['시간'] < '10:30:00')
    df_추가['타임존_B'] = (df_추가['시간'] >= '10:30:00') & (df_추가['시간'] < '11:30:00')
    df_추가['타임존_C'] = (df_추가['시간'] >= '11:30:00') & (df_추가['시간'] < '13:00:00')
    df_추가['타임존_D'] = (df_추가['시간'] >= '13:00:00') & (df_추가['시간'] < '14:30:00')
    df_추가['타임존_E'] = (df_추가['시간'] >= '14:30:00') & (df_추가['시간'] < '15:30:00')

    return df_추가


# noinspection PyPep8Naming
def make_라벨데이터_rf(df, n_대기봉수=2):
    """ 데이터셋을 입력 받아서 라벨 데이터 생성 후 df 리턴 """
    # 상승률 생성 (봉수 이내에서 최고가, 최저가 상승률 확인)
    df['임시_고가_max'] = df['고가'].rolling(n_대기봉수).max().shift(-1 * (n_대기봉수 - 1))
    df['임시_저가_min'] = df['저가'].rolling(n_대기봉수).min().shift(-1 * (n_대기봉수 - 1))

    df['임시_상승률(%)_고가_max'] = (df['임시_고가_max'] / df['종가_1봉'] - 1) * 100
    df['임시_상승률(%)_저가_min'] = (df['임시_저가_min'] / df['종가_1봉'] - 1) * 100

    # 라벨 데이터 생성 (고가는 3% 이상 오르고, 저가는 -3% 밑으로 안 떨어지는 조건)
    s_라벨 = '라벨_상승여부'
    df[s_라벨] = (df['임시_상승률(%)_고가_max'] >= 3) & (df['임시_상승률(%)_저가_min'] > -3)
    df[s_라벨] = df[s_라벨].astype(int)

    df = df.loc[:, [컬럼명 for 컬럼명 in df.columns if '임시_' not in 컬럼명]]

    # 인자 값 shift (기준정보와 라벨은 시점 유지, 인자 값들은 1칸 shift)
    li_인자 = [컬럼명 for 컬럼명 in df.columns if 컬럼명 not in ['일자', '종목코드', '종목명', '시간', s_라벨]]
    for s_컬럼명 in li_인자:
        df[s_컬럼명] = df[s_컬럼명].shift(1)

    df = df.dropna()

    return df


# noinspection PyPep8Naming
def make_입력용xy_rf(df, n_학습일수=30):
    """ 추가 데이터 정리된 df 받아서 모델 입력을 위한 ary_x, ary_y 정리 후 dic 리턴 """
    # 데이터 길이 확인 (학습할 일수보다 데이터 일수가 적으면 종료, 입력된 데이터는 max 60일치)
    li_일자 = sorted(df['일자'].unique())
    if len(li_일자) < n_학습일수:
        return None

    # 데이터 잘라내기
    s_시작일 = li_일자[-1 * n_학습일수]
    df_데이터 = df[df['일자'] >= s_시작일]

    # 입력용 ary 생성 (인자 값은 이미 shift 되어 있음 - df 생성 시 shift 함)
    s_라벨 = '라벨_상승여부'
    li_인자 = [컬럼명 for 컬럼명 in df_데이터.columns if 컬럼명 not in ['일자', '종목코드', '종목명', '시간', s_라벨]]

    ary_x_학습 = df.loc[:, li_인자].values
    ary_y_학습 = df[s_라벨].values

    # dic에 저장
    dic_데이터셋 = dict()
    dic_데이터셋['ary_x_학습'] = ary_x_학습
    dic_데이터셋['ary_y_학습'] = ary_y_학습

    return dic_데이터셋


# noinspection PyPep8Naming
def make_모델_rf(dic_데이터셋, n_rf_트리=100, n_rf_깊이=20):
    """ dic 형태의 데이터셋을 받아서 lstm 모델 생성 후 리턴 """
    # 데이터 ary 설정
    ary_x_학습 = dic_데이터셋['ary_x_학습']
    ary_y_학습 = dic_데이터셋['ary_y_학습']

    # random forest 모델 생성
    모델 = RandomForestClassifier(n_estimators=n_rf_트리, max_depth=n_rf_깊이, random_state=42)

    # 모델 학습
    모델.fit(ary_x_학습, ary_y_학습)

    return 모델


# noinspection PyArgumentList
def make_추가데이터_lstm(df):
    """ ohlcv 데이터 (ma 포함, na 삭제) 받아서 분석을 위한 추가 데이터 처리 후 df 리턴 """
    # df_추가 생성
    df_추가 = df.loc[:, ['일자', '종목코드', '종목명', '시간', '시가', '고가', '저가', '종가', '거래량']].copy()

    # 상승률 생성
    df_추가['종가_1봉'] = df['종가'].shift(1)
    df_추가['상승률(%)'] = (df['종가'] / df_추가['종가_1봉'] - 1) * 100

    # ma값 가져오기
    df_추가['종가ma5'] = df['종가ma5'].values
    df_추가['종가ma10'] = df['종가ma10'].values
    df_추가['종가ma20'] = df['종가ma20'].values
    df_추가['종가ma60'] = df['종가ma60'].values
    df_추가['종가ma120'] = df['종가ma120'].values

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
            sri_컬럼 = pd.Series((df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료),
                               name=f'ohe_{s_인자}_{s_구간명}')
            df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    li_인자_ohe = [f'상승률(%)_거래량_{i + 1}봉' for i in range(3)]
    for s_인자 in li_인자_ohe:
        li_구간 = [-float('inf'), 30, 50, 100, 150, 200, 300, float('inf')]
        li_구간명 = ['0', '50', '100', '150', '200', '300', '500']
        for s_구간명, n_구간_시작, n_구간_종료 in zip(li_구간명, li_구간[:-1], li_구간[1:]):
            sri_컬럼 = pd.Series((df_추가[s_인자] > n_구간_시작) & (df_추가[s_인자] <= n_구간_종료),
                               name=f'ohe_{s_인자}_{s_구간명}')
            df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    # 라벨 데이터 생성
    s_컬럼명_라벨 = '상승률(%)_고가'
    n_스펙_라벨 = 3

    sri_컬럼 = pd.Series(df_추가[s_컬럼명_라벨] > n_스펙_라벨, name=f'라벨_{s_컬럼명_라벨}')
    df_추가 = pd.concat([df_추가, sri_컬럼], axis=1)

    # ohe 데이터 1봉 shift (모델에 입력 시 1봉 이전값 적용)
    li_컬럼명 = [컬럼명 for 컬럼명 in df_추가.columns if 'ohe_' in 컬럼명]
    for s_컬럼명 in li_컬럼명:
        df_추가[s_컬럼명] = df_추가[s_컬럼명].shift(1)

    # 데이터 잘라내기
    df_추가 = df_추가.dropna()[-1000:]

    # ohe 데이터 숫자로 변환 (기존은 bool 타입)
    li_컬럼명 = [컬럼명 for 컬럼명 in df_추가.columns if 'ohe_' in 컬럼명 or '라벨_' in 컬럼명]
    for s_컬럼명 in li_컬럼명:
        df_추가[s_컬럼명] = df_추가[s_컬럼명] * 1
        df_추가[s_컬럼명] = df_추가[s_컬럼명].astype(int)

    return df_추가


# noinspection PyArgumentList
def make_입력용xy_lstm(df):
    """ 추가 데이터 정리된 df 받아서 모델 입력을 위한 ary_x, ary_y 정리 후 dic 리턴 """
    # x, y 데이터 생성
    n_윈도우 = 20
    li_인자_ohe = [컬럼명 for 컬럼명 in df.columns if 'ohe_' in 컬럼명]
    s_라벨컬럼 = [컬럼명 for 컬럼명 in df.columns if '라벨_' in 컬럼명][0]

    # 데이터 길이가 윈도우 크기보다 작으면 종료
    if len(df) <= n_윈도우:
        return None

    else:
        ary_데이터_x = df.loc[:, li_인자_ohe].values
        ary_데이터_y = df[s_라벨컬럼].values

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


# noinspection PyPep8Naming
def make_모델_lstm(dic_데이터셋):
    """ dic 형태의 데이터셋을 받아서 lstm 모델 생성 후 리턴 """
    # 데이터 ary 설정
    ary_x = dic_데이터셋['ary_x']
    ary_y = dic_데이터셋['ary_y']
    ary_x_학습 = dic_데이터셋['ary_x_학습']
    ary_y_학습 = dic_데이터셋['ary_y_학습']
    ary_x_검증 = dic_데이터셋['ary_x_검증']
    ary_y_검증 = dic_데이터셋['ary_y_검증']

    # 모델 생성
    obj_형태 = ary_x.shape[1:]

    모델 = Sequential()
    모델.add(LSTM(200, input_shape=obj_형태, bias_initializer='he_normal', return_sequences=True))
    모델.add(LSTM(50, return_sequences=False, recurrent_dropout=0.5))
    모델.add(Dense(1, activation='sigmoid'))

    adam = optimizers.Adam(learning_rate=0.001)
    모델.compile(loss='mse', optimizer=adam, metrics=['Accuracy', 'Precision', 'Recall'])
    # Accuracy: (TP+TN) / (TP+TN+FP+FN) => 전체 중 True/False를 올바르게 예측한 수
    # Precision: (TP) / (TP+FP) => 모델이 Tru\e로 예측한 값 중 실제 True인 값 수
    # Recall: (TP) / (TP+FN) => 실제 True인 값을 모델이 True로 예측한 수
    #                     실제값
    #                  True    False
    # 예측값  True      TP       FP
    #        False      FN      TN

    # 조기종료 설정
    obj_조기종료 = EarlyStopping(monitor='val_loss', patience=20)

    # 학습 실행
    obj_hist = 모델.fit(ary_x_학습, ary_y_학습, epochs=1000, batch_size=20, validation_data=(ary_x_검증, ary_y_검증),
                         callbacks=[obj_조기종료], verbose=0)

    return 모델

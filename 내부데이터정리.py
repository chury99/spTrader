import os
import pandas as pd
import sqlite3
from tqdm import tqdm


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
def 기존db변환_일봉():
    """ (1회성) 기존 DB 파일 읽어와서 신규로 저장할 포맷에 맞춰 DB 파일 생성 후 저장 (일봉) """
    # 일봉 파일 처리
    folder_기존 = 'D:\\분봉데이터_local\\ohlcv_only'
    folder_신규 = 'D:\\ProjectWork\\spTrader\\데이터\\ohlcv'
    li_파일명_기존 = [파일명 for 파일명 in os.listdir(folder_기존) if 'ohlcv_only_day_' in 파일명 and '.db' in 파일명
                 and '2017' not in 파일명 and '2018' not in 파일명 and '2019' not in 파일명 and '2020' not in 파일명
                 and '2021' not in 파일명 and '2022' not in 파일명]

    for s_파일명_기존 in li_파일명_기존:
        con_기존 = sqlite3.connect(os.path.join(folder_기존, s_파일명_기존))
        s_년도 = s_파일명_기존.split('_')[3].replace('.db', '')
        파일명_신규 = f'ohlcv_일봉_{s_년도}.db'
        con_신규 = sqlite3.connect(os.path.join(folder_신규, 파일명_신규))

        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con_기존)
        for s_테이블명 in df_테이블명['name'].values:
            df_기존 = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con_기존, index_col='index')
            df_신규 = pd.DataFrame()
            df_신규['일자'] = df_기존['date'].apply(lambda x: x.replace('-', ''))
            df_신규['종목코드'] = df_기존['code'].values
            df_신규['종목명'] = df_기존['name'].values
            df_신규['시가'] = df_기존['open'].apply(lambda x: int(x))
            df_신규['고가'] = df_기존['high'].apply(lambda x: int(x))
            df_신규['저가'] = df_기존['low'].apply(lambda x: int(x))
            df_신규['종가'] = df_기존['close'].apply(lambda x: int(x))
            df_신규['거래량'] = df_기존['volume'].apply(lambda x: int(x))
            df_신규['거래대금(백만)'] = df_기존['cash'].apply(lambda x: int(float(x) * 100))  # 기존 db는 억 단위로 표기
            df_신규 = df_신규.sort_values(['일자', '종목코드']).reset_index(drop=True)

            s_년월 = s_테이블명.split('_')[3]
            df_신규.to_sql(f'ohlcv_일봉_{s_년월}', con=con_신규, index=False, if_exists='append')


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
def 기존db변환_분봉():
    """ (1회성) 기존 DB 파일 읽어와서 신규로 저장할 포맷에 맞춰 DB 파일 생성 후 저장 (분봉) """
    # 일봉 파일 처리
    folder_기존 = 'D:\\분봉데이터_local\\ohlcv_only'
    folder_신규 = 'D:\\ProjectWork\\spTrader\\데이터\\ohlcv'
    li_파일명_기존 = [파일명 for 파일명 in os.listdir(folder_기존) if 'ohlcv_only_min_' in 파일명 and '.db' in 파일명
                 and '2020_02' not in 파일명]

    for s_파일명_기존 in li_파일명_기존:
        con_기존 = sqlite3.connect(os.path.join(folder_기존, s_파일명_기존))
        s_년도 = s_파일명_기존.split('_')[3]
        s_월 = s_파일명_기존.split('_')[4].replace('.db', '')
        s_년_월 = f'{s_년도}_{s_월}'
        파일명_신규 = f'ohlcv_분봉_{s_년_월}.db'
        con_신규 = sqlite3.connect(os.path.join(folder_신규, 파일명_신규))

        df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con_기존)
        for s_테이블명 in tqdm(df_테이블명['name'].values, desc=f'{s_파일명_기존}'):
            df_기존 = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con_기존, index_col='index')
            if len(df_기존) == 0:
                continue
            else:
                df_신규 = pd.DataFrame()
                df_신규['일자'] = df_기존['date'].apply(lambda x: x.replace('-', ''))
                df_신규['종목코드'] = df_기존['code'].values
                df_신규['종목명'] = df_기존['name'].values
                df_신규['시간'] = df_기존['time'].values
                df_신규['시가'] = df_기존['open'].apply(lambda x: int(x))
                df_신규['고가'] = df_기존['high'].apply(lambda x: int(x))
                df_신규['저가'] = df_기존['low'].apply(lambda x: int(x))
                df_신규['종가'] = df_기존['close'].apply(lambda x: int(x))
                df_신규['거래량'] = df_기존['volume'].apply(lambda x: int(x))
                df_신규 = df_신규.sort_values(['종목코드', '시간'], ascending=[True, False]).reset_index(drop=True)

                s_일자 = s_테이블명.split('_')[3]
                df_신규.to_sql(f'ohlcv_분봉_{s_일자}', con=con_신규, index=False, if_exists='append')


#######################################################################################################################
if __name__ == "__main__":
    # 기존db변환_일봉()
    기존db변환_분봉()
    pass

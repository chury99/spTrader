import os
import sys
import pandas as pd
import json
import sqlite3

import pandas.errors
from tqdm import tqdm


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Collector:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_collector"]}_{self.s_오늘}.log')

        # 폴더 정의
        import UT_폴더정보
        dic_폴더정보 = UT_폴더정보.dic_폴더정보
        self.folder_ohlcv = dic_폴더정보['데이터|ohlcv']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']

        # 전체 항목 확인 (df_전체종목.pkl 확인)
        df_전체종목 = pd.read_pickle(os.path.join(self.folder_정보수집, 'df_전체종목.pkl'))
        self.li_종목코드_전체 = list(df_전체종목['종목코드'].values)

        # 제외 항목 확인 - 일봉 (데이터 길이 0 인 종목코드)
        try:
            li_종목코드_제외_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'li_종목코드_제외_일봉.pkl'))
        except FileNotFoundError:
            li_종목코드_제외_일봉 = list()
        self.li_종목코드_제외_일봉 = li_종목코드_제외_일봉

        # 제외 항목 확인 - 분봉 (데이터 길이 0 인 종목코드)
        try:
            li_종목코드_제외_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, 'li_종목코드_제외_분봉.pkl'))
        except FileNotFoundError:
            li_종목코드_제외_분봉 = list()
        self.li_종목코드_제외_분봉 = li_종목코드_제외_분봉

        # log 기록
        self.make_log(f'### 데이터 변환 시작 ###')

    def db저장_일봉(self):
        """ pkl 형식으로 임시 저장된 일봉 파일 읽어와서 db 파일 저장 """
        # pkl 읽어오기
        s_파일명 = 'df_ohlcv_일봉_임시.pkl'
        path_pkl_임시 = os.path.join(self.folder_정보수집, s_파일명)
        try:
            df_일봉 = pd.read_pickle(path_pkl_임시)
        except FileNotFoundError:
            self.make_log(f'[error] {s_파일명} 파일 미존재')
            return

        # 데이터 무결성 확인
        n_전체종목 = len(self.li_종목코드_전체)
        n_제외종목 = len(self.li_종목코드_제외_일봉)
        n_수집종목 = len(df_일봉['종목코드'].unique())
        if n_전체종목 != n_수집종목 + n_제외종목:
            # log 기록
            self.make_log(f'!!! 데이터 무결성 오류 - 전체 {n_전체종목:,}종목, 수집 {n_수집종목:,}종목, 제외 {n_제외종목:,}종목 !!!')
            return

        # 일별로 분리
        gr_일봉 = df_일봉.groupby('일자')
        for s_일자, df_일봉_신규 in gr_일봉:
            # db 파일 불러오기
            s_년도 = s_일자[:4]
            s_년월 = s_일자[:6]
            s_파일명 = f'ohlcv_일봉_{s_년도}.db'
            s_테이블명 = f'ohlcv_일봉_{s_년월}'
            con_일봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))
            try:
                df_일봉_기존 = pd.read_sql(f'SELECT * FROM {s_테이블명}', con=con_일봉)
            except pandas.errors.DatabaseError:
                df_일봉_기존 = pd.DataFrame()

            # df 합쳐서 저장
            df_일봉 = pd.concat([df_일봉_기존, df_일봉_신규], axis=0)
            df_일봉 = df_일봉.drop_duplicates(subset=['일자', '종목코드'], keep='last')
            df_일봉 = df_일봉.sort_values(['일자', '종목코드'], ascending=True)
            df_일봉.to_sql(s_테이블명, con=con_일봉, index=False, if_exists='replace')

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

        # 데이터 무결성 파일 생성
        s_파일내용 = f'데이터 무결성 확인 완료\n   전체 {n_전체종목:,}종목\n   수집 {n_수집종목:,}종목\n   제외 {n_제외종목:,}종목'
        with open(os.path.join(self.folder_정보수집, '데이터무결성_일봉.txt'), 'wt') as 파일:
            파일.writelines(s_파일내용)
        self.make_log(f'데이터 무결성 파일 생성 완료')

    def db저장_분봉(self):
        """ pkl 형식으로 임시 저장된 분봉 파일 읽어와서 db 파일 저장 """
        # pkl 읽어오기
        s_파일명 = 'df_ohlcv_분봉_임시.pkl'
        path_pkl_임시 = os.path.join(self.folder_정보수집, s_파일명)
        try:
            df_분봉 = pd.read_pickle(path_pkl_임시)
        except FileNotFoundError:
            self.make_log(f'[error] {s_파일명} 파일 미존재')
            return

        # 데이터 무결성 확인
        n_전체종목 = len(self.li_종목코드_전체)
        n_제외종목 = len(self.li_종목코드_제외_분봉)
        n_수집종목 = len(df_분봉['종목코드'].unique())
        if n_전체종목 != n_수집종목 + n_제외종목:
            # log 기록
            self.make_log(f'!!! 데이터 무결성 오류 - 전체 {n_전체종목:,}종목, 수집 {n_수집종목:,}종목, 제외 {n_제외종목:,}종목 !!!')
            return

        # 일별로 분리
        gr_분봉 = df_분봉.groupby('일자')
        for s_일자, df_분봉 in gr_분봉:
            # db 파일 연결
            s_년도 = s_일자[:4]
            s_월 = s_일자[4:6]
            s_파일명 = f'ohlcv_분봉_{s_년도}_{s_월}.db'
            s_테이블명 = f'ohlcv_분봉_{s_일자}'
            con_분봉 = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))

            # df 저장
            df_분봉 = df_분봉.drop_duplicates(subset=['종목코드', '시간'], keep='last')
            df_분봉 = df_분봉.sort_values(['종목코드', '시간'], ascending=[True, False])
            df_분봉.to_sql(s_테이블명, con=con_분봉, index=False, if_exists='replace')

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

        # 데이터 무결성 파일 생성
        s_파일내용 = f'데이터 무결성 확인 완료\n   전체 {n_전체종목:,}종목\n   수집 {n_수집종목:,}종목\n   제외 {n_제외종목:,}종목'
        with open(os.path.join(self.folder_정보수집, '데이터무결성_분봉.txt'), 'wt') as 파일:
            파일.writelines(s_파일내용)
        self.make_log(f'데이터 무결성 파일 생성 완료')

    def 캐시저장_일봉(self):
        """ db 파일 불러와서 종목별 분류 후 pkl 파일 저장 (일봉) """
        # 전체 db 확인
        li_파일명_일봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_일봉_' in 파일명 and '.db' in 파일명]
        li_df_테이블명 = list()
        for 파일명 in li_파일명_일봉:
            con = sqlite3.connect(os.path.join(self.folder_ohlcv, 파일명))
            df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
            li_df_테이블명.append(df_테이블명)
        df_테이블명 = pd.concat(li_df_테이블명, axis=0).sort_values('name')
        li_테이블명_전체 = list(df_테이블명['name'].values)

        # 저장된 캐시파일 확인
        li_파일명_캐시 = [파일명 for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_일봉_' in 파일명 and '.pkl' in 파일명]
        li_테이블명_캐시 = [파일명.replace('dic_코드별_', 'ohlcv_').replace('.pkl', '') for 파일명 in li_파일명_캐시]

        # 변환 대상 선정
        li_테이블명_대상 = [테이블명 for 테이블명 in li_테이블명_전체 if 테이블명 not in li_테이블명_캐시]
        li_테이블명_대상 = [max(li_테이블명_전체)] if len(li_테이블명_대상) == 0 else li_테이블명_대상

        # 캐시파일 생성
        for s_테이블명 in li_테이블명_대상:
            # 불러올 db 테이블 대상 선정
            s_년월 = s_테이블명.split('_')[2]
            dt_대상일 = pd.Timestamp(f'{s_년월}01')
            li_대상일 = [dt_대상일 - pd.DateOffset(months=n_개월) for n_개월 in range(12)]
            li_테이블명_리딩 = [f'ohlcv_일봉_{dt.strftime("%Y%m")}' for dt in li_대상일]

            # db 파일 불러오기
            li_df_전체 = list()
            for s_테이블명_리딩 in li_테이블명_리딩:
                con = sqlite3.connect(os.path.join(self.folder_ohlcv, f'{s_테이블명_리딩[:-2]}.db'))
                try:
                    df_리딩 = pd.read_sql(f'SELECT * FROM {s_테이블명_리딩}', con=con)
                    li_df_전체.append(df_리딩)
                except pandas.errors.DatabaseError:
                    continue
            df_전체 = pd.concat(li_df_전체, axis=0)

            # 종목별 분류
            dic_일봉 = dict()
            gr_전체 = df_전체.groupby('종목코드')
            for s_종목코드, df_종목별 in tqdm(gr_전체, desc=f'캐시저장|{s_테이블명}'):
                # df 정리 (오름차순)
                df_종목별 = df_종목별.drop_duplicates().sort_values('일자', ascending=True)
                # 추가 데이터 생성
                df_종목별['전일종가'] = df_종목별['종가'].shift(1)
                df_종목별['전일대비(%)'] = (df_종목별['종가'] / df_종목별['전일종가'] - 1) * 100
                df_종목별['종가ma5'] = df_종목별['종가'].rolling(5).mean()
                df_종목별['종가ma10'] = df_종목별['종가'].rolling(10).mean()
                df_종목별['종가ma20'] = df_종목별['종가'].rolling(20).mean()
                df_종목별['종가ma60'] = df_종목별['종가'].rolling(60).mean()
                df_종목별['종가ma120'] = df_종목별['종가'].rolling(120).mean()
                df_종목별['거래량ma5'] = df_종목별['거래량'].rolling(5).mean()
                df_종목별['거래량ma20'] = df_종목별['거래량'].rolling(20).mean()
                df_종목별['거래량ma60'] = df_종목별['거래량'].rolling(60).mean()
                df_종목별['거래량ma120'] = df_종목별['거래량'].rolling(120).mean()
                # 해당 월만 골라내기
                df_종목별['년월'] = df_종목별['일자'].apply(lambda x: x[:6])
                df_종목별 = df_종목별[df_종목별['년월'] == s_년월]
                df_종목별 = df_종목별.drop('년월', axis=1)
                # dic 할당
                dic_일봉[s_종목코드] = df_종목별

            # pkl 저장
            pd.to_pickle(dic_일봉, os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_년월}.pkl'))

            # log 기록
            self.make_log(f'{s_년월} 데이터 저장 완료')

    def 캐시저장_분봉(self):
        """ db 파일 불러와서 종목별 분류 후 pkl 파일 저장 (분봉) """
        # 전체 db 확인
        li_파일명_분봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_분봉_' in 파일명 and '.db' in 파일명]
        li_df_테이블명 = list()
        for 파일명 in li_파일명_분봉:
            con = sqlite3.connect(os.path.join(self.folder_ohlcv, 파일명))
            df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
            li_df_테이블명.append(df_테이블명)
        df_테이블명 = pd.concat(li_df_테이블명, axis=0).sort_values('name')
        li_테이블명_전체 = list(df_테이블명['name'].values)

        # 저장된 캐시파일 확인
        li_파일명_캐시 = [파일명 for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_분봉_' in 파일명 and '.pkl' in 파일명]
        li_테이블명_캐시 = [파일명.replace('dic_코드별_', 'ohlcv_').replace('.pkl', '') for 파일명 in li_파일명_캐시]

        # 변환 대상 선정
        li_테이블명_대상 = [테이블명 for 테이블명 in li_테이블명_전체 if 테이블명 not in li_테이블명_캐시]

        # 캐시파일 생성
        for s_테이블명 in li_테이블명_대상:
            # 불러올 db 테이블 대상 선정
            s_년월일 = s_테이블명.split('_')[2]
            s_년월 = s_년월일[:6]
            dt_대상일 = pd.Timestamp(s_년월일)
            li_대상일 = [dt_대상일 - pd.Timedelta(days=n_일) for n_일 in range(12)]
            li_테이블명_리딩 = [f'ohlcv_분봉_{dt.strftime("%Y%m%d")}' for dt in li_대상일]

            # db 파일 불러오기
            li_df_전체 = list()
            for s_테이블명_리딩 in li_테이블명_리딩:
                s_파일명 = f'{s_테이블명_리딩[:-4]}_{s_테이블명_리딩[-4:-2]}.db'
                con = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))
                try:
                    df_리딩 = pd.read_sql(f'SELECT * FROM {s_테이블명_리딩}', con=con)
                    li_df_전체.append(df_리딩)
                except pandas.errors.DatabaseError:
                    continue
            df_전체 = pd.concat(li_df_전체, axis=0)

            # 종목별 분류
            dic_분봉 = dict()
            dic_코드별_일봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_년월}.pkl'))
            gr_전체 = df_전체.groupby('종목코드')
            for s_종목코드, df_종목별 in tqdm(gr_전체, desc=f'캐시저장|{s_테이블명}'):
                # df 정리 (오름차순)
                df_종목별 = df_종목별.drop_duplicates().sort_values(['일자', '시간'], ascending=True)
                # 추가 데이터 생성 (!!!주의!!! 해당 날짜 이외 데이터는 신뢰성 없음)
                df_일봉 = dic_코드별_일봉[s_종목코드] if s_종목코드 in dic_코드별_일봉.keys() else None
                dic_전일종가 = df_일봉.set_index('일자').to_dict()['전일종가'] if df_일봉 is not None else None
                try:
                    df_종목별['전일종가'] = dic_전일종가[s_년월일] if df_일봉 is not None else None
                except KeyError:
                    df_종목별['전일종가'] = None
                df_종목별['전일대비(%)'] = (df_종목별['종가'] / df_종목별['전일종가'] - 1) * 100 if df_일봉 is not None else None
                df_종목별['종가ma5'] = df_종목별['종가'].rolling(5).mean()
                df_종목별['종가ma10'] = df_종목별['종가'].rolling(10).mean()
                df_종목별['종가ma20'] = df_종목별['종가'].rolling(20).mean()
                df_종목별['종가ma60'] = df_종목별['종가'].rolling(60).mean()
                df_종목별['종가ma120'] = df_종목별['종가'].rolling(120).mean()
                df_종목별['거래량ma5'] = df_종목별['거래량'].rolling(5).mean()
                df_종목별['거래량ma20'] = df_종목별['거래량'].rolling(20).mean()
                df_종목별['거래량ma60'] = df_종목별['거래량'].rolling(60).mean()
                df_종목별['거래량ma120'] = df_종목별['거래량'].rolling(120).mean()
                # 해당 일만 골라내기
                df_종목별 = df_종목별[df_종목별['일자'] == s_년월일]
                # dic 할당
                dic_분봉[s_종목코드] = df_종목별

            # pkl 저장
            pd.to_pickle(dic_분봉, os.path.join(self.folder_캐시변환, f'dic_코드별_분봉_{s_년월일}.pkl'))

            # log 기록
            self.make_log(f'{s_년월일} 데이터 저장 완료')

    def 캐시저장_10분봉(self):
        """ 분봉 db 파일 불러와서 종목별 분류 후 10분봉 pkl 파일 저장 """
        # 전체 db 확인
        li_파일명_분봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_분봉_' in 파일명 and '.db' in 파일명]
        li_df_테이블명 = list()
        for 파일명 in li_파일명_분봉:
            con = sqlite3.connect(os.path.join(self.folder_ohlcv, 파일명))
            df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
            li_df_테이블명.append(df_테이블명)
        df_테이블명 = pd.concat(li_df_테이블명, axis=0).sort_values('name')
        li_테이블명_전체 = list(df_테이블명['name'].values)

        # 저장된 캐시파일 확인
        li_파일명_캐시 = [파일명 for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명]
        li_테이블명_캐시 = [파일명.replace('dic_코드별_10', 'ohlcv_').replace('.pkl', '') for 파일명 in li_파일명_캐시]

        # 변환 대상 선정
        li_테이블명_대상 = [테이블명 for 테이블명 in li_테이블명_전체 if 테이블명 not in li_테이블명_캐시]

        # 캐시파일 생성
        for s_테이블명 in li_테이블명_대상:
            # 불러올 db 테이블 대상 선정
            s_년월일 = s_테이블명.split('_')[2]
            s_년월 = s_년월일[:6]
            dt_대상일 = pd.Timestamp(s_년월일)
            li_대상일 = [dt_대상일 - pd.Timedelta(days=n_일) for n_일 in range(12)]
            li_테이블명_리딩 = [f'ohlcv_분봉_{dt.strftime("%Y%m%d")}' for dt in li_대상일]

            # db 파일 불러오기
            li_df_전체 = list()
            for s_테이블명_리딩 in li_테이블명_리딩:
                s_파일명 = f'{s_테이블명_리딩[:-4]}_{s_테이블명_리딩[-4:-2]}.db'
                con = sqlite3.connect(os.path.join(self.folder_ohlcv, s_파일명))
                try:
                    df_리딩 = pd.read_sql(f'SELECT * FROM {s_테이블명_리딩}', con=con)
                    li_df_전체.append(df_리딩)
                except pandas.errors.DatabaseError:
                    continue
            df_전체 = pd.concat(li_df_전체, axis=0)

            # 종목별 분류
            dic_10분봉 = dict()
            dic_코드별_일봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_년월}.pkl'))
            gr_전체 = df_전체.groupby('종목코드')
            for s_종목코드, df_종목별 in tqdm(gr_전체, desc=f'캐시저장|{s_테이블명}(10분봉)'):
                # df 정리 (오름차순)
                df_종목별 = df_종목별.drop_duplicates().sort_values(['일자', '시간'], ascending=True)
                # 인덱스 설정
                df_종목별['일자시간'] = df_종목별['일자'] + ' ' + df_종목별['시간']
                df_종목별['일자시간'] = pd.to_datetime(df_종목별['일자시간'], format='%Y%m%d %H:%M:%S')
                df_종목별 = df_종목별.set_index(keys='일자시간').sort_index(ascending=True)
                # 10분봉 변환
                df_리샘플 = df_종목별.resample('10T')
                df_10분봉 = df_리샘플.first()
                df_10분봉['시가'] = df_리샘플['시가'].first()
                df_10분봉['고가'] = df_리샘플['고가'].max()
                df_10분봉['저가'] = df_리샘플['저가'].min()
                df_10분봉['종가'] = df_리샘플['종가'].last()
                df_10분봉['거래량'] = df_리샘플['거래량'].sum()

                df_10분봉 = df_10분봉.dropna(subset=['시간'])
                df_10분봉['시간'] = df_10분봉.index.strftime('%H:%M:%S')

                # 추가 데이터 생성 (!!!주의!!! 해당 날짜 이외 데이터는 신뢰성 없음)
                df_일봉 = dic_코드별_일봉[s_종목코드] if s_종목코드 in dic_코드별_일봉.keys() else None
                dic_전일종가 = df_일봉.set_index('일자').to_dict()['전일종가'] if df_일봉 is not None else None
                try:
                    df_10분봉['전일종가'] = dic_전일종가[s_년월일] if df_일봉 is not None else None
                except KeyError:
                    df_10분봉['전일종가'] = None
                df_10분봉['전일대비(%)'] = (df_10분봉['종가'] / df_10분봉['전일종가'] - 1) * 100 if df_일봉 is not None else None
                df_10분봉['종가ma5'] = df_10분봉['종가'].rolling(5).mean()
                df_10분봉['종가ma10'] = df_10분봉['종가'].rolling(10).mean()
                df_10분봉['종가ma20'] = df_10분봉['종가'].rolling(20).mean()
                df_10분봉['종가ma60'] = df_10분봉['종가'].rolling(60).mean()
                df_10분봉['종가ma120'] = df_10분봉['종가'].rolling(120).mean()
                df_10분봉['거래량ma5'] = df_10분봉['거래량'].rolling(5).mean()
                df_10분봉['거래량ma20'] = df_10분봉['거래량'].rolling(20).mean()
                df_10분봉['거래량ma60'] = df_10분봉['거래량'].rolling(60).mean()
                df_10분봉['거래량ma120'] = df_10분봉['거래량'].rolling(120).mean()
                # 해당 일만 골라내기
                df_10분봉 = df_10분봉[df_10분봉['일자'] == s_년월일]
                # dic 할당
                dic_10분봉[s_종목코드] = df_10분봉

            # pkl 저장
            pd.to_pickle(dic_10분봉, os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_년월일}.pkl'))

            # log 기록
            self.make_log(f'{s_년월일} 데이터 저장 완료')

    ###################################################################################################################
    def make_log(self, s_text, li_loc=None):
        """ 입력 받은 s_text에 시간 붙여서 self.path_log에 저장 """
        # 정보 설정
        s_시각 = pd.Timestamp('now').strftime('%H:%M:%S')
        s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
        s_모듈 = sys._getframe(1).f_code.co_name

        # log 생성
        s_log = f'[{s_시각}] {s_파일} | {s_모듈} | {s_text}'

        # log 출력
        li_출력 = ['콘솔', '파일'] if li_loc is None else li_loc
        if '콘솔' in li_출력:
            print(s_log)
        if '파일' in li_출력:
            with open(self.path_log, mode='at', encoding='cp949') as file:
                file.write(f'{s_log}\n')


#######################################################################################################################
if __name__ == "__main__":
    c = Collector()

    c.db저장_일봉()
    c.db저장_분봉()
    c.캐시저장_일봉()
    c.캐시저장_분봉()
    c.캐시저장_10분봉()

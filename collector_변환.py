import os
import sys
import pandas as pd
import json
import re
import sqlite3

import pandas.errors
from tqdm import tqdm

import analyzer_tf알고리즘 as Logic


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
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_ohlcv = dic_폴더정보['데이터|ohlcv']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_전체종목 = dic_폴더정보['데이터|전체종목']
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_체결정보 = dic_폴더정보['데이터|체결정보']
        self.folder_실시간 = dic_폴더정보['이력|실시간']
        os.makedirs(self.folder_체결정보, exist_ok=True)

        # 수집정보 가져오기
        self.dic_수집정보_일봉 = pd.read_pickle(os.path.join(self.folder_정보수집, f'dic_수집정보_일봉.pkl'))
        self.dic_수집정보_분봉 = pd.read_pickle(os.path.join(self.folder_정보수집, f'dic_수집정보_분봉.pkl'))

        # # 전체 항목 확인
        self.li_전체종목_일봉 = self.dic_수집정보_일봉['df_전체종목']['종목코드'].to_list()
        self.li_전체종목_분봉 = self.dic_수집정보_분봉['df_전체종목']['종목코드'].to_list()

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
        n_전체종목 = len(self.li_전체종목_일봉)
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
        n_전체종목 = len(self.li_전체종목_분봉)
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
        li_테이블명_전체 = df_테이블명['name'].to_list()

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
        li_테이블명_전체 = df_테이블명['name'].to_list()

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

    def 캐시저장_3분봉(self):
        """ 분봉 db 파일 불러와서 종목별 분류 후 3분봉 pkl 파일 저장 """
        # 전체 db 확인
        li_파일명_분봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_분봉_' in 파일명 and '.db' in 파일명]
        li_df_테이블명 = list()
        for 파일명 in li_파일명_분봉:
            con = sqlite3.connect(os.path.join(self.folder_ohlcv, 파일명))
            df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
            li_df_테이블명.append(df_테이블명)
        df_테이블명 = pd.concat(li_df_테이블명, axis=0).sort_values('name')
        li_테이블명_전체 = df_테이블명['name'].to_list()

        # 저장된 캐시파일 확인
        li_파일명_캐시 = [파일명 for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_3분봉_' in 파일명 and '.pkl' in 파일명]
        li_테이블명_캐시 = [파일명.replace('dic_코드별_3', 'ohlcv_').replace('.pkl', '') for 파일명 in li_파일명_캐시]

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
            dic_3분봉 = dict()
            dic_코드별_일봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_년월}.pkl'))
            gr_전체 = df_전체.groupby('종목코드')
            for s_종목코드, df_종목별 in tqdm(gr_전체, desc=f'캐시저장|{s_테이블명}(3분봉)'):
                # df 정리 (오름차순)
                df_종목별 = df_종목별.drop_duplicates().sort_values(['일자', '시간'], ascending=True)
                # 인덱스 설정
                df_종목별['일자시간'] = df_종목별['일자'] + ' ' + df_종목별['시간']
                df_종목별['일자시간'] = pd.to_datetime(df_종목별['일자시간'], format='%Y%m%d %H:%M:%S')
                df_종목별 = df_종목별.set_index(keys='일자시간').sort_index(ascending=True)
                # 3분봉 변환
                df_리샘플 = df_종목별.resample('3T')
                df_3분봉 = df_리샘플.first()
                df_3분봉['시가'] = df_리샘플['시가'].first()
                df_3분봉['고가'] = df_리샘플['고가'].max()
                df_3분봉['저가'] = df_리샘플['저가'].min()
                df_3분봉['종가'] = df_리샘플['종가'].last()
                df_3분봉['거래량'] = df_리샘플['거래량'].sum()

                df_3분봉 = df_3분봉.dropna(subset=['시간'])
                df_3분봉['시간'] = df_3분봉.index.strftime('%H:%M:%S')

                # 15:30 이후 데이터 삭제
                df_3분봉 = df_3분봉[df_3분봉['시간'] <= '15:30:00']

                # 추가 데이터 생성 (!!!주의!!! 해당 날짜 이외 데이터는 신뢰성 없음)
                df_일봉 = dic_코드별_일봉[s_종목코드] if s_종목코드 in dic_코드별_일봉.keys() else None
                dic_전일종가 = df_일봉.set_index('일자').to_dict()['전일종가'] if df_일봉 is not None else None
                try:
                    df_3분봉['전일종가'] = dic_전일종가[s_년월일] if df_일봉 is not None else None
                except KeyError:
                    df_3분봉['전일종가'] = None
                df_3분봉['전일대비(%)'] = (df_3분봉['종가'] / df_3분봉['전일종가'] - 1) * 100 if df_일봉 is not None else None
                df_3분봉['종가ma5'] = df_3분봉['종가'].rolling(5).mean()
                df_3분봉['종가ma10'] = df_3분봉['종가'].rolling(10).mean()
                df_3분봉['종가ma20'] = df_3분봉['종가'].rolling(20).mean()
                df_3분봉['종가ma60'] = df_3분봉['종가'].rolling(60).mean()
                df_3분봉['종가ma120'] = df_3분봉['종가'].rolling(120).mean()
                df_3분봉['거래량ma5'] = df_3분봉['거래량'].rolling(5).mean()
                df_3분봉['거래량ma20'] = df_3분봉['거래량'].rolling(20).mean()
                df_3분봉['거래량ma60'] = df_3분봉['거래량'].rolling(60).mean()
                df_3분봉['거래량ma120'] = df_3분봉['거래량'].rolling(120).mean()
                # 해당 일만 골라내기
                df_3분봉 = df_3분봉[df_3분봉['일자'] == s_년월일]
                # dic 할당
                dic_3분봉[s_종목코드] = df_3분봉

            # pkl 저장
            pd.to_pickle(dic_3분봉, os.path.join(self.folder_캐시변환, f'dic_코드별_3분봉_{s_년월일}.pkl'))

            # log 기록
            self.make_log(f'{s_년월일} 데이터 저장 완료')

    def 캐시저장_5분봉(self):
        """ 분봉 db 파일 불러와서 종목별 분류 후 5분봉 pkl 파일 저장 """
        # 전체 db 확인
        li_파일명_분봉 = [파일명 for 파일명 in os.listdir(self.folder_ohlcv) if 'ohlcv_분봉_' in 파일명 and '.db' in 파일명]
        li_df_테이블명 = list()
        for 파일명 in li_파일명_분봉:
            con = sqlite3.connect(os.path.join(self.folder_ohlcv, 파일명))
            df_테이블명 = pd.read_sql(f'SELECT name FROM sqlite_master WHERE type="table"', con=con)
            li_df_테이블명.append(df_테이블명)
        df_테이블명 = pd.concat(li_df_테이블명, axis=0).sort_values('name')
        li_테이블명_전체 = df_테이블명['name'].to_list()

        # 저장된 캐시파일 확인
        li_파일명_캐시 = [파일명 for 파일명 in os.listdir(self.folder_캐시변환)
                     if 'dic_코드별_5분봉_' in 파일명 and '.pkl' in 파일명]
        li_테이블명_캐시 = [파일명.replace('dic_코드별_5', 'ohlcv_').replace('.pkl', '') for 파일명 in li_파일명_캐시]

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
            dic_5분봉 = dict()
            dic_코드별_일봉 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_년월}.pkl'))
            gr_전체 = df_전체.groupby('종목코드')
            for s_종목코드, df_종목별 in tqdm(gr_전체, desc=f'캐시저장|{s_테이블명}(5분봉)'):
                # df 정리 (오름차순)
                df_종목별 = df_종목별.drop_duplicates().sort_values(['일자', '시간'], ascending=True)
                # 인덱스 설정
                df_종목별['일자시간'] = df_종목별['일자'] + ' ' + df_종목별['시간']
                df_종목별['일자시간'] = pd.to_datetime(df_종목별['일자시간'], format='%Y%m%d %H:%M:%S')
                df_종목별 = df_종목별.set_index(keys='일자시간').sort_index(ascending=True)
                # 5분봉 변환
                df_리샘플 = df_종목별.resample('5T')
                df_5분봉 = df_리샘플.first()
                df_5분봉['시가'] = df_리샘플['시가'].first()
                df_5분봉['고가'] = df_리샘플['고가'].max()
                df_5분봉['저가'] = df_리샘플['저가'].min()
                df_5분봉['종가'] = df_리샘플['종가'].last()
                df_5분봉['거래량'] = df_리샘플['거래량'].sum()

                df_5분봉 = df_5분봉.dropna(subset=['시간'])
                df_5분봉['시간'] = df_5분봉.index.strftime('%H:%M:%S')

                # 15:30 이후 데이터 삭제
                df_5분봉 = df_5분봉[df_5분봉['시간'] <= '15:30:00']

                # 추가 데이터 생성 (!!!주의!!! 해당 날짜 이외 데이터는 신뢰성 없음)
                df_일봉 = dic_코드별_일봉[s_종목코드] if s_종목코드 in dic_코드별_일봉.keys() else None
                dic_전일종가 = df_일봉.set_index('일자').to_dict()['전일종가'] if df_일봉 is not None else None
                try:
                    df_5분봉['전일종가'] = dic_전일종가[s_년월일] if df_일봉 is not None else None
                except KeyError:
                    df_5분봉['전일종가'] = None
                df_5분봉['전일대비(%)'] = (df_5분봉['종가'] / df_5분봉['전일종가'] - 1) * 100 if df_일봉 is not None else None
                df_5분봉['종가ma5'] = df_5분봉['종가'].rolling(5).mean()
                df_5분봉['종가ma10'] = df_5분봉['종가'].rolling(10).mean()
                df_5분봉['종가ma20'] = df_5분봉['종가'].rolling(20).mean()
                df_5분봉['종가ma60'] = df_5분봉['종가'].rolling(60).mean()
                df_5분봉['종가ma120'] = df_5분봉['종가'].rolling(120).mean()
                df_5분봉['거래량ma5'] = df_5분봉['거래량'].rolling(5).mean()
                df_5분봉['거래량ma20'] = df_5분봉['거래량'].rolling(20).mean()
                df_5분봉['거래량ma60'] = df_5분봉['거래량'].rolling(60).mean()
                df_5분봉['거래량ma120'] = df_5분봉['거래량'].rolling(120).mean()
                # 해당 일만 골라내기
                df_5분봉 = df_5분봉[df_5분봉['일자'] == s_년월일]
                # dic 할당
                dic_5분봉[s_종목코드] = df_5분봉

            # pkl 저장
            pd.to_pickle(dic_5분봉, os.path.join(self.folder_캐시변환, f'dic_코드별_5분봉_{s_년월일}.pkl'))

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
        li_테이블명_전체 = df_테이블명['name'].to_list()

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

    def 실시간_체결정보(self):
        """ 텍스트 파일로 저장된 실시간 정보를 불러와서 df 변환 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = '실시간_주식체결'
        s_파일명_생성 = 'df_체결정보'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_실시간)
                    if s_파일명_기준 in 파일명 and '.txt' in 파일명]
        li_일자_전체 = [일자 for 일자 in li_일자_전체 if 일자 >= '20241105']
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            with open(os.path.join(self.folder_실시간, f'실시간_주식체결_{s_일자}.txt'), 'rt') as file:
                s_체결정보_원본 = file.read()

            # 데이터 정리
            s_체결정보 = s_체결정보_원본.replace('실시간 | 주식체결 | ', '')
            s_체결정보 = s_체결정보.replace("'", '').replace('[', '').replace(']', '')
            li_체결정보 = s_체결정보.split('\n')
            li_체결정보 = [체결정보.split(', ') for 체결정보 in li_체결정보]
            df_체결정보 = pd.DataFrame(li_체결정보,
                                   columns=['종목코드', '체결시간', '체결단가', '전일대비(%)', '체결량', '매수매도', '체결금액'])
            df_체결정보 = df_체결정보.dropna().sort_values('체결시간')
            for s_컬럼명 in ['체결단가', '체결량']:
                df_체결정보[s_컬럼명] = df_체결정보[s_컬럼명].astype(int)
            df_체결정보['dt일시'] = pd.to_datetime(s_일자 + ' ' + df_체결정보['체결시간'])
            df_체결정보 = df_체결정보.set_index('dt일시')

            # df 저장
            df_체결정보.to_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_생성}_{s_일자}.pkl'))
            df_체결정보.to_csv(os.path.join(self.folder_체결정보, f'{s_파일명_생성}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 실시간_1초봉(self):
        """ 저장된 체결정보 기준으로 1초봉 생성 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_체결정보'
        s_파일명_생성 = 'dic_코드별_1초봉'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            df_체결정보 = pd.read_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 초봉 생성
            dic_1초봉 = self.make_초봉데이터(df_체결정보=df_체결정보, n_초봉=1)

            # dic 저장
            pd.to_pickle(dic_1초봉, os.path.join(self.folder_캐시변환, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 실시간_2초봉(self):
        """ 저장된 체결정보 기준으로 1초봉 생성 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_체결정보'
        s_파일명_생성 = 'dic_코드별_2초봉'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            df_체결정보 = pd.read_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 초봉 생성
            dic_2초봉 = self.make_초봉데이터(df_체결정보=df_체결정보, n_초봉=2)

            # dic 저장
            pd.to_pickle(dic_2초봉, os.path.join(self.folder_캐시변환, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 실시간_3초봉(self):
        """ 저장된 체결정보 기준으로 1초봉 생성 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_체결정보'
        s_파일명_생성 = 'dic_코드별_3초봉'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            df_체결정보 = pd.read_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 초봉 생성
            dic_3초봉 = self.make_초봉데이터(df_체결정보=df_체결정보, n_초봉=3)

            # dic 저장
            pd.to_pickle(dic_3초봉, os.path.join(self.folder_캐시변환, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 실시간_5초봉(self):
        """ 저장된 체결정보 기준으로 1초봉 생성 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_체결정보'
        s_파일명_생성 = 'dic_코드별_5초봉'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            df_체결정보 = pd.read_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 초봉 생성
            dic_5초봉 = self.make_초봉데이터(df_체결정보=df_체결정보, n_초봉=5)

            # dic 저장
            pd.to_pickle(dic_5초봉, os.path.join(self.folder_캐시변환, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    def 실시간_10초봉(self):
        """ 저장된 체결정보 기준으로 1초봉 생성 후 pkl 파일 저장 """
        # 파일명 정의
        s_파일명_기준 = 'df_체결정보'
        s_파일명_생성 = 'dic_코드별_10초봉'

        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_체결정보)
                    if s_파일명_기준 in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                    if s_파일명_생성 in 파일명 and '.pkl' in 파일명]
        li_일자_대상 = [일자 for 일자 in li_일자_전체 if 일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 파일 읽어오기
            df_체결정보 = pd.read_pickle(os.path.join(self.folder_체결정보, f'{s_파일명_기준}_{s_일자}.pkl'))

            # 초봉 생성
            dic_10초봉 = self.make_초봉데이터(df_체결정보=df_체결정보, n_초봉=10)

            # dic 저장
            pd.to_pickle(dic_10초봉, os.path.join(self.folder_캐시변환, f'{s_파일명_생성}_{s_일자}.pkl'))

            # log 기록
            self.make_log(f'{s_일자} 데이터 저장 완료')

    @staticmethod
    def make_초봉데이터(df_체결정보, n_초봉):
        """ 체결정보 기준 초봉 데이터로 생성 후 dic_초봉 리턴 """
        # df 정의
        df_체결정보 = df_체결정보.copy()

        # 종목별 데이터 처리
        dic_초봉 = dict()
        for s_종목코드 in df_체결정보['종목코드'].unique():
            # 종목 분리
            df_체결정보_종목 = df_체결정보[df_체결정보['종목코드'] == s_종목코드].copy()

            # 초봉데이터 생성
            df_초봉 = Logic.make_초봉데이터(df_체결정보=df_체결정보_종목, n_초봉=n_초봉, s_종목코드=s_종목코드)

            # dict 입력
            dic_초봉[s_종목코드] = df_초봉

        return dic_초봉

    # @staticmethod
    # def make_초봉데이터(df_체결정보, n_초봉):
    #     """ 체결정보 기준 초봉 데이터로 생성 후 dic_초봉 리턴 """
    #     # df 정의
    #     df_체결정보 = df_체결정보.copy()
    #
    #     # 종목별 데이터 처리
    #     dic_초봉 = dict()
    #     for s_종목코드 in df_체결정보['종목코드'].unique():
    #         # 종목 분리
    #         df_체결정보_종목 = df_체결정보[df_체결정보['종목코드'] == s_종목코드].copy()
    #
    #         # 매수매도 분리
    #         df_체결정보_종목_매수 = df_체결정보_종목[df_체결정보_종목['매수매도'] == '매수']
    #         df_체결정보_종목_매도 = df_체결정보_종목[df_체결정보_종목['매수매도'] == '매도']
    #
    #         # 초봉 생성
    #         df_리샘플 = df_체결정보_종목.resample(f'{n_초봉}s')
    #         df_리샘플_매수 = df_체결정보_종목_매수.resample(f'{n_초봉}s')
    #         df_리샘플_매도 = df_체결정보_종목_매도.resample(f'{n_초봉}s')
    #         df_초봉 = df_리샘플_매수.first().loc[:, '종목코드':'체결시간']
    #         df_초봉['종목코드'] = s_종목코드
    #         df_초봉['체결시간'] = df_초봉.index.strftime('%H:%M:%S')
    #         df_초봉['시가'] = df_리샘플['체결단가'].first()
    #         df_초봉['고가'] = df_리샘플['체결단가'].max()
    #         df_초봉['저가'] = df_리샘플['체결단가'].min()
    #         df_초봉['종가'] = df_리샘플['체결단가'].last()
    #         df_초봉['거래량'] = df_리샘플['체결량'].sum()
    #         df_초봉['매수량'] = df_리샘플_매수['체결량'].sum()
    #         df_초봉['매도량'] = df_리샘플_매도['체결량'].sum()
    #         df_초봉['매수량'] = df_초봉['매수량'].fillna(0).astype(int)
    #         df_초봉['매도량'] = df_초봉['매도량'].fillna(0).astype(int)
    #
    #         # dict 입력
    #         dic_초봉[s_종목코드] = df_초봉
    #
    #     return dic_초봉

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
    c.캐시저장_3분봉()
    c.캐시저장_5분봉()
    c.캐시저장_10분봉()
    c.실시간_체결정보()
    c.실시간_1초봉()
    c.실시간_2초봉()
    c.실시간_3초봉()
    c.실시간_5초봉()
    c.실시간_10초봉()

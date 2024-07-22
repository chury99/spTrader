import os
import sys
import pandas as pd
import json
import re

from scipy import stats
from tqdm import tqdm
import pandas.errors

import analyzer_rf알고리즘 as Logic


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class ChartMaker:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')

        # 폴더 정의
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']

    def find_전일종가(self, df_ohlcv):
        """ 입력 받은 일봉 또는 분봉에 해당 종목의 전일 종가 생성 후 df 리턴 """
        # 대상일 확인
        li_전체일 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                  if 'dic_코드별_분봉_' in 파일명 and '.pkl' in 파일명]
        ary_기준일 = df_ohlcv['일자'].unique()
        s_추가일 = max([일자 for 일자 in li_전체일 if 일자 < ary_기준일.min()])
        li_대상일 = [s_추가일] + list(ary_기준일)

        # 일봉 불러오기
        s_종목코드 = df_ohlcv['종목코드'].values[0]
        li_대상월 = list(pd.Series(li_대상일).apply(lambda x: x[:6]).unique())
        li_df_일봉 = [pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_일봉_{s_해당월}.pkl'))[s_종목코드]
                    for s_해당월 in li_대상월]
        df_일봉 = pd.concat(li_df_일봉, axis=0).sort_values('일자').reset_index(drop=True)
        dic_일자2전일종가 = df_일봉.set_index('일자').to_dict()['전일종가']
        n_전일종가_마지막 = df_일봉['종가'].values[-1]

        # df_전일종가 생성
        df_전일종가 = df_ohlcv.copy()
        df_전일종가['전일종가'] = df_전일종가['일자'].apply(lambda x: dic_일자2전일종가[x] if x in dic_일자2전일종가.keys()
                                                                                        else n_전일종가_마지막)

        return df_전일종가

    def make_이동평균(self):
        pass

    def make_차트(self, n_봉수=120):
        pass


#######################################################################################################################
if __name__ == "__main__":
    c = ChartMaker()

    c.find_전일종가()

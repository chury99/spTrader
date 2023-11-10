import os
import sys
import pandas as pd
import json
import re

from tqdm import tqdm

import analyzer_알고리즘 as Logic

# 그래프 한글 설정
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc, rcParams
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
rcParams['axes.unicode_minus'] = False


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Analyzer:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_analyzer"]}_{self.s_오늘}.log')
        self.n_파일보관기간 = int(dic_config['파일보관기간(일)_analyzer'])

        # 폴더 정의
        import UT_폴더정보
        dic_폴더정보 = UT_폴더정보.dic_폴더정보
        self.folder_run = dic_폴더정보['run']
        self.folder_ohlcv = dic_폴더정보['데이터|ohlcv']
        self.folder_캐시변환 = dic_폴더정보['데이터|캐시변환']
        self.folder_정보수집 = dic_폴더정보['데이터|정보수집']
        self.folder_감시대상 = dic_폴더정보['분석1종목|감시대상']
        self.folder_감시대상모델 = dic_폴더정보['분석1종목|모델_감시대상']
        self.folder_상승예측 = dic_폴더정보['분석2공통|10_상승예측']
        self.folder_수익검증 = dic_폴더정보['분석2공통|20_수익검증']
        os.makedirs(self.folder_상승예측, exist_ok=True)
        os.makedirs(self.folder_수익검증, exist_ok=True)

        # 변수 설정
        dic_조건검색 = pd.read_pickle(os.path.join(self.folder_정보수집, 'dic_조건검색.pkl'))
        df_분석대상종목 = dic_조건검색['분석대상종목']
        self.li_종목_분석대상 = list(df_분석대상종목['종목코드'].sort_values())
        self.dic_코드2종목명 = df_분석대상종목.set_index('종목코드').to_dict()['종목명']

        self.li_일자_전체 = sorted([re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_캐시변환)
                               if 'dic_코드별_10분봉_' in 파일명 and '.pkl' in 파일명])

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])
        self.s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # log 기록
        self.make_log(f'### 공통 분석 시작 ###')

    def 분석1검증_상승예측(self, s_모델):
        """ 감시대상 종목 불러와서 10분봉 기준 상승여부 예측 """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_감시대상)
                    if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 전일 일자 확인
            try:
                s_일자_전일 = max([일자 for 일자 in li_일자_전체 if 일자 < s_일자])
            except ValueError:
                continue

            # 감시대상 불러오기 (전일 기준 - 전일 생성한 데이터 불러와서 당일 감시)
            df_감시대상 = pd.read_pickle(os.path.join(self.folder_감시대상, f'df_감시대상_{s_모델}_{s_일자_전일}.pkl'))
            df_감시대상['li_조건'] = [list(ary) for ary in df_감시대상.loc[:, '대기봉수': '확률스펙'].values]
            try:
                dic_조건 = df_감시대상.set_index('종목코드').to_dict()['li_조건']
                li_감시대상 = list(df_감시대상['종목코드'].values)
            except KeyError:
                dic_조건 = dict()
                li_감시대상 = list()

            # 모델 불러오기 (전일 기준 - 전일 생성한 모델 불러와서 당일 적용)
            dic_모델 = pd.read_pickle(os.path.join(self.folder_감시대상모델, f'dic_모델_감시대상_{s_모델}_{s_일자_전일}.pkl'))

            # 10분봉 불러오기 (당일 기준)
            dic_df_10분봉_당일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자}.pkl'))
            dic_df_10분봉_전일 = pd.read_pickle(os.path.join(self.folder_캐시변환, f'dic_코드별_10분봉_{s_일자_전일}.pkl'))

            # 종목별 상승 예측값 생성
            dic_df_상승예측 = dict()
            for s_종목코드 in tqdm(li_감시대상, desc=f'{s_모델} 상승 예측값 생성({s_일자})'):
                # 조건 설정
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이, n_예측성공, n_확률스펙 = dic_조건[s_종목코드]
                n_대기봉수, n_학습일수, n_rf_트리, n_rf_깊이 = int(n_대기봉수), int(n_학습일수), int(n_rf_트리), int(n_rf_깊이)
                s_케이스 = f'{n_대기봉수}_{n_학습일수}_{n_rf_트리}_{n_rf_깊이}'

                # 모델 설정 (데이터 부족 등 이유로 모델 미생성된 종목은 예외처리)
                try:
                    obj_모델 = dic_모델[s_종목코드][s_케이스]
                except KeyError:
                    continue

                # 데이터셋 준비 (추가 데이터 생성, 라벨 생성, 당일만 잘라내기, 입력용 ary로 변환)
                df_10분봉 = pd.concat([dic_df_10분봉_전일[s_종목코드], dic_df_10분봉_당일[s_종목코드]], axis=0).sort_index()
                df_데이터셋 = Logic.make_추가데이터_rf(df=df_10분봉)
                df_데이터셋 = Logic.make_라벨데이터_rf(df=df_데이터셋, n_대기봉수=n_대기봉수)
                df_데이터셋 = df_데이터셋[df_데이터셋['일자'] == s_일자]
                if len(df_데이터셋) == 0 or len(df_데이터셋) != len(df_10분봉[df_10분봉['일자'] == s_일자]):
                    continue
                dic_데이터셋 = Logic.make_입력용xy_rf(df=df_데이터셋, n_학습일수=1)
                ary_x_평가 = dic_데이터셋['ary_x_학습']
                ary_y_정답 = dic_데이터셋['ary_y_학습']

                # 상승확률 및 상승예측 생성 (10분봉 데이터를 당일 데이터만 잘라낸 후, 상승확률 및 상승예측 추가)
                df_10분봉 = df_10분봉[df_10분봉['일자'] == s_일자]
                try:
                    df_10분봉['상승확률(%)'] = obj_모델.predict_proba(ary_x_평가)[:, 1] * 100
                except IndexError:
                    df_10분봉['상승확률(%)'] = 0
                df_10분봉['상승예측'] = (df_10분봉['상승확률(%)'] > n_확률스펙) * 1
                df_10분봉['정답'] = ary_y_정답

                # 케이스 정보 추가
                df_10분봉['케이스'] = s_케이스

                # 평가 정보 추가
                df_10분봉['예측성공'] = int(n_예측성공)
                df_10분봉['확률스펙'] = n_확률스펙

                # dic_df에 입력
                dic_df_상승예측[s_종목코드] = df_10분봉

            # 상승예측 결과 하나의 df로 합치기
            li_df = [pd.DataFrame()] + [dic_df_상승예측[종목코드] for 종목코드 in dic_df_상승예측.keys()]
            df_상승예측 = pd.concat(li_df, axis=0)

            # 결과 저장
            df_상승예측.to_pickle(os.path.join(self.folder_상승예측, f'df_상승예측_{s_모델}_{s_일자}.pkl'))
            df_상승예측.to_csv(os.path.join(self.folder_상승예측, f'상승예측_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # log 기록
            self.make_log(f'종목별 상승예측 완료({s_일자}, {len(dic_df_상승예측):,}개 종목, {s_모델})')

    def 분석1검증_수익검증(self, s_모델):
        """ 상승여부 예측한 결과를 바탕으로 종목선정 조건에 따른 결과 확인 (예측 엑셀, 리포트 저장) """
        # 분석대상 일자 선정
        li_일자_전체 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_상승예측)
                    if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
        li_일자_완료 = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in os.listdir(self.folder_수익검증)
                    if f'df_수익검증_{s_모델}_' in 파일명 and f'.pkl' in 파일명]
        li_일자_대상 = [s_일자 for s_일자 in li_일자_전체 if s_일자 not in li_일자_완료]

        # 일자별 분석 진행
        for s_일자 in li_일자_대상:
            # 상승예측 데이터 불러오기
            li_파일명 = [파일명 for 파일명 in os.listdir(self.folder_상승예측)
                      if f'df_상승예측_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            li_파일명 = [파일명 for 파일명 in li_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]
            li_df = [pd.read_pickle(os.path.join(self.folder_상승예측, 파일명)) for 파일명 in li_파일명]
            df_상승예측 = pd.concat(li_df, axis=0)

            # 예측한 값만 잘라내기
            df_수익검증 = df_상승예측[df_상승예측['상승예측'] == 1].drop_duplicates()

            # 예측 엑셀 저장 (pkl 포함)
            df_수익검증.to_pickle(os.path.join(self.folder_수익검증, f'df_수익검증_{s_모델}_{s_일자}.pkl'))
            df_수익검증.to_csv(os.path.join(self.folder_수익검증, f'수익검증_{s_모델}_{s_일자}.csv'),
                           index=False, encoding='cp949')

            # ### 리포트 생성 및 처리 ###
            # 감시대상 종목 정보 생성
            li_감시대상_파일명 = [파일명 for 파일명 in os.listdir(self.folder_감시대상)
                           if f'df_감시대상_{s_모델}_' in 파일명 and '.pkl' in 파일명]
            s_시작일자 = min([re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_파일명])
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] <= s_일자]
            li_감시대상_파일명 = [파일명 for 파일명 in li_감시대상_파일명 if re.findall(r'\d{8}', 파일명)[0] >= s_시작일자]
            df_감시대상 = pd.DataFrame()
            df_감시대상['일자'] = [re.findall(r'\d{8}', 파일명)[0] for 파일명 in li_감시대상_파일명]
            df_감시대상['종목수'] = [len(pd.read_pickle(os.path.join(self.folder_감시대상, 파일명)))
                              for 파일명 in li_감시대상_파일명]

            # 리포트 생성
            plt.figure(figsize=[16, 9])

            plt.subplot(2, 2, 1)
            plt.title('[ 감시대상 종목 수 ]')
            ary_x, ary_y = df_감시대상['일자'].values, df_감시대상['종목수'].values
            li_색깔 = ['C1' if 종목수 > 15 else 'C0' for 종목수 in ary_y]
            plt.bar(ary_x, ary_y, color=li_색깔)
            plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
            plt.grid(linestyle='--', alpha=0.5)

            plt.subplot(2, 2, 2)
            plt.title('[ 상승예측 건수 ]')
            sri_상승예측건수 = df_수익검증.groupby('일자')['상승예측'].sum()
            ary_x, ary_y = sri_상승예측건수.index.values, sri_상승예측건수.values
            li_색깔 = ['C3' if 예측건수 > 10 else 'C0' for 예측건수 in ary_y]
            plt.bar(ary_x, ary_y, color=li_색깔)
            plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
            plt.grid(linestyle='--', alpha=0.5)

            plt.subplot(2, 2, 4)
            plt.title('[ 예측 성공률 (%) ]')
            sri_예측성공건수 = df_수익검증.groupby('일자')['정답'].sum()
            sri_예측성공률 = sri_예측성공건수 / sri_상승예측건수 * 100
            ary_x, ary_y = sri_예측성공률.index.values, sri_예측성공률.values
            li_색깔 = ['C0' if 성공률 > 70 else 'C3' for 성공률 in ary_y]
            plt.bar(ary_x, ary_y, color=li_색깔)
            plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
            plt.grid(linestyle='--', alpha=0.5)
            plt.axhline(100, color='C0', alpha=0)
            plt.axhline(70, color='C1')

            plt.subplot(2, 2, 3)
            plt.title('[ 예측 성공률 (%, 누적) ]')
            sri_예측성공률_누적 = sri_예측성공건수.cumsum() / sri_상승예측건수.cumsum() * 100
            ary_x, ary_y = sri_예측성공률_누적.index.values, sri_예측성공률_누적.values
            plt.plot(ary_x, ary_y)
            plt.xticks([0, len(ary_x) - 1], [ary_x[0], ary_x[-1]])
            plt.grid(linestyle='--', alpha=0.5)
            plt.axhline(100, color='C0', alpha=0)
            plt.axhline(70, color='C1')

            # 리포트 저장
            plt.savefig(os.path.join(self.folder_수익검증, f'수익검증_리포트_{s_모델}_{s_일자}.png'))
            plt.close()

            # 리포트 복사 to 서버
            folder_서버 = 'kakao/수익검증'
            s_파일명_리포트 = f'수익검증_리포트_{s_모델}_{s_일자}.png'
            self.to_ftp(s_파일명=s_파일명_리포트, folder_로컬=self.folder_수익검증, folder_서버=folder_서버)

            # 카톡 보내기
            import API_kakao
            k = API_kakao.KakaoAPI()
            result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{self.s_파일}] 분석1검증 완료',
                                    s_button_title=f'수익검증 리포트 - {s_일자}',
                                    s_url=f'http://goniee.com/{folder_서버}/{s_파일명_리포트}')

            # log 기록
            self.make_log(f'수익검증 리포트 생성 완료({s_일자}, {s_모델})')

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

    def to_ftp(self, s_파일명, folder_로컬, folder_서버):
        """ ftp 서버에 접속해서 파일을 folder_로컬에서 folder_서버로 업로드 """
        import ftplib
        import API_kakao
        k = API_kakao.KakaoAPI()

        # 정보 읽어오기
        dic_ftp = pd.read_pickle(os.path.join(self.folder_run, 'acc_info.dll'))['ftp']

        # ftp 서버 연결
        with ftplib.FTP() as ftp:
            ret_서버접속 = ftp.connect(host=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['host'])]),
                                   port=int(''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['port'])])))
            if ret_서버접속[:3] != '220':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{self.s_파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_서버접속}')

            ret_로그인 = ftp.login(user=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['id'])]),
                                passwd=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['pw'])]))
            if ret_로그인[:3] != '230':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{self.s_파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_로그인}')

            ret_폴더변경 = ftp.cwd(dirname=f'/99.www/{folder_서버}')
            if ret_폴더변경[:3] != '250':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{self.s_파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_폴더변경}')

            # 신규파일 업로드
            with open(os.path.join(folder_로컬, s_파일명), 'rb') as file:
                ret_업로드 = ftp.storbinary(f'STOR {s_파일명}', file)
                if ret_업로드[:3] != '226':
                    result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{self.s_파일}] ftp 오류 !!!',
                                            s_button_title=f'{ret_업로드}')

            # 기존파일 삭제 (config.json 파일에서 정의한 analyzer 파일 보관 기간과 동일)
            dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.Timedelta(days=self.n_파일보관기간)
            s_기준일자 = dt_기준일자.strftime('%Y%m%d')

            li_기존파일 = ftp.nlst()
            li_파일_일자존재 = [파일 for 파일 in li_기존파일 if re.findall(r'\d{8}', 파일)]
            li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
            for 파일 in li_파일_삭제대상:
                ret_삭제 = ftp.delete(filename=파일)
                if ret_삭제[:3] != '250':
                    result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{self.s_파일}] ftp 오류 !!!',
                                            s_button_title=f'{ret_삭제}')


#######################################################################################################################
if __name__ == "__main__":
    a = Analyzer()

    a.분석1검증_상승예측(s_모델='rf')
    a.분석1검증_수익검증(s_모델='rf')

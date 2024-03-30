import os
import sys
import pandas as pd
import json
import re


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Worker:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.n_파일보관기간_analyzer = int(dic_config['파일보관기간(일)_analyzer'])

        # 폴더 정의
        import UT_폴더manager
        dic_폴더정보 = UT_폴더manager.dic_폴더정보
        self.folder_run = dic_폴더정보['run']

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])

    # noinspection PyUnusedLocal
    def to_ftp(self, s_파일명, folder_로컬, folder_서버):
        """ ftp 서버에 접속해서 파일을 folder_로컬에서 folder_서버로 업로드 """
        import ftplib
        import API_kakao
        k = API_kakao.KakaoAPI()

        # 호출한 파일명 찾기
        s_호출파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # 정보 읽어오기
        dic_ftp = pd.read_pickle(os.path.join(self.folder_run, 'acc_info.dll'))['ftp']

        # ftp 서버 연결
        with ftplib.FTP() as ftp:
            ret_서버접속 = ftp.connect(host=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['host'])]),
                                   port=int(''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['port'])])))
            if ret_서버접속[:3] != '220':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_서버접속}')

            ret_로그인 = ftp.login(user=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['id'])]),
                                passwd=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['pw'])]))
            if ret_로그인[:3] != '230':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_로그인}')

            ret_폴더변경 = ftp.cwd(dirname=f'/99.www/{folder_서버}')
            if ret_폴더변경[:3] != '250':
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{ret_폴더변경}')

            # 신규파일 업로드
            with open(os.path.join(folder_로컬, s_파일명), 'rb') as file:
                ret_업로드 = ftp.storbinary(f'STOR {s_파일명}', file)
                if ret_업로드[:3] != '226':
                    result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                            s_button_title=f'{ret_업로드}')

            # 기존파일 삭제 (config.json 파일에서 정의한 analyzer 파일 보관 기간과 동일)
            dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.Timedelta(days=self.n_파일보관기간_analyzer)
            s_기준일자 = dt_기준일자.strftime('%Y%m%d')

            li_기존파일 = ftp.nlst()
            li_파일_일자존재 = [파일 for 파일 in li_기존파일 if re.findall(r'\d{8}', 파일)]
            li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
            for 파일 in li_파일_삭제대상:
                ret_삭제 = ftp.delete(filename=파일)
                if ret_삭제[:3] != '250':
                    result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                            s_button_title=f'{ret_삭제}')


#######################################################################################################################
if __name__ == "__main__":
    w = Worker()

    w.to_ftp(s_파일명='수익검증_리포트_rf_20240329.png',
             folder_로컬='D:\\ProjectWork\\spTrader\\분석2공통\\20_수익검증', folder_서버='kakao/수익검증')

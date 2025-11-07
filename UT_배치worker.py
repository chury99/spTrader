import os
import socket
import sys
import pandas as pd
import json
import re

import paramiko


# noinspection PyUnresolvedReferences,NonAsciiCharacters,PyPep8Naming
class Worker:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp.now().strftime('%Y%m%d')
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
            try:
                ret_서버접속 = ftp.connect(host=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['host'])]),
                                       port=int(''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['port'])])))
            except ftplib.error_perm as error:
                s_에러코드 = error.args[0]
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{s_에러코드}')

            try:
                ret_로그인 = ftp.login(user=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['id'])]),
                                    passwd=''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['pw'])]))
            except ftplib.error_perm as error:
                s_에러코드 = error.args[0]
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{s_에러코드}')

            try:
                ret_폴더생성 = ftp.mkd(dirname=f'/99.www/{folder_서버}')
            except ftplib.error_perm as error:
                s_에러코드 = error.args[0]

            try:
                ret_폴더변경 = ftp.cwd(dirname=f'/99.www/{folder_서버}')
            except ftplib.error_perm as error:
                s_에러코드 = error.args[0]
                result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                        s_button_title=f'{s_에러코드}')

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
                try:
                    ret_삭제 = ftp.delete(filename=파일)
                except ftplib.error_perm as error:
                    s_에러코드 = error.args[0]
                    result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'!!! [{s_호출파일}] ftp 오류 !!!',
                                            s_button_title=f'{ret_삭제}')

    # noinspection PyUnusedLocal
    def to_sftp서버(self, s_파일명, folder_로컬, folder_서버):
        """ sftp 서버에 접속해서 파일을 folder_로컬에서 folder_서버로 업로드 """
        # 필요 모듈 연결
        import API_kakao
        k = API_kakao.KakaoAPI()

        # 호출한 파일명 찾기
        s_호출파일 = os.path.basename(sys.argv[0]).replace('.py', '')

        # 서버정보 읽어오기
        dic_서버정보 = json.load(open('server_info.json', mode='rt', encoding='utf-8'))['sftp']

        # sftp 서버 접속
        with paramiko.SSHClient() as ssh:
            # ssh 서버 연결 (알수없는 서버 경고 방지 포함)
            try:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=dic_서버정보['hostname'], port=dic_서버정보['port'],
                            username=dic_서버정보['username'], password=dic_서버정보['password'])
            except (socket.error, paramiko.SSHException) as e:
                s_에러명 = e.args[-1]
                ret = k.send_메세지(s_사용자='알림봇', s_수신인='여봉이', s_메세지=f'!!! [{s_호출파일}] sftp 오류 !!!',
                                 s_버튼이름=f'{s_에러명}')

            # sftp 세션
            with ssh.open_sftp() as sftp:
                # 폴더 생성
                folder_웹 = '/Volumes/extSSD4tb/90_web'
                folder_서버 = f'{folder_웹}/{folder_서버}'
                try:
                    sftp.mkdir(folder_서버)
                    sftp.chmod(folder_서버, mode=0o775)
                except OSError:
                    pass

                # 파일 업로드
                sftp.put(os.path.join(folder_로컬, s_파일명), f'{folder_서버}/{s_파일명}')

                # 기존파일 삭제 (config.json 파일에서 정의한 analyzer 파일 보관 기간과 동일)
                dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.Timedelta(days=self.n_파일보관기간_analyzer)
                s_기준일자 = dt_기준일자.strftime('%Y%m%d')

                li_기존파일 = sftp.listdir(folder_서버)
                li_파일_일자존재 = [파일 for 파일 in li_기존파일 if re.findall(r'\d{8}', 파일)]
                li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
                for s_삭제대상 in li_파일_삭제대상:
                    sftp.remove(f'{folder_서버}/{s_삭제대상}')


#######################################################################################################################
if __name__ == "__main__":
    w = Worker()

    # w.to_ftp(s_파일명='수익검증_리포트_rf_20240329.png',
    #          folder_로컬='D:\\ProjectWork\\spTrader\\분석2공통\\20_수익검증', folder_서버='kakao/수익검증')

    w.to_sftp서버(s_파일명='백테스팅_리포트_20250819.png',
                folder_로컬='D:/ProjectWork/spTrader/analyzer_TransactionFlow/백테스팅/50_매매이력_리포트',
                folder_서버='kakao/test')

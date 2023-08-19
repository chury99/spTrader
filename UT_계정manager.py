import os
import sys
import pandas as pd
import json


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames
class Manager:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'sp_acc_manager_{self.s_오늘}.log')

        # 파일 정의
        folder_work = dic_config['folder_work']
        folder_run = os.path.join(folder_work, 'run')
        self.path_파일 = os.path.join(folder_run, 'acc_info.dll')

        # log 기록
        self.make_log(f'### 계정 정보 관리 시작 ###')

    def 계정관리_ftp(self):
        """ ftp 관련 정보 관리 """
        # 파일 불러오기
        try:
            dic_계정 = pd.read_pickle(self.path_파일)
        except FileNotFoundError:
            dic_계정 = dict()

        # 정보 입력 (정보 입력 시에만 기입)
        s_url = ''
        s_port = ''
        s_id = ''
        s_pw = ''

        # 정보 저장
        dic_ftp = dict()
        dic_ftp['url'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_url)])
        dic_ftp['port'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_port)])
        dic_ftp['id'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_id)])
        dic_ftp['pw'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_pw)])

        # 파일 저장
        dic_계정['ftp'] = dic_ftp
        pd.to_pickle(dic_계정, self.path_파일)

        # 복호화
        # s_url2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['url'])])
        # s_port2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['port'])])
        # s_id2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['id'])])
        # s_pw2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_ftp['pw'])])

        # log 기록
        self.make_log(f'정보 저장 완료')

    def 계정관리_영웅문(self):
        """ 영웅문 관련 정보 관리 """
        # 파일 불러오기
        try:
            dic_계정 = pd.read_pickle(self.path_파일)
        except FileNotFoundError:
            dic_계정 = dict()

        # 정보 입력 (정보 입력 시에만 기입)
        s_id = ''
        s_pw = ''
        s_cert = ''

        # 정보 저장
        dic_영웅문 = dict()
        dic_영웅문['id'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_id)])
        dic_영웅문['pw'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_pw)])
        dic_영웅문['cert'] = ''.join([chr(ord(글자) + 369) for 글자 in list(s_cert)])

        # 파일 저장
        dic_계정['영웅문'] = dic_영웅문
        pd.to_pickle(dic_계정, self.path_파일)

        # 복호화
        # s_id2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_영웅문['id'])])
        # s_pw2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_영웅문['pw'])])
        # s_cert2 = ''.join([chr(ord(글자) - 369) for 글자 in list(dic_영웅문['cert'])])

        # log 기록
        self.make_log(f'정보 저장 완료')

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
    m = Manager()

    # m.계정관리_ftp()
    # m.계정관리_영웅문()

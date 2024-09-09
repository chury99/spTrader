import os
import sys
import pandas as pd
import json
import re

import shutil


# noinspection PyPep8Naming,PyUnresolvedReferences,PyProtectedMember,PyAttributeOutsideInit,PyArgumentList
# noinspection PyShadowingNames,PyUnusedLocal
class Rotator:
    def __init__(self):
        # config 읽어 오기
        with open('config.json', mode='rt', encoding='utf-8') as file:
            dic_config = json.load(file)

        # 기준정보 정의
        self.s_오늘 = pd.Timestamp('now').strftime('%Y%m%d')
        self.path_log = os.path.join(dic_config['folder_log'], f'{dic_config["로그이름_rotator"]}_{self.s_오늘}.log')

        # 폴더 정의
        import UT_폴더manager
        self.folder_work = UT_폴더manager.dic_폴더정보['work']

        # dic_folder 생성
        self.dic_folder = dict()
        self.dic_folder['log'] = [dic_config['folder_log']]

        # 변수 설정
        self.dic_보관기간 = dict()
        self.dic_보관기간['log'] = int(dic_config['파일보관기간(일)_log'])
        self.dic_보관기간['analyzer'] = int(dic_config['파일보관기간(일)_analyzer'])
        self.dic_보관기간['collector'] = int(dic_config['파일보관기간(일)_collector'])
        self.dic_보관기간['trader'] = int(dic_config['파일보관기간(일)_trader'])

        # 카카오 API 폴더 연결
        sys.path.append(dic_config['folder_kakao'])

        # log 기록
        self.make_log(f'### 파일 보관기간 관리 시작 ###')

    def 폴더정보탐색(self):
        """ 폴더 구조 파악 해서 self.dic_folder 등록 """
        # dic_folder 불러오기
        dic_folder = self.dic_folder

        # 메인 폴더 구조 생성
        li_폴더분류 = [폴더 for 폴더 in os.listdir(self.folder_work)
                   if os.path.isdir(os.path.join(self.folder_work, 폴더)) and '테스트' not in 폴더]

        # 하위 폴더 구성
        for s_폴더분류 in li_폴더분류:
            li_path_상위폴더 = [os.path.join(self.folder_work, s_폴더분류)]
            dic_folder[s_폴더분류] = li_path_상위폴더
            while len(li_path_상위폴더) > 0:
                li_path_다음폴더 = list()
                for path_상위폴더 in li_path_상위폴더:
                    li_path_하위폴더 = [os.path.join(path_상위폴더, 폴더) for 폴더 in os.listdir(path_상위폴더)
                                    if os.path.isdir(os.path.join(path_상위폴더, 폴더))]
                    dic_folder[s_폴더분류] = dic_folder[s_폴더분류] + li_path_하위폴더
                    li_path_다음폴더 = li_path_다음폴더 + li_path_하위폴더
                li_path_상위폴더 = li_path_다음폴더

        # 모듈별 폴더 통합
        dic_folder_모듈별 = dict()
        for s_폴더분류 in dic_folder.keys():
            s_모듈분류 = s_폴더분류.split('_')[0]
            try:
                dic_folder_모듈별[s_모듈분류] = dic_folder_모듈별[s_모듈분류] + dic_folder[s_폴더분류]
            except KeyError:
                dic_folder_모듈별[s_모듈분류] = dic_folder[s_폴더분류]

        # 데이터 수집 폴더 제외 (ohlcv, 캐시변환)
        li_제외 = ['ohlcv', '캐시변환']
        dic_folder_수집제외 = dict()
        for s_모듈 in dic_folder_모듈별.keys():
            dic_folder_수집제외[s_모듈] = list()
            for path_폴더 in dic_folder_모듈별[s_모듈]:
                li_판정 = ['제외' for s_제외 in li_제외 if s_제외 in path_폴더]
                if len(li_판정) == 0:
                    dic_folder_수집제외[s_모듈].append(path_폴더)

        # 날짜 포함된 폴더 제외 (폴더 통째로 삭제 예정)
        dic_folder_날짜제외 = dict()
        for s_모듈 in dic_folder_수집제외.keys():
            dic_folder_날짜제외[s_모듈] = list()
            for path_폴더 in dic_folder_수집제외[s_모듈]:
                li_날짜 = re.findall(r'\d{8}', path_폴더)
                if len(li_날짜) == 0:
                    dic_folder_날짜제외[s_모듈].append(path_폴더)

        # 결과를 self.dic_folder 설정
        self.dic_folder = dic_folder_날짜제외

    def 보관파일관리(self):
        """ 등록된 폴더 정보에 따라 보관기간 경과된 파일 삭제 """
        # 모듈별 폴더 탐색
        for s_모듈 in self.dic_folder.keys():
            # 기준 일자 정의
            n_보관기간 = self.dic_보관기간[s_모듈]
            dt_기준일자 = pd.Timestamp(self.s_오늘) - pd.DateOffset(days=n_보관기간)
            s_기준일자 = dt_기준일자.strftime('%Y%m%d')

            # 삭제대상 파일 찾기
            li_path_삭제대상 = list()
            for path_폴더 in self.dic_folder[s_모듈]:
                li_파일_전체 = os.listdir(path_폴더)
                li_파일_일자존재 = [파일 for 파일 in li_파일_전체 if re.findall(r'\d{8}', 파일)]
                li_파일_삭제대상 = [파일 for 파일 in li_파일_일자존재 if re.findall(r'\d{8}', 파일)[0] < s_기준일자]
                [li_path_삭제대상.append(os.path.join(path_폴더, 파일)) for 파일 in li_파일_삭제대상]

            # 대상 파일 삭제
            li_파일사이즈 = []
            for path_삭제대상 in li_path_삭제대상:
                # 대상이 파일일 때 삭제 용량 합산
                if os.path.isfile(path_삭제대상):
                    li_파일사이즈.append(os.path.getsize(path_삭제대상))
                    os.system(f'del {path_삭제대상}')

                # 대상이 폴더일 때 삭제 용량 합산
                if os.path.isdir(path_삭제대상):
                    for path1, li_path2, li_파일명 in os.walk(path_삭제대상):
                        for s_파일명 in li_파일명:
                            path_파일 = os.path.join(path1, s_파일명)
                            li_파일사이즈.append(os.path.getsize(path_파일))
                    os.system(f'rmdir /s /q {path_삭제대상}')

            # 파일사이즈 변환 및 단위 생성
            n_파일사이즈 = sum(li_파일사이즈)
            s_파일사이즈 = self.convert_파일사이즈(n_파일사이즈=n_파일사이즈)

            # log 기록
            self.make_log(f'{s_모듈} 파일 삭제 완료({n_보관기간}일 경과, {s_기준일자} 기준,'
                          f' {len(li_path_삭제대상):,}개 파일, {s_파일사이즈})')

    def 잔여공간확인(self):
        """ folder_work 폴더가 위치한 드라이브의 잔여 공간 확인하여 출력 """
        # 드라이브 위치 확인
        s_드라이브 = self.folder_work[0]

        # 용량 확인
        obj_디스크공간 = shutil.disk_usage(self.folder_work)
        n_잔여공간_GB = obj_디스크공간.free / (1024 ** 3)
        n_전체공간_GB = obj_디스크공간.total / (1024 ** 3)
        n_잔여비율_퍼센트 = n_잔여공간_GB / n_전체공간_GB * 100

        # 카톡 보내기 (잔여공간 10GB 미만 시)
        if n_잔여공간_GB < 10:
            import API_kakao
            k = API_kakao.KakaoAPI()
            s_파일 = os.path.basename(sys.argv[0]).replace('.py', '')
            result = k.send_message(s_user='알림봇', s_friend='여봉이', s_text=f'[{s_파일}] 저장 공간 Full 경고',
                                    s_button_title=f'잔여 공간 - {n_잔여공간_GB:.1f}GB ({n_잔여비율_퍼센트:.0f}%)')

        # log 기록
        self.make_log(f'{s_드라이브}드라이브 잔여 공간 - {n_잔여공간_GB:.1f}GB ({n_잔여비율_퍼센트:.0f}%)')

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

    @staticmethod
    def convert_파일사이즈(n_파일사이즈):
        """ byte 단위로 입력된 파일 용량을 단위 변환 후 str 형식으로 리턴 """
        # 기준정보
        li_단위 = ['B', 'KB', 'MB', 'GB', 'TB']
        n_단위 = 0

        # 단위 변환
        while n_파일사이즈 > 1024:
            n_파일사이즈 = n_파일사이즈 / 1024
            n_단위 = n_단위 + 1

        s_단위 = li_단위[n_단위]
        s_파일사이즈 = f'{n_파일사이즈:.1f}{s_단위}' if s_단위 in ['GB', 'TB'] else f'{n_파일사이즈:.0f}{s_단위}'

        return s_파일사이즈


#######################################################################################################################
if __name__ == "__main__":
    r = Rotator()

    r.폴더정보탐색()
    r.보관파일관리()
    r.잔여공간확인()

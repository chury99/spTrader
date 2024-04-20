import os
import json

# config 읽어 오기
with open('config.json', mode='rt', encoding='utf-8') as file:
    dic_config = json.load(file)

# 기준정보 생성
dic_폴더정보 = dict()
folder_work = dic_config['folder_work']

# work, run 폴더 정의
dic_폴더정보['work'] = folder_work
dic_폴더정보['run'] = os.path.join(folder_work, 'run')

# 이력 폴더 정의
folder_이력 = os.path.join(folder_work, '이력')
dic_폴더정보['이력'] = folder_이력
dic_폴더정보['이력|메세지'] = os.path.join(folder_이력, '메세지')
dic_폴더정보['이력|실시간'] = os.path.join(folder_이력, '실시간')
dic_폴더정보['이력|체결잔고'] = os.path.join(folder_이력, '체결잔고')

# 데이터 폴더 정의
folder_데이터 = os.path.join(folder_work, '데이터')
dic_폴더정보['데이터'] = folder_데이터
dic_폴더정보['데이터|ohlcv'] = os.path.join(folder_데이터, 'ohlcv')
dic_폴더정보['데이터|캐시변환'] = os.path.join(folder_데이터, '캐시변환')
dic_폴더정보['데이터|정보수집'] = os.path.join(folder_데이터, '정보수집')

# 분석1종목 폴더 정의
folder_분석1종목 = os.path.join(folder_work, '분석1종목')
dic_폴더정보['분석1종목'] = folder_분석1종목
dic_폴더정보['분석1종목|10_변동성종목'] = os.path.join(folder_분석1종목, '10_변동성종목')
dic_폴더정보['분석1종목|20_종목_데이터셋'] = os.path.join(folder_분석1종목, '20_종목_데이터셋')
dic_폴더정보['분석1종목|30_종목_모델'] = os.path.join(folder_분석1종목, '30_종목_모델')
dic_폴더정보['분석1종목|40_종목_성능평가'] = os.path.join(folder_분석1종목, '40_종목_성능평가')
dic_폴더정보['분석1종목|50_종목_감시대상'] = os.path.join(folder_분석1종목, '50_종목_감시대상')
dic_폴더정보['분석1종목|60_종목_모델_감시대상'] = os.path.join(folder_분석1종목, '60_종목_모델_감시대상')

# 분석2공통 폴더 정의
folder_분석2공통 = os.path.join(folder_work, '분석2공통')
dic_폴더정보['분석2공통'] = folder_분석2공통
dic_폴더정보['분석2공통|10_종목_상승예측'] = os.path.join(folder_분석2공통, '10_종목_상승예측')
dic_폴더정보['분석2공통|20_종목_수익검증'] = os.path.join(folder_분석2공통, '20_종목_수익검증')
dic_폴더정보['분석2공통|30_공통_데이터셋'] = os.path.join(folder_분석2공통, '30_공통_데이터셋')
dic_폴더정보['분석2공통|40_공통_모델'] = os.path.join(folder_분석2공통, '40_공통_모델')
dic_폴더정보['분석2공통|50_공통_성능평가'] = os.path.join(folder_분석2공통, '50_공통_성능평가')
dic_폴더정보['분석2공통|60_공통_수익검증'] = os.path.join(folder_분석2공통, '60_공통_수익검증')

# 백테스팅 폴더 정의
folder_백테스팅 = os.path.join(folder_work, '백테스팅')
dic_폴더정보['백테스팅'] = folder_백테스팅
dic_폴더정보['백테스팅|10_데이터준비'] = os.path.join(folder_백테스팅, '10_데이터준비')
dic_폴더정보['백테스팅|20_매수검증'] = os.path.join(folder_백테스팅, '20_매수검증')
dic_폴더정보['백테스팅|30_매도검증'] = os.path.join(folder_백테스팅, '30_매도검증')
dic_폴더정보['백테스팅|40_수익검증'] = os.path.join(folder_백테스팅, '40_수익검증')

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
dic_폴더정보['run'] = os.path.join(folder_work, 'trader_run')

# 이력 폴더 정의
folder_이력 = os.path.join(folder_work, 'trader_이력')
dic_폴더정보['이력'] = folder_이력
dic_폴더정보['이력|메세지'] = os.path.join(folder_이력, '메세지')
dic_폴더정보['이력|실시간'] = os.path.join(folder_이력, '실시간')
dic_폴더정보['이력|체결잔고'] = os.path.join(folder_이력, '체결잔고')
dic_폴더정보['이력|신호탐색'] = os.path.join(folder_이력, '신호탐색')
dic_폴더정보['이력|주문정보'] = os.path.join(folder_이력, '주문정보')
dic_폴더정보['이력|대상종목'] = os.path.join(folder_이력, '대상종목')
dic_폴더정보['이력|초봉정보'] = os.path.join(folder_이력, '초봉정보')
dic_폴더정보['이력|매개변수'] = os.path.join(folder_이력, '매개변수')

# 데이터 폴더 정의
folder_데이터 = os.path.join(folder_work, 'collector_데이터')
dic_폴더정보['데이터'] = folder_데이터
dic_폴더정보['데이터|ohlcv'] = os.path.join(folder_데이터, 'ohlcv')
dic_폴더정보['데이터|캐시변환'] = os.path.join(folder_데이터, '캐시변환')
dic_폴더정보['데이터|정보수집'] = os.path.join(folder_데이터, '정보수집')
dic_폴더정보['데이터|전체종목'] = os.path.join(folder_데이터, '전체종목')
dic_폴더정보['데이터|분석대상'] = os.path.join(folder_데이터, '분석대상')
dic_폴더정보['데이터|체결정보'] = os.path.join(folder_데이터, '체결정보')

#######################################################################################################################

# Random Forest 분석 폴더 정의
folder_RandomForest = os.path.join(folder_work, 'analyzer_RandomForest')
dic_폴더정보['rf분석'] = folder_RandomForest

# 분석1종목 폴더 정의
folder_분석1종목 = os.path.join(folder_RandomForest, '분석1종목')
dic_폴더정보['분석1종목'] = folder_분석1종목
dic_폴더정보['분석1종목|10_변동성종목'] = os.path.join(folder_분석1종목, '10_변동성종목')
dic_폴더정보['분석1종목|20_종목_데이터셋'] = os.path.join(folder_분석1종목, '20_종목_데이터셋')
dic_폴더정보['분석1종목|30_종목_모델'] = os.path.join(folder_분석1종목, '30_종목_모델')
dic_폴더정보['분석1종목|40_종목_성능평가'] = os.path.join(folder_분석1종목, '40_종목_성능평가')
dic_폴더정보['분석1종목|50_종목_감시대상'] = os.path.join(folder_분석1종목, '50_종목_감시대상')
dic_폴더정보['분석1종목|60_종목_모델_감시대상'] = os.path.join(folder_분석1종목, '60_종목_모델_감시대상')

# 분석2공통 폴더 정의
folder_분석2공통 = os.path.join(folder_RandomForest, '분석2공통')
dic_폴더정보['분석2공통'] = folder_분석2공통
dic_폴더정보['분석2공통|10_종목_상승예측'] = os.path.join(folder_분석2공통, '10_종목_상승예측')
dic_폴더정보['분석2공통|20_종목_수익검증'] = os.path.join(folder_분석2공통, '20_종목_수익검증')
dic_폴더정보['분석2공통|30_공통_데이터셋'] = os.path.join(folder_분석2공통, '30_공통_데이터셋')
dic_폴더정보['분석2공통|40_공통_모델'] = os.path.join(folder_분석2공통, '40_공통_모델')
dic_폴더정보['분석2공통|50_공통_성능평가'] = os.path.join(folder_분석2공통, '50_공통_성능평가')
dic_폴더정보['분석2공통|60_공통_수익검증'] = os.path.join(folder_분석2공통, '60_공통_수익검증')

# 백테스팅 폴더 정의
folder_백테스팅 = os.path.join(folder_RandomForest, '백테스팅')
dic_폴더정보['백테스팅'] = folder_백테스팅
dic_폴더정보['백테스팅|10_데이터준비'] = os.path.join(folder_백테스팅, '10_데이터준비')
dic_폴더정보['백테스팅|20_매수검증'] = os.path.join(folder_백테스팅, '20_매수검증')
dic_폴더정보['백테스팅|30_매도검증'] = os.path.join(folder_백테스팅, '30_매도검증')
dic_폴더정보['백테스팅|40_수익검증'] = os.path.join(folder_백테스팅, '40_수익검증')

#######################################################################################################################

# SRline 분석 폴더 정의
folder_SRline = os.path.join(folder_work, 'analyzer_SRline')
dic_폴더정보['sr분석'] = folder_SRline

# 종목선정 폴더 정의
folder_종목선정 = os.path.join(folder_SRline, '종목선정')
dic_폴더정보['sr종목선정'] = folder_종목선정
dic_폴더정보['sr종목선정|10_일봉변동'] = os.path.join(folder_종목선정, '10_일봉변동')
dic_폴더정보['sr종목선정|20_지지저항'] = os.path.join(folder_종목선정, '20_지지저항')
dic_폴더정보['sr종목선정|30_매수신호'] = os.path.join(folder_종목선정, '30_매수신호')
dic_폴더정보['sr종목선정|40_매도신호'] = os.path.join(folder_종목선정, '40_매도신호')
dic_폴더정보['sr종목선정|50_종목선정'] = os.path.join(folder_종목선정, '50_종목선정')

# 백테스팅 폴더 정의
folder_백테스팅 = os.path.join(folder_SRline, '백테스팅')
dic_폴더정보['sr백테스팅'] = folder_백테스팅
dic_폴더정보['sr백테스팅|10_매수매도'] = os.path.join(folder_백테스팅, '10_매수매도')
dic_폴더정보['sr백테스팅|20_결과정리'] = os.path.join(folder_백테스팅, '20_결과정리')

#######################################################################################################################

# Transaction Flow 분석 폴더 정의
folder_TransactionFlow = os.path.join(folder_work, 'analyzer_TransactionFlow')
dic_폴더정보['tf분석'] = folder_TransactionFlow

# 종목분석 폴더 정의
folder_종목분석 = os.path.join(folder_TransactionFlow, '종목분석')
dic_폴더정보['tf종목분석'] = folder_종목분석
dic_폴더정보['tf종목분석|00_일봉변동'] = os.path.join(folder_종목분석, '00_일봉변동')
dic_폴더정보['tf종목분석|10_지표생성'] = os.path.join(folder_종목분석, '10_지표생성')
dic_폴더정보['tf종목분석|20_분봉확인'] = os.path.join(folder_종목분석, '20_분봉확인')

# 백테스팅 폴더 정의
folder_백테스팅 = os.path.join(folder_TransactionFlow, '백테스팅')
dic_폴더정보['tf백테스팅'] = folder_백테스팅
dic_폴더정보['tf백테스팅|10_매수매도'] = os.path.join(folder_백테스팅, '10_매수매도')
dic_폴더정보['tf백테스팅|20_결과정리'] = os.path.join(folder_백테스팅, '20_결과정리')
dic_폴더정보['tf백테스팅|30_결과요약'] = os.path.join(folder_백테스팅, '30_결과요약')
dic_폴더정보['tf백테스팅|40_수익요약'] = os.path.join(folder_백테스팅, '40_수익요약')
dic_폴더정보['tf백테스팅|50_매매이력'] = os.path.join(folder_백테스팅, '50_매매이력')

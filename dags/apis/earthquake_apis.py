import requests  # requests 모듈 임포트
from datetime import date, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

def get_api_xmldata(base_url, sdate, edate, auth_key):
    url = "{base_url}?orderTy={order_ty}&frDate={fr_date}&laDate={la_date}&authKey={auth_key}".format(
        base_url=base_url,
        order_ty='xml',
        # fr_date=fr_date,
        fr_date=sdate,
        la_date=edate,
        auth_key=auth_key
    )
        
    #URL 데이터 호출
    try:
        response = requests.get(url)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        xml_data = response.content

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        exit()
    
    return xml_data

def rtn_eq_dataframe(xml_data):
    # XML 데이터 파싱
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        exit()                      
    
    data_list = []
    
    # XML 구조에 따라 데이터를 추출합니다.
    # 여기서는 <info> 태그를 순회하고 그 하위 태그에서 값을 추출하는 예시입니다.
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    root = root.find('earthqueakNoti')
    record = root.find('info')
    if len(root.findall('info')) == 0 or record.text and "등록된 내용이 없습니다" in record.text:
        columns = ['msgCode', 'cntDiv', 'arDiv', 'eqArCdNm', 'eqPt', 'nkDiv', 
                    'tmIssue', 'eqDate', 'magMl', 'magDiff', 'eqDt', 'eqLt', 
                    'eqLn', 'majorAxis', 'minorAxis', 'depthDiff', 'jdLoc', 
                    'jdLocA', 'reFer', 'created_at', 'updated_at']
        return pd.DataFrame(columns=columns)
    
    for record in root.findall('info'):
        msgCode = record.find('msgCode').text if record.find('msgCode') is not None else None
        cntDiv = record.find('cntDiv').text if record.find('cntDiv') is not None else None
        arDiv = record.find('arDiv').text if record.find('arDiv') is not None else None
        eqArCdNm = record.find('eqArCdNm').text if record.find('eqArCdNm') is not None else None
        eqPt = record.find('eqPt').text if record.find('eqPt') is not None else None
        nkDiv = record.find('nkDiv').text if record.find('nkDiv') is not None else None
        tmIssue = record.find('tmIssue').text if record.find('tmIssue') is not None else None
        eqDate = record.find('eqDate').text if record.find('eqDate') is not None else None
        magMl = record.find('magMl').text if record.find('magMl') is not None else None
        magDiff = record.find('magDiff').text if record.find('magDiff') is not None else None
        eqDt = record.find('eqDt').text if record.find('eqDt') is not None else None
        eqLt = record.find('eqLt').text if record.find('eqLt') is not None else None
        eqLn = record.find('eqLn').text if record.find('eqLn') is not None else None
        majorAxis = record.find('majorAxis').text if record.find('majorAxis') is not None else None
        minorAxis = record.find('minorAxis').text if record.find('minorAxis') is not None else None
        depthDiff = record.find('depthDiff').text if record.find('depthDiff') is not None else None
        jdLoc = record.find('jdLoc').text if record.find('jdLoc') is not None else None
        jdLocA = record.find('jdLocA').text if record.find('jdLocA') is not None else None
        reFer = change_line_to_space(record.find('ReFer').text) if record.find('ReFer') is not None else None
        
        # 추출한 데이터를 딕셔너리 형태로 저장
        data_list.append({
            'msgCode': msgCode,
            'cntDiv': cntDiv,
            'arDiv': arDiv,
            'eqArCdNm': eqArCdNm,
            'eqPt': eqPt,
            'nkDiv': nkDiv,
            'tmIssue': tmIssue,
            'eqDate': eqDate,
            'magMl': magMl,
            'magDiff': magDiff,
            'eqDt': eqDt,
            'eqLt': eqLt,
            'eqLn': eqLn,
            'majorAxis': majorAxis,
            'minorAxis': minorAxis,
            'depthDiff': depthDiff,
            'jdLoc': jdLoc,
            'jdLocA': jdLocA,
            'reFer': reFer,
            'created_at':now,
            'updated_at':now
        })

    #Pandas 데이터 프레임 변환
    df = pd.DataFrame(data_list)
    print(df.head())
    
    return df

def change_line_to_space(value):
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ')
    return value
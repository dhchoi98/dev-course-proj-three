# 설명: 날씨 예보 데이터를 수집하고 처리하는 기능을 담당합니다.
# 외부 API에서 날씨 데이터를 수집하고, S3에서 좌표 데이터를 로드하며,
# 데이터를 피벗, 숫자 변환, 상태 매핑(SKY, PTY), 강풍 및 불쾌지수 경고를 추가하여 정제합니다.

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from io import StringIO
from botocore.exceptions import NoCredentialsError
from weather_config import logger, S3_BUCKET, S3_COORDINATES_PATH, API_URL, AUTH_KEY, BASE_DATE, BASE_TIME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

def load_locations_from_s3(bucket, key):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_DEFAULT_REGION
        )
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_content = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content), encoding='utf-8')
        
        required_columns = ['level1', 'level2', 'nx', 'ny']
        if not all(col in df.columns for col in required_columns):
            logger.error(f"coordinates.csv에 필수 컬럼이 누락되었습니다: {required_columns}")
            raise ValueError("Missing required columns in coordinates.csv")
        
        df['level2'] = df['level2'].fillna('')
        locations = df.to_dict('records')
        logger.info(f"Loaded {len(locations)} locations from s3://{bucket}/{key}")
        return locations
    except NoCredentialsError:
        logger.error("AWS credentials not found")
        raise
    except Exception as e:
        logger.error(f"Error loading coordinates.csv from S3: {str(e)}")
        raise

def fetch_data():
    results = []
    locations = load_locations_from_s3(S3_BUCKET, S3_COORDINATES_PATH)

    def fetch_single(location):
        location_name = f"{location['level1']} {location['level2']}".strip() if location['level2'] else location['level1']
        broad_location = location['level1']
        logger.info(f"{location_name} 지역 데이터 수집 중...")
        params = {
            "pageNo": 1,
            "numOfRows": 1000,
            "dataType": "JSON",
            "base_date": BASE_DATE,
            "base_time": BASE_TIME,
            "nx": location['nx'],
            "ny": location['ny'],
            "authKey": AUTH_KEY
        }
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            data = response.json()
            if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
                items = data['response']['body']['items']['item']
                if not items:
                    logger.warning(f"{location_name} 지역 데이터가 비어 있습니다.")
                    return pd.DataFrame()
                df = pd.DataFrame(items)
                df['LOCATION'] = location_name
                df['BROAD_LOCATION'] = broad_location
                return df
            else:
                logger.warning(f"{location_name} 지역 데이터 없음: 응답 구조 비정상")
                return pd.DataFrame()
        except requests.RequestException as e:
            logger.error(f"{location_name} API 요청 실패: {str(e)}")
            return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_single, loc) for loc in locations]
        for future in as_completed(futures):
            df = future.result()
            if not df.empty:
                results.append(df)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

def process_data(df):
    if df.empty:
        logger.warning("처리할 데이터가 없습니다.")
        return df

    try:
        df_pivot = df.pivot(
            index=['baseDate', 'baseTime', 'fcstDate', 'fcstTime', 'nx', 'ny', 'LOCATION', 'BROAD_LOCATION'],
            columns='category',
            values='fcstValue'
        ).reset_index()

        numeric_columns = ['WSD', 'VEC', 'TMP', 'POP', 'REH']
        for col in numeric_columns:
            if col in df_pivot.columns:
                df_pivot[col] = pd.to_numeric(df_pivot[col], errors='coerce')

        sky_map = {'1': '맑음', '2': '구름조금', '3': '구름많음', '4': '흐림'}
        df_pivot['SKY'] = df_pivot.get('SKY', '알수없음').map(lambda x: sky_map.get(x, x))

        def convert_pty(x):
            try:
                val = float(x)
                return {0: '없음', 1: '비', 2: '비/눈', 3: '눈', 4: '눈/비'}.get(val, x)
            except:
                return x
        df_pivot['PTY'] = df_pivot.get('PTY', '알수없음').apply(convert_pty)

        df_pivot['STRONG_WIND_ALERT'] = df_pivot['WSD'].apply(
            lambda x: 'Yes' if pd.notna(x) and x >= 14 else 'No'
        )

        if 'TMP' in df_pivot.columns and 'REH' in df_pivot.columns:
            df_pivot['DI'] = 0.81 * df_pivot['TMP'] + 0.01 * df_pivot['REH'] * (0.99 * df_pivot['TMP'] - 14.3) + 46.3
            df_pivot['DISCOMFORT_ALERT'] = df_pivot['DI'].apply(
                lambda x: '매우 높음 🟥' if x >= 80 else '높음 🟧' if x >= 75 else '보통 🟨'
            )
        else:
            df_pivot['DI'] = None
            df_pivot['DISCOMFORT_ALERT'] = '알 수 없음'

        df_pivot.rename(columns={
            'baseDate': 'BASE_DATE',
            'baseTime': 'BASE_TIME',
           'ducstDate': 'FCST_DATE',
            'fcstTime': 'FCST_TIME',
            'nx': 'NX',
            'ny': 'NY'
        }, inplace=True)

        df_pivot = df_pivot[df_pivot['FCST_DATE'] >= BASE_DATE]
        return df_pivot
    except Exception as e:
        logger.error(f"데이터 처리 중 오류: {str(e)}")
        return pd.DataFrame()
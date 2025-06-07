import requests
from scripts.weather_config import weather_load_api_key, weather_load_base_url
from scripts.weather_utils import parse_weather_text

# 지역 코드 정보
region_name_to_code = {
    "서울": "108",
    "부산": "159",
    "대구": "143",
    "인천": "112",
    "광주": "156",
    "대전": "133",
    "울산": "152",
    "수원": "119",
    "춘천": "101",
    "강릉": "105",
    "청주": "131",
    "전주": "146",
    "제주": "184",
    "포항": "138",
    "여수": "168",
    "창원": "155",
    "목포": "165"
}

# 데이터 추출 함수
def extract_weather(region_name_to_code: str, tm1: str, tm2: str):
    key = weather_load_api_key()
    url = weather_load_base_url()
    
    params = {
        "stn": region_name_to_code,
        "tm1": tm1,
        "tm2": tm2,
        "help": "0",
        "authKey": key
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return parse_weather_text(response.text)
    else:
        raise Exception(f"❌ API 호출 실패: {response.status_code} {response.text}")


def get_region_code(region_name: str) -> str:
    try:
        return region_name_to_code[region_name]
    except KeyError:
        raise ValueError(f"❌ '{region_name}' 지역은 현재 지원하지 않아요.")
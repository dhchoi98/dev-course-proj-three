import json
import os

def weather_load_config():
    config_path = os.path.join(os.path.dirname(__file__), "../config/weather_api_keys.json")
    with open(config_path, "r") as f:
        return json.load(f)

def weather_load_api_key():
    return weather_load_config()["weather_api"]["key"]

def weather_load_base_url(): 
    return weather_load_config()["weather_api"]["base_url"]

# import json
# import os

# print("✅ weather_config.py 시작됨")

# def weather_load_config():
#     config_path = os.path.join(os.path.dirname(__file__), "../config/weather_api_keys.json")
#     print("🔎 config_path:", config_path)
#     try:
#         with open(config_path, "r") as f:
#             return json.load(f)
#     except Exception as e:
#         print("❌ 파일 열기 실패:", e)
#         return {}

# def weather_load_api_key():
#     return weather_load_config().get("weather_api", {}).get("key")

# def weather_load_base_url(): 
#     return weather_load_config().get("weather_api", {}).get("base_url")

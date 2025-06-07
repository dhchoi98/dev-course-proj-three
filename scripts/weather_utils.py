from datetime import datetime, timedelta

def parse_weather_text(text):
    lines = text.strip().splitlines()

    # 데이터 줄만 추출 (숫자로 시작하는 줄)
    data_lines = [line for line in lines if line.strip() and line[0].isdigit()]

    records = []

    for line in data_lines:
        parts = line.split()
        try:
            # 관측시각 (KST)
            tm = parts[0]
            # 풍향 (16방위)
            wd = int(parts[2])
            # 풍속 (m/s)
            ws = float(parts[3])
            # 기온 (C)
            ta = float(parts[11])
            # 상대습도 (%)
            hm = float(parts[13])
            # 강수량(mm)
            rn = float(parts[15])
            # 지면온도 (C)
            ts = float(parts[37])

            record = {
                "timestamp": tm,
                "timestamp_readable": format_simple_datetime(tm),
                "wind_dir": wd if wd != -9 else None,
                "wind_speed": ws if ws != -9.0 else None,
                "temp": ta if ta != -9.0 else None,
                "humidity": hm if hm != -9.0 else None,
                "precipitation": rn if rn != -9.0 else 0.0,  # 강수 없음을 0.0으로
                "ground_temp": ts if ts != -9.0 else None
            }
            records.append(record)

        except (IndexError, ValueError):
            continue

    return records


def summarize_weather(records):
    def avg(values):
        values = [v for v in values if v is not None]
        return round(sum(values) / len(values), 2) if values else None

    result = {
        "avg_temp": avg([r["temp"] for r in records]),
        "wind_speed": avg([r["wind_speed"] for r in records]),
        "precipitation": avg([r["precipitation"] for r in records]),
        "humidity": avg([r["humidity"] for r in records]),
        "ground_temp": avg([r["ground_temp"] for r in records]),
        "wind_dir": avg([r["wind_dir"] for r in records]),  # 참고: 원형 평균은 별도 처리 필요
    }

    # 가장 최근 시각 (마지막 레코드의 readable 시간) 
    if records and "timestamp_readable" in records[-1]:
        result["last_updated"] = records[-1]["timestamp_readable"]

    return result


def calculate_outdoor_score(weather):
    score = 100

    temp = weather["avg_temp"]
    if temp < 10 or temp > 30:
        score -= 40
    elif 10 <= temp < 15 or 26 <= temp <= 30:
        score -= 20

    rain = weather["precipitation"]
    if rain > 0:
        score -= 30

    wind = weather["wind_speed"]
    if wind > 5:
        score -= 10
    elif wind > 3:
        score -= 5

    return max(score, 0)

def format_simple_datetime(timestamp_str: str) -> str:
    dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M")
    return dt.strftime("%Y-%m-%d %H:%M")

def get_last_week_range(execution_date: datetime):
    """
    주어진 execution_date를 기준으로
    - tm1: 일주일 전 월요일 00시
    - tm2: 전날(일요일) 23시
    를 반환한다.
    """
    end_date = execution_date - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    
    tm1 = start_date.strftime("%Y%m%d") + "0000"
    tm2 = end_date.strftime("%Y%m%d") + "2300"
    
    return tm1, tm2
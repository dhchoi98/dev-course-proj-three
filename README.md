# DE06_Team8_Airflow_Project
### 프로그래머스 데이터 엔지니어링 6기 3차 프로젝트 8팀

<br>

## ✍🏻 프로젝트 개요
- 기상청 API 허브에서 제공해주는 데이터 API를 주기적으로 수집하여 대시보드로 시각화
- Airflow 기반 데이터 파이프라인 구축을 통한 자동화
- S3와 Snowflake를 활용한 데이터 저장 및 관리
- Superset(Preset)으로 시각화하여 인사이트 도출
- Slack을 통한 분석 정보 알림 기능 구현
<br>

## 🚀 프로젝트 구조
![Image](https://github.com/user-attachments/assets/619a9a9b-9618-40b1-b174-fc47cae493e8)

<br>

## ⚙️ 기술 스택

<table>
    <thead>
        <tr>
            <th>분류</th>
            <th>기술 스택</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <p>데이터 처리</p>
            </td>
            <td>
                <img src="https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white" />
                <img src="https://img.shields.io/badge/XML-E76F00?logo=xml&logoColor=white" />
                <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" />
            </td>
        </tr>
        <tr>
            <td>
                  <p>데이터 적재</p>
            </td>
            <td>
              <img src="https://img.shields.io/badge/AWS_S3-569A31?logo=amazon-aws&logoColor=white" />
              <img src="https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white" />
            </td>
        </tr>
        <tr>
            <td>
                <p>데이터 파이프라인</p>
            </td>
            <td>
              <img src="https://img.shields.io/badge/Airflow-017CEE?logo=apache-airflow&logoColor=white" />
              <img src="https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white" />
            </td>
        </tr>
                <tr>
            <td>
                <p>컨테이너</p>
            </td>
            <td>
               <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" />
            </td>
        </tr>
                <tr>
            <td>
                <p>데이터 시각화</p>
            </td>
            <td>
              <img src="https://img.shields.io/badge/Preset-1A73E8?logo=apache-superset&logoColor=white" />
              <img src="https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=white" />
            </td>
        </tr>
        <tr>
            <td>
                <p>협업</p>
            </td>
            <td>
              <img src="https://img.shields.io/badge/GitHub-181717?logo=github&logoColor=white" />
              <img src="https://img.shields.io/badge/ZEP-673DE6?logoColor=white" />
              <img src="https://img.shields.io/badge/Slack-4A154B?logo=slack&logoColor=white" />
              <img src="https://img.shields.io/badge/Notion-000000?logo=notion&logoColor=white" />
            </td>
        </tr>
    </tbody>
</table>
<br>

## ✅ Airflow Process
### ETL Process의 기본적인 구조는 S3 -> Snowflake -> Preset 입니다.
### Airflow로 스케줄링 작업을 진행하고 있습니다.

<br>

1. 국내외 지진 정보 조회
- ETL
  - eq_daily_Update_to_Snowflake.py
  - eq_once_Update_to_Snowflake.py
- ELT
  - run_daily_dbt.py

2. 북서 태평양 태풍 정보 조회
- ETL
  - typhoon_daily_Update_to_Snowflake.py
- ELT
  - run_daily_dbt.py

3. 날씨 예보 조회 및 불쾌지수 알림
- ETL
  - kma_weather_dag.py

4. 휴일 날씨 만족도 분석
- ETL
  - weather_pipeline_dag.py

<br>

## 🔁 Data Insights
- Preset
  - 미세먼지 대시보드
  - 국내/해외 지진 발생 정보 대시보드
  - 태풍 발생 정보 대시보드
- Slack
  - 날씨 예보 및 불쾌지수 알림
  - 휴일 날씨 만족도 알림

<br>

## 💻 시각화 세부 결과
### Preset Dashboard
 
![Image](https://github.com/user-attachments/assets/89fa7530-88c4-407e-8dca-7f64e1c848eb)
![Image](https://github.com/user-attachments/assets/2127de84-5a4f-43e5-90ab-2ec9703cff88)
![Image](https://github.com/user-attachments/assets/d46b4cb4-fa24-4367-830b-b42df54dc19c)
![Image](https://github.com/user-attachments/assets/d259334d-29e4-4d65-b8a1-207b9fb0a2f9)

### Slack Alert

![Image](https://github.com/user-attachments/assets/e4a8ad18-d3f1-4111-b318-53fa02d475a7)
![Image](https://github.com/user-attachments/assets/318d9d63-392a-4dda-b368-205fb3cd4275)
![Image](https://github.com/user-attachments/assets/eaf07ad6-b38f-408f-b833-7e757021b9ed)
![Image](https://github.com/user-attachments/assets/f3c24b4a-0bff-4213-b50a-df87c80a752e)

# DE32-3rd_team5

## 기술스택                                                                                                             <img src="https://img.shields.io/badge/python 3.11-3776AB?style=plastic&logo=python&logoColor=white">  <img src="https://img.shields.io/badge/FastAPI-009688?style=plastic&logo=fastapi&logoColor=white">  <img src="https://img.shields.io/badge/Streamlit 1.29.0-FF4B4B?style=plastic&logo=fastapi&logoColor=white">  <img src="https://img.shields.io/badge/airflow-017CEE?style=plastic&logo=apacheairflow&logoColor=white">  <img src="https://img.shields.io/badge/mariaDB-003545?style=plastic&logo=mariaDB&logoColor=white">  <img src="https://img.shields.io/badge/apachespark 3.5.1-E25A1C?style=plastic&logo=apachespark&logoColor=white">

## 프로젝트 개요
얼굴 인식을 활용하여 유동 인구 파악하기

## 아키텍처
![image](https://github.com/user-attachments/assets/94ddb666-13a7-441a-960f-9bd1691785c1)

## 지역 인구 활동 데이터 분석 및 시각화
이 프로젝트의 주요 목적은 지역 인구 활동 데이터를 수집하고 분석하여 사용자에게 유용한 정보를 제공하는 것입니다.

데이터 수집:
지역 인구 활동 데이터를 매 시간별로 수집하여 이를 효율적으로 저장하고 관리합니다. 
데이터는 파티셔닝을 통해 연도, 월, 일, 시간별로 분류되어 저장되며, 이를 통해 데이터 접근성과 관리 효율성을 높이고자 합니다.

데이터 시각화 및 분석:
Streamlit을 활용하여 수집된 데이터를 사용자에게 시각적으로 보여주고, 사용자 지정 기간에 따른 데이터 조회 및 필터링을 지원합니다. 예를 들어, 특정 기간 동안의 인구 활동 분포도를 지도 상에 시각화하고, 필터링을 통해 성별이나 평균 점수와 같은 통계적 정보를 제공합니다.

## 기능
Streamlit 접속
카메라를 통한 얼굴 인식
  - 43.201.252.238:8090
관리자
  - 43.201.252.238:8091

## PPT
[PPT](https://docs.google.com/presentation/d/1SGar9_-OyyddAMztfljl8G9gBTSYg6XWr_eULdLLcKA/edit#slide=id.g309f0248bd2_0_161)

## 참조 모델
https://huggingface.co/NTQAI/pedestrian_gender_recognition

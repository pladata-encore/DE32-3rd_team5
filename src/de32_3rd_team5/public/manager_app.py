import streamlit as st
import os
from datetime import datetime, timedelta
import pandas as pd

# 파티셔닝된 데이터의 기본 경로 설정
partitioning_dir = os.getenv(
    "PARTITINNING_DIR", os.path.expanduser("~/pyspark_data/origin_data")
)

# 사이드바에 셀렉터로 페이지 선택
page = st.sidebar.selectbox(
    "Select a page",
    [
        "Home",
        "시간별 스코어 평균 값",
        "지역별 활동 분포도",
        "[Sample] 위도 경도 및 사진 넣기",
    ],
)

# 선택한 페이지에 따라 내용 표시
if page == "Home":
    st.title("Welcome to the Home Page")
    st.write("This is the home page.")

elif page == "시간별 스코어 평균 값":
    st.title("시간별 스코어 평균 값")

    # 파티션 선택을 위한 입력
    year = st.sidebar.selectbox("Year", list(range(2020, 2031)), index=4)
    month = st.sidebar.selectbox("Month", list(range(1, 13)), index=9)
    day = st.sidebar.selectbox("Day", list(range(1, 32)), index=9)
    hour = st.sidebar.selectbox("Hour", list(range(0, 24)), index=2)

    # 파일 경로 설정
    path = os.path.join(
        partitioning_dir,
        f"year={year}",
        f"month={month}",
        f"day={day}",
        f"hour={hour}",
    )

    # 데이터 로드
    if os.path.exists(path):
        try:
            df = pd.read_parquet(path, engine="pyarrow")
            st.write(f"Loaded data for {year}-{month:02d}-{day:02d} {hour:02d}:00")

            # 'score' 컬럼을 숫자형으로 변환하며, 잘못된 형식의 데이터는 NaN으로 처리
            if "score" in df.columns:
                df["score"] = pd.to_numeric(df["score"], errors="coerce")

            st.dataframe(df)

            # 필터링과 같은 추가 기능 제공
            filter_gender = st.selectbox("Filter by Gender", ["All", "Male", "Female"])
            if filter_gender != "All":
                filtered_df = df[df["gender"] == filter_gender]
                st.write(f"Filtered Data ({filter_gender}):")
                st.dataframe(filtered_df)

            # 평균 점수 계산 등의 추가 통계 정보 제공
            if "score" in df.columns:
                avg_score = df["score"].mean()
                st.write(f"score 평균 값: {avg_score:.2f}")
            else:
                st.warning("'score' column not found in the data.")

        except Exception as e:
            st.error(
                f"{path}해당 경로의 데이터를 읽어 들이다 에러가 발생했습니다 : {e}"
            )
    else:
        st.warning(
            f" {year}-{month:02d}-{day:02d} {hour:02d}:00 해당 일자의 데이터를 찾을 수 없습니다."
        )
elif page == "지역별 활동 분포도":
    st.title("지역 인구 활동 분포도")

    # 날짜 범위 선택 위젯 추가
    start_date = st.date_input("시작 날짜", datetime.now() - timedelta(days=7))
    end_date = st.date_input("종료 날짜", datetime.now())

    # 날짜가 올바른지 검증
    if start_date > end_date:
        st.error("시작 날짜는 종료 날짜보다 이전이어야 합니다.")
    else:
        # 사용자에게 시간 선택을 위한 입력도 추가
        start_hour = st.sidebar.selectbox("시작 시간", list(range(0, 24)), index=0)
        end_hour = st.sidebar.selectbox("종료 시간", list(range(0, 24)), index=23)

        # 선택한 날짜와 시간에 맞는 파일 경로 설정 및 데이터 로드
        dataframes = []
        for single_date in pd.date_range(start=start_date, end=end_date):
            year = single_date.year
            month = single_date.month
            day = single_date.day
            for hour in range(start_hour, end_hour + 1):
                path = os.path.join(
                    partitioning_dir,
                    f"year={year}",
                    f"month={month}",
                    f"day={day}",
                    f"hour={hour}",
                )

                # 파일 경로가 존재하는지 확인하고 데이터를 읽어들임
                if os.path.exists(path):
                    try:
                        df = pd.read_parquet(path, engine="pyarrow")
                        dataframes.append(df)
                    except Exception as e:
                        st.error(
                            f"{path} 해당 경로의 데이터를 읽어들이는 중 오류 발생: {e}"
                        )

        # 데이터가 있는 경우 병합하여 사용
        if dataframes:
            df = pd.concat(dataframes, ignore_index=True)

            # 위치 데이터 시각화
            if "latitude" in df.columns and "longitude" in df.columns:
                # 위도와 경도 열을 숫자형으로 변환
                df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
                df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

                # 변환 후 NaN 값을 제거
                location_df = df[["latitude", "longitude"]].dropna()

                # 데이터 시각화
                st.write("위치 데이터 시각화")
                st.map(location_df)

                # 추가 분석 기능 제공
                st.write("탐지 개수:", len(df))
            else:
                st.warning("위치 데이터가 없습니다.")
        else:
            st.warning(
                f"{start_date} ~ {end_date} 기간 동안의 데이터를 찾을 수 없습니다."
            )


elif page == "[Sample] 위도 경도 및 사진 넣기":

    st.title("🌃 샘플 사진 등록")
    uploaded_file = st.file_uploader("Choose a CSV file", accept_multiple_files=False)

    if uploaded_file is not None:
        bytes_data = uploaded_file.read()
        st.write("파일이 정상적으로 등록되었습니다.")
    else:
        st.write("사진을 업로드 해주세요.")

    if st.button("제출"):
        st.write("사진이 제출되었습니다.")

    st.title("🧑‍🌾 샘플 위도 경도 등록")

    latitude = st.text_input("위도")
    longitude = st.text_input("경도")

    if st.button("등록"):
        if latitude and longitude:
            try:
                st.success("데이터가 성공적으로 저장되었습니다!")
            except Exception as e:
                st.error(f"데이터 저장 중 오류 발생: {e}")
        else:
            st.warning("위도와 경도를 입력해주세요.")

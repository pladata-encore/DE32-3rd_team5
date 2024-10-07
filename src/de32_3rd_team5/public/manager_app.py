import streamlit as st
import pymysql
import os

conn = pymysql.connect(
    host=os.getenv("MANAGER_ST_DB_HOST", "43.201.252.238"),
    port=os.getenv("MANAGER_ST_DB_PORT", 32768),
    user=os.getenv("MANAGER_ST_DB_USER", "pic"),
    password=os.getenv("MANAGER_ST_DB_PASSWORD", "1234"),
    db=os.getenv("MANAGER_ST_DB", "picturedb"),
    charset="utf8",
)

# 사이드바에 셀렉터로 페이지 선택
page = st.sidebar.selectbox(
    "Select a page", ["Home", "Page 1", "[Sample] 위도 경도 및 사진 넣기"]
)

# 선택한 페이지에 따라 내용 표시
if page == "Home":
    st.title("Welcome to the Home Page")
    st.write("This is the home page.")
elif page == "Page 1":
    st.title("Page 1")
    st.write("This is the first page.")
elif page == "[Sample] 위도 경도 및 사진 넣기":

    st.title("🌃 샘플 사진 등록")
    uploaded_file = st.file_uploader("Choose a CSV file", accept_multiple_files=False)

    if uploaded_file is not None:
        bytes_data = uploaded_file.read()
        st.write("파일이 정상적으로 등록되었습니다.")
    else:
        st.write("사진을 업로드 해주세요.")

    if st.button("제출"):
        ...

    st.title("🧭 샘플 위도 경도 등록")

    latitude = st.text_input("위도")
    longitude = st.text_input("경도")

    if st.button("등록"):
        if latitude and longitude:
            try:
                with conn.cursor() as cursor:
                    # Insert query 실행
                    sql = "INSERT INTO samp_position (latitude, longitude) VALUES (%s, %s)"
                    cursor.execute(sql, (latitude, longitude))
                    conn.commit()  # 변경사항 저장
                st.success("데이터가 성공적으로 저장되었습니다!")
            except pymysql.MySQLError as e:
                st.error(f"데이터베이스 오류 발생: {e}")
            finally:
                conn.close()  # DB 연결 종료
        else:
            st.warning("위도와 경도를 입력해주세요.")

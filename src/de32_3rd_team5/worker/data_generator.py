import random
import os
import requests
import pymysql


def get_connection():
    connection = pymysql.connect(
        host=os.getenv("DB_IP", "43.201.252.238"),
        user="pic",
        password="1234",
        port=int(os.getenv("MY_PORT", "32768")),
        database="picturedb",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


# 이미지 선택 함수
def select_pic():
    py_path = __file__
    py_dir_path = os.path.dirname(py_path)
    man_image = os.path.join(py_dir_path, "image_man.jpg")
    girl_image = os.path.join(py_dir_path, "image_girl.jpg")

    pics = [man_image, girl_image]
    pic = random.choice(pics)
    return pic


# 구글 Geocoding API를 사용해 위도, 경도로부터 주소 구하기 (역지오코딩)
def get_address_from_google(latitude, longitude):
    # 구글 API 키 가져오기
    google_api_key = os.getenv(
        "GOOGLE_API_KEY", "AIzaSyDl4Nte4s05r0q1CrPcUjyD-aZHudnGiOs"
    )

    # 구글 Geocoding API의 역지오코딩 엔드포인트 URL
    url = "https://maps.googleapis.com/maps/api/geocode/json"

    # API 요청에 필요한 파라미터
    params = {
        "latlng": f"{latitude},{longitude}",
        "key": google_api_key,
        "language": "ko",
    }

    # API 요청 보내기
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            address = data["results"][0]["formatted_address"]
            return address
        else:
            print(f"해당 위치의 주소를 찾을 수 없습니다: {latitude}, {longitude}")
            return None
    else:
        print(f"API 요청 실패: {response.status_code}, {response.text}")
        return None


# 라인 알림 전송
def send_line_noti(message):
    api_url = "https://notify-api.line.me/api/notify"
    token = os.getenv("LINE_NOTI_TOKEN", "NULL")
    headers = {"Authorization": "Bearer " + token}

    data = {"message": message}

    resp = requests.post(api_url, headers=headers, data=data)
    print(resp.text)
    print("SEND LINE NOTI")


# 데이터베이스에서 랜덤 위도, 경도 가져오기
def get_random_lat_lon():
    connection = get_connection()
    cursor = connection.cursor()

    # 랜덤으로 위도, 경도를 가져오는 SQL 쿼리
    sql = "SELECT latitude, longitude FROM samp_position ORDER BY RAND() LIMIT 1"
    cursor.execute(sql)

    result = cursor.fetchone()  # 랜덤으로 선택된 한 행 가져오기
    cursor.close()
    connection.close()

    if result:
        return result["latitude"], result["longitude"]  # latitude와 longitude 반환
    else:
        print("위도와 경도를 가져오는 데 실패했습니다.")
        return None, None


def upload_file_to_fastapi_server(label="male"):
    pic = select_pic()
    latitude, longitude = get_random_lat_lon()

    if latitude is None or longitude is None:
        print("유효한 위도, 경도 데이터를 찾을 수 없습니다.")
        return

    # FastAPI 서버의 엔드포인트 URL 및 포트 설정
    api_url = f'http://{os.getenv("AUTO_API_REQUEST", "43.201.252.238")}:{os.getenv("MANAGER_ST_DB_PORT", "8070")}/uploadpic'
    print(f"선택된 좌표: 위도 {latitude}, 경도 {longitude}")
    # 파일을 FastAPI 서버로 업로드
    try:
        with open(pic, "rb") as f:
            files = {"file": (pic, f, "image/jpeg")}
            data = {
                "label": "male",  # 라벨 값 설정
                "latitude": latitude,
                "longitude": longitude,
            }

            # POST 요청 보내기
            response = requests.post(api_url, files=files, data=data)

            # 응답 확인
            if response.status_code == 200:
                print(f"Face saved as: {pic} and uploaded successfully.")
            else:
                print(
                    f"Failed to upload {pic}: {response.status_code}, {response.text}"
                )

    except Exception as e:
        print(f"파일 업로드 중 오류 발생: {e}")


# 데이터 전송 및 테스트용 실행
def run():
    import datetime

    send_line_noti(
        f"""[TEAM 5]
샘플 데이터 생성 Woker 시작
실행 시각 : {datetime.datetime.now()}
"""
    )

    print("==== 등록 프로세스 시작 ====")

    for i in range(int(os.getenv("TEST_COUNT", 20))):
        upload_file_to_fastapi_server()

    print("==== Sample 데이터 생성 완료 ====")

    # 라인 알림
    send_line_noti(
        f"""[TEAM 5]
샘플 데이터 생성 완료
종료 시간 : {datetime.datetime.now()}
"""
    )

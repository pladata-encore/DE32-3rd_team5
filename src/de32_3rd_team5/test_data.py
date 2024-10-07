import random
import os
import requests

# 이미지 선택 함수
def select_pic():
    pics = ["./image_man.jpg", "./image_girl.jpg"]
    pic = random.choice(pics)
    return pic

# 구글 Geocoding API를 사용해 위도, 경도로부터 주소 구하기 (역지오코딩)
def get_address_from_google(latitude, longitude):
    # 구글 API 키 가져오기
    google_api_key = os.getenv("GOOGLE_API_KEY", "AIzaSyDl4Nte4s05r0q1CrPcUjyD-aZHudnGiOs")

    # 구글 Geocoding API의 역지오코딩 엔드포인트 URL
    url = "https://maps.googleapis.com/maps/api/geocode/json"

    # API 요청에 필요한 파라미터
    params = {
        "latlng": f"{latitude},{longitude}",
        "key": google_api_key,
        "language": "ko"
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
    token = os.getenv('LINE_NOTI_TOKEN', 'NULL')
    headers = {'Authorization': 'Bearer ' + token}

    data = {
        "message": message
    }

    resp = requests.post(api_url, headers=headers, data=data)
    print(resp.text)
    print("SEND LINE NOTI")


# 데이터 전송 및 테스트용 실행
def run():
    # 사진 선택
    print("==== 등록 프로세스 시작 ====")

    for i in range(int(os.getenv("TEST_COUNT", 20))):
        pic = select_pic()

        # 임시 랜덤 좌표 목록 (위도, 경도)
        locations = [
            (37.5665, 126.9780),  # 서울특별시 위도, 경도
            (37.3943, 127.1107),  # 경기도 성남시 위도, 경도
            (35.1587, 129.1604)   # 부산광역시 위도, 경도
        ]

        # 랜덤 좌표 선택
        latitude, longitude = random.choice(locations)
        print(f"선택된 좌표: 위도 {latitude}, 경도 {longitude}")

        # 위도, 경도로부터 주소 구하기
        address = get_address_from_google(latitude, longitude)
        if address:
            print(f"구해진 주소: {address}")

#            # 이미지와 주소를 가지고 HTTP 통신 (예: 서버로 전송)
#            server_url = "https://example.com/upload"
#            files = {"image": open(pic, "rb")}
#            data = {
#                "address": address,
#                "latitude": latitude,
#                "longitude": longitude
#            }
#
#            # POST 요청 보내기
#            response = requests.post(server_url, files=files, data=data)
#            if response.status_code == 200:
#                print(f"서버 응답 성공: {response.text}")
#            else:
#                print(f"서버 응답 실패: {response.status_code}")
#
    print("==== Sample 데이터 생성 완료 ====")

    # 라인 알림
    send_line_noti("완료")

# 테스트 실행
if __name__ == "__main__":
    run()


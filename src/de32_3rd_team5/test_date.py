import random
import os
import requests

# 이미지 선택 함수
def select_pic():
    pics = ["./image_man.jpg", "./image_girl.jpg"]
    pic = random.choice(pics)
    return pic

# 구글 Geocoding API를 사용해 주소로부터 위도, 경도 구하기
def get_geocode_from_google(address):
    # 구글 API 키 가져오기
    google_api_key = os.getenv("GOOGLE_API_KEY", "AIzaSyDl4Nte4s05r0q1CrPcUjyD-aZHudnGiOs")

    # 구글 Geocoding API의 엔드포인트 URL
    url = "https://maps.googleapis.com/maps/api/geocode/json"

    # API 요청에 필요한 파라미터
    params = {
        "address": address,
        "key": google_api_key
    }

    # API 요청 보내기
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            location = data["results"][0]["geometry"]["location"]
            lat = location["lat"]
            lon = location["lng"]
            return lat, lon
        else:
            print(f"주소를 찾을 수 없습니다: {address}")
            return None, None
    else:
        print(f"API 요청 실패: {response.status_code}, {response.text}")
        return None, None

# 데이터 전송 및 테스트용 실행 함수
def run():
    # 사진 선택
    print("==== 등록 프로세스 시작 ====")

    for _ in range(int(os.getenv("TEST_COUNT", 20))):
        pic = select_pic()

        # 임시 랜덤 주소 목록
        addresses = [
            "서울특별시 강남구 테헤란로 427",
            "경기도 성남시 분당구 판교역로 235",
            "부산광역시 해운대구 해운대해변로 203"
        ]

        # 랜덤 주소 선택
        address = random.choice(addresses)
        print(f"선택된 주소: {address}")

        # 주소로부터 위도와 경도 구하기
        latitude, longitude = get_geocode_from_google(address)
        if latitude and longitude:
            print(f"위도: {latitude}, 경도: {longitude}")

#            # 이미지와 위도, 경도를 가지고 HTTP 통신 (예: 서버로 전송)
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

# 테스트 실행
if __name__ == "__main__":
    run()


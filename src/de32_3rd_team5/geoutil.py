
from geopy.geocoders import Nominatim

def loc_trans(location):
    geolocator = Nominatim(user_agent='South Korea', timeout=None)
    address = geolocator.reverse(location)

    try:
        str_address = str(address)
        address_list = str_address.split(',')

        # 주소 구성 요소 추출 (예시)
        if len(address_list) >= 5:  # 충분한 요소가 있는지 확인
            result = f"{address_list[3].strip()} {address_list[2].strip()} {address_list[1].strip()} {address_list[0].strip()} {address_list[4].strip()}"
        else:
            # 주소 형식이 예상과 다른 경우 처리
            result = str_address  # 전체 주소 문자열 사용 또는 기본값 설정
            print(f"Unexpected address format: {str_address}")

    except IndexError as e:
        # 에러 발생 시 로그 기록 및 기본값 반환
        print(f"Error extracting address: {e}, address: {address}")
        result = "Unknown Address"

    return result

a = loc_trans('36.048366, 125.534064')
print(a)

from fastapi import FastAPI, Query
import os

app = FastAPI()

# 데이터 경로
file_path = "/home/hun/data/add_data"

# 전역 변수로 첫 번째 함수에서 저장된 도, 시, 구 정보 유지
stored_data = []

# 경상남도를 입력하면 해당 도, 시, 구 정보 반환
def get_full_location_data(location: str):
    global stored_data  # 전역 변수 사용

    if not os.path.exists(file_path):
        return {"error": f"파일을 찾을 수 없습니다: {file_path}"}

    matching_lines = []

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if location in line:
                # 도, 시, 구 정보 모두 가져오기
                full_location = line.strip()
                matching_lines.append(full_location)

    if not matching_lines:
        return {"message": f"'{location}'에 대한 정보를 찾을 수 없습니다."}

    # 전역 변수에 결과 저장
    stored_data = matching_lines
    return matching_lines

# 시를 입력하면 해당 시에 속한 구 정보 반환
def get_district_from_stored_data(sub_location: str):
    global stored_data  # 전역 변수 사용

    if not stored_data:
        return {"error": "이전에 검색된 도의 정보가 없습니다. 먼저 도를 검색해 주세요."}

    matching_districts = []

    for line in stored_data:
        if sub_location in line:
            district = line.strip().split(",")[2]  # 구 정보만 가져오기
            matching_districts.append(district)

    if not matching_districts:
        return {"message": f"'{sub_location}'에 대한 구 정보를 찾을 수 없습니다."}

    return matching_districts


@app.get("/location")
def get_location(location: str = Query(..., description="검색할 도명을 입력하세요")):
    result = get_full_location_data(location)
    return {"matching_data": result}


@app.get("/sub_location")
def get_sub_location(sub_location: str = Query(..., description="세부 시명을 입력하세요")):
    result = get_district_from_stored_data(sub_location)
    return {"matching_districts": result}


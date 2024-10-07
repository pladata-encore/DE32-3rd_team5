from fastapi import FastAPI, Query
import os

app = FastAPI()

# 주소 데이터를 포함하는 파일 경로
file_path = "/home/hun/data/add_data"

# 하나의 함수에서 도, 시를 받아 처리하는 함수
@app.get("/location")
def get_location(
    location: str = Query(..., description="검색할 도명을 입력하세요"),
    sub_location: str = Query(None, description="세부 시명을 입력하세요")
):
    if not os.path.exists(file_path):
        return {"error": f"파일을 찾을 수 없습니다: {file_path}"}

    matching_lines = []

    # 파일 읽기
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            # location에 따른 도, 시, 구 전체 데이터를 필터링
            if location in line:
                matching_lines.append(line.strip())

    # sub_location이 입력되었을 경우 해당 시에 속하는 구 정보 필터링
    if sub_location:
        matching_lines = [line for line in matching_lines if sub_location in line]

    if not matching_lines:
        return {"message": f"'{location}' 및 '{sub_location}'에 대한 데이터를 찾을 수 없습니다."}

    return {"matching_data": matching_lines}


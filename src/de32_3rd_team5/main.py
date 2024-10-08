from transformers import pipeline
from fastapi import FastAPI, UploadFile, Form, HTTPException
from typing import Annotated
from datetime import datetime
from jigutime import jigu
from geopy.geocoders import Nominatim
from PIL import Image
import pytz
import os
import exifread
import pymysql.cursors

app = FastAPI()

gender_classifier = pipeline(model="NTQAI/pedestrian_gender_recognition")

request_user = "t5"


@app.get("/docs")
def test():
    re = {"test": "okay"}
    return re


@app.post("/uploadpic")
async def create_upload_file(
    file: UploadFile,
    label: Annotated[str, Form()],
    latitude: Annotated[float, Form()],
    longitude: Annotated[float, Form()],
):
    # 파일 저장
    korea = datetime.now(pytz.timezone("Asia/Seoul"))
    request_time = korea.strftime("%Y-%m-%d %H:%M:%S")

    img = await file.read()
    file_name = file.filename
    # file_ext = file.content_type.split('/')[-1]  #"image/png"
    # 디렉토리가 없으면 오류, 코드에서 확인 및 만들기 추가
    upload_dir = os.getenv("UPLOAD_DIR", "/home/young12/code/DE32-3rd_team5/img")
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)
    import uuid

    ffpath = os.path.join(upload_dir, f"{uuid.uuid4()}.png")

    with open(ffpath, "wb") as f:
        f.write(img)

    results = gender_classifier(ffpath)

    con = pymysql.connect(
        host=os.getenv("MANAGER_ST_DB_HOST", "172.17.0.1"),
        port=os.getenv("MANAGER_ST_DB_PORT", 32768),
        user=os.getenv("MANAGER_ST_DB_USER", "pic"),
        password=os.getenv("MANAGER_ST_DB_PASSWORD", "1234"),
        db=os.getenv("MANAGER_ST_DB", "picturedb"),
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor,
    )

    gender = results[0]["label"]
    score_index = results[0]["score"]
    score = round(score_index, 2)

    values = (
        file_name,
        ffpath,
        gender,
        score,
        request_time,
        request_user,
        latitude,
        longitude,
    )
    sql = "INSERT INTO picture (file_name, file_path, gender, score, request_time, request_user, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    with con:
        with con.cursor() as cursor:
            cursor.execute(sql, values)
            con.commit()

    return {
        "filename": file_name,
        "ontent_type": file.content_type,
        "file_path": ffpath,
        "request_user": request_user,
        "result": results,
        "성별": gender,
        "예측점수": score,
        "위도": latitude,
        "경도": longitude,
    }

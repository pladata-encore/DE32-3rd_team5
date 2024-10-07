from transformers import pipeline
from fastapi import FastAPI, UploadFile, Form, HTTPException
from typing import Annotated
from datetime import datetime
from jigutime import jigu
import pytz
import os
from geopy.geocoders import Nominatim
from PIL import Image
import exifread

app = FastAPI()

gender_classifier = pipeline(model="NTQAI/pedestrian_gender_recognition")

@app.get("/gender")
def pic():
    image_path = "https://newsimg.sedaily.com/2021/12/09/22V85NTJGY_1.jpg"
    results = gender_classifier(image_path)
    return results

@app.post("/uploadpic")
async def create_upload_file(file: UploadFile, label: Annotated[str, Form()], latitude: Annotated[float, Form()], longitude: Annotated[float, Form()]):
    # 파일 저장
    korea = datetime.now(pytz.timezone('Asia/Seoul'))
    request_time = korea.strftime('%Y-%m-%d %H:%M:%S')

    img = await file.read()
    file_name = file.filename
    #file_ext = file.content_type.split('/')[-1]  #"image/png"
    # 디렉토리가 없으면 오류, 코드에서 확인 및 만들기 추가
    upload_dir = os.getenv('UPLOAD_DIR', '/home/young12/code/DE32-3rd_team5/img')
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)
    import uuid
    ffpath = os.path.join(upload_dir, f'{uuid.uuid4()}.png')

    with open(ffpath, "wb") as f:
        f.write(img)

    results = gender_classifier(ffpath)

    #geolocator = Nominatim(user_agent="my_geocoder_app")
    #location = geolocator.geocode("Seoul")

    return {
            "filename": file_name,
            "ontent_type": file.content_type,
            "file_path": ffpath,
            "label": label,
            "result": results,
            "위도": latitude,
            "경도": longitude,
            }

from fastapi import FastAPI, File, UploadFile
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
from fastapi import Request
from transformers import pipeline
from PIL import Image

import os
import json
import random
import io
import uuid

app = FastAPI()

@app.get("/")
def read_root():
    return {"DE32's 3rd Project": "Team 5 Work's root"}

@app.post("/uploadfile/")
async def upload_image(file: UploadFile):

    img = await file.read()
    filename = f"{uuid.uuid4()}.png"
    upload_folder = "uploads"
    if not os.path.exists(upload_folder):
        os.makedirs(upload_folder)
    with open(os.path.join(upload_folder, filename), "wb") as f:
        f.write(img)

    image = Image.open(io.BytesIO(img))

    return {
        "DE32's 3rd Project": "Team 5 Work's section for upload Imagefile",
    }

@app.get("/uploads/{filename}")
async def get_uploaded_file(filename: str):
    return FileResponse(os.path.join("uploads", filename))

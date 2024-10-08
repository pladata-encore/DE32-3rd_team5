FROM python:3.11

WORKDIR /code

# FastAPI main 복제
COPY /src/de32_3rd_team5/main.py /code/

# 얼굴 인식 streamlit 복제
COPY /src/de32_3rd_team5/public/recognition.py /code/
COPY haarcascade_frontalface_default.xml /code/


ENV STREAMLIT_SERVER_HEADLESS=true

RUN apt-get update && apt-get install -y vim libgl1-mesa-glx libglib2.0-0

RUN pip install --no-cache-dir opencv-python opencv-python-headless

RUN pip install --no-cache-dir --upgrade git+https://github.com/pladata-encore/DE32-3rd_team5.git@main

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port 8070 --reload & streamlit run recognition.py --server.port 8090"]

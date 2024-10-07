FROM python:3.11

WORKDIR /code

COPY /src/de32_3rd_team5/main.py /code/

RUN apt-get update && apt-get install -y vim

RUN pip install --no-cache-dir --upgrade git+https://github.com/pladata-encore/DE32-3rd_team5.git@main


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]

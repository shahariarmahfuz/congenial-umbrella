FROM python:3.10-slim

WORKDIR /app

RUN apt-get update &&     apt-get install -y ffmpeg &&     apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8000"]

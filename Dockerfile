FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir mutagen beets

COPY app.py .
COPY beets.yaml /beets/config.yaml

CMD ["python", "app.py"]

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV MULTICAST_GROUP=239.0.0.1 \
    MULTICAST_PORT=5007 \
    MULTICAST_TTL=1

CMD ["python", "main.py"]

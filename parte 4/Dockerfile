FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV AMQP_URL=amqp://guest:guest@rabbitmq:5672/ \
    UPDATES_EXCHANGE=kv4_updates \
    RPC_PORT=50071 \
    ROLE=follower

CMD ["python", "node_main.py"]


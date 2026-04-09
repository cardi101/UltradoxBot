FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py db.py parser.py broadcaster.py handlers.py main.py ./

VOLUME ["/data"]

ENV BOT_TOKEN=""
ENV ADMIN_CHAT_ID="443539115"
ENV ADMIN_USERNAME="Cardinal_GriseX"
ENV DB_PATH="/data/bot_data.db"

CMD ["python", "main.py"]

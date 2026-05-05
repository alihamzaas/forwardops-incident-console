FROM python:3.12-slim

WORKDIR /app

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend ./backend
COPY docs ./docs
COPY run_local.sh ./run_local.sh

ENV LISTEN_HOST=0.0.0.0
ENV PORT=8000

EXPOSE 8000

CMD ["python", "-m", "backend.main"]

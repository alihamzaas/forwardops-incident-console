FROM node:22-slim AS web

WORKDIR /web
COPY web/package.json ./package.json
RUN npm install
COPY web ./
RUN npm run build

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend ./backend
COPY docs ./docs
COPY samples ./samples
COPY --from=web /web/dist ./web/dist
COPY run_local.sh ./run_local.sh

ENV LISTEN_HOST=0.0.0.0
ENV PORT=8000
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["sh", "-c", "uvicorn backend.fastapi_app:app --host ${LISTEN_HOST:-0.0.0.0} --port ${PORT:-8000}"]

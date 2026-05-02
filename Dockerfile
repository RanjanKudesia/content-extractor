# ---- Build stage ----
FROM python:3.12-slim AS builder

WORKDIR /app
COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- Runtime stage ----
FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

EXPOSE 8000
CMD ["sh", "-c", "gunicorn main:app -w 2 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:${PORT:-8000}"]

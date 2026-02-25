FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -f http://localhost:5000/readyz || exit 1

CMD ["gunicorn", "--workers", "1", "--threads", "8", "--timeout", "180", "--bind", "0.0.0.0:5000", "wsgi:app"]

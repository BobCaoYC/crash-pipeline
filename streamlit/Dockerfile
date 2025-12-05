# Pipeline/streamlit/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install \
      streamlit \
      requests \
      boto3 \
      pika \
      duckdb \
      pandas \
      streamlit-autorefresh \
      croniter \
      reportlab \
      minio \
      scikit-learn==1.6.1 joblib

# copy the main app, overview, and ALL pages
# main script should be named Home.py inside the container
COPY Home.py /app/Home.py
COPY overview.md /app/overview.md
COPY pages/ /app/pages/
COPY artifacts/ /app/artifacts/

# (optional) prove which build you’re on:
ARG BUILD_TAG=dev
ENV BUILD_TAG=${BUILD_TAG}

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]

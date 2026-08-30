FROM python:3.9-slim
# Install build tools for vaex-core
RUN apt-get update && apt-get install -y \
    g++ \
    gcc \
    libpcre3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py", "--dataset", "/data/test.csv"]


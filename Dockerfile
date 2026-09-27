FROM python:3.12-slim

WORKDIR /app

# System deps needed for pip packages that compile C extensions,
# including RocksDB (which is blocked on Windows but installs cleanly here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    pkg-config \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["python", "app.py", "worker", "-l", "info"]
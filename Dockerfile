FROM python:3.12-slim

WORKDIR /app

# System deps (pdfplumber needs a few basics; keep it lean)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

ENV PYTHONUNBUFFERED=1

# Default command does nothing; you will run specific modules via docker compose
CMD ["python", "-c", "print('pipeline container ready')"]

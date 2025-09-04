# Imagen pequeña, sin apt-get
FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Instalar deps Python (todas en wheel/binario)
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiar código
COPY app.py .

# Variables por defecto (puedes sobreescribir en compose)
ENV PORT=8000
EXPOSE 8000

# Arranque del servidor ASGI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

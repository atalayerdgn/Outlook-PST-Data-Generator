FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Yalnızca gerekli bağımlılıklar (faker için ek sistem bağımlılığı gerekmiyor)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodu
COPY datagen.py ./

# Çalışma zamanı için metadata ve output dizinleri (host volümlerle override edilecek)
RUN mkdir -p /data/metadata /data/output

ENTRYPOINT ["python", "datagen.py"]
# Varsayılan: Mevcut mount edilen metadata'yı birleştirip çıktı üretir
CMD ["-m", "/data/metadata", "--make-accounts", "20", "--emails-per-account", "150", "-o", "/data/output"]


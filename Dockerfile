FROM python:3.13-slim

LABEL org.opencontainers.image.title="fyta-exporter" \
      org.opencontainers.image.description="Reads FYTA plant sensors and pushes Prometheus metrics by remote_write" \
      org.opencontainers.image.source="https://github.com/vinitu/fyta-exporter" \
      org.opencontainers.image.licenses="MIT"

# No pip packages: the script uses the Python standard library only.
# PYTHONDONTWRITEBYTECODE keeps the image usable with a read-only root filesystem.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY fyta_exporter.py /app/fyta_exporter.py

# 65534 is "nobody": no home directory, nothing to write.
USER 65534:65534
WORKDIR /app

ENTRYPOINT ["python", "/app/fyta_exporter.py"]

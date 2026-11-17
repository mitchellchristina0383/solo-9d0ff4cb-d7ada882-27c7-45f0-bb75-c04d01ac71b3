FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JOBLOG_DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY joblog ./joblog

RUN useradd --create-home --uid 10001 joblog \
    && mkdir -p /data/joblogs /data/backups /data/restores \
    && chown -R joblog:joblog /data
USER joblog

EXPOSE 8000

VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status==200 else 1)"

CMD ["uvicorn", "joblog.main:app", "--host", "0.0.0.0", "--port", "8000"]

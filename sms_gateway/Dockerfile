FROM python:3.12-slim
WORKDIR /srv
RUN pip install --no-cache-dir prometheus_client==0.26.0
COPY app ./app
RUN useradd --create-home smsgw && mkdir -p /data && chown -R smsgw /srv /data
USER smsgw
EXPOSE 8092 9100
CMD ["python", "-m", "app.main"]

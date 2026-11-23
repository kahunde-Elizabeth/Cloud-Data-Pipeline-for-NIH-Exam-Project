FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN useradd --create-home sensorbridge && chown -R sensorbridge /srv
USER sensorbridge
EXPOSE 9100
CMD ["python", "-m", "app.main"]

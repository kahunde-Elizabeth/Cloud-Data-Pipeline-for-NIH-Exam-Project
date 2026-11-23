FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN useradd --create-home sensorsim && chown -R sensorsim /srv
USER sensorsim
EXPOSE 9100 8091
CMD ["python", "-m", "app.main"]

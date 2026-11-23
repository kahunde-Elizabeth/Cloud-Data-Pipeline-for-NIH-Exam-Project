# Test tool (generator + reconcile). Runs as root so it can write to the ./results bind mount
# on any host; it is not part of the pipeline.
FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENTRYPOINT ["python", "-m", "app.generate"]

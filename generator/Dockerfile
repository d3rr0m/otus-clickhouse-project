FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metro_sim/ metro_sim/
COPY config.yaml .

ENTRYPOINT ["python", "-m", "metro_sim.main"]

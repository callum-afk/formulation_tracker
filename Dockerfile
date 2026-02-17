FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Persist build-time git metadata into a file consumed by app config for footer version display.
ARG APP_BUILD_VERSION=dev
RUN printf "%s\n" "$APP_BUILD_VERSION" > app/version.txt

ENV PORT=8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

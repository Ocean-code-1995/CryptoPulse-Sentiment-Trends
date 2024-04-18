FROM python:3.9-slim

WORKDIR /app

# copy the dashboard folder to the /app directory
COPY dashboard .
# requiremenets.txt file #
COPY dashboard/dashboard_requirements.txt /app/dashboard_requirements.txt
# secrets file #
COPY secrets/cryptopulse-secret.json /app/secrets/cryptopulse-secret.json

# secrets file
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# expose port for the dashboard
EXPOSE 8500

# upgrade pip & install the required packages  for the dashboard
RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --verbose -r /app/dashboard_requirements.txt

# environment variables for the secrets file
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/cryptopulse-secret.json

# entrypoint for the dashboard
ENTRYPOINT [ "streamlit", "run", "/app/dashboard_app.py", "--server.port", "8500", "--server.headless", "True", "server.address", "0.0.0.0" ]

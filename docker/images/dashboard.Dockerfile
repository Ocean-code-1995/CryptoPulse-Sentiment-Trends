FROM python:3.10

WORKDIR /app

# requiremenets.txt file #
COPY ./requirements/dashboard_req.txt /app/requirements.txt

# secrets file 
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r /app/requirements.txt

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health


FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/gx

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY serve_data_docs.sh /usr/local/bin/serve_data_docs.sh
RUN chmod +x /usr/local/bin/serve_data_docs.sh

CMD ["bash", "-lc", "sleep infinity"]

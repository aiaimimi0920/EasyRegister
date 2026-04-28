ARG REGISTER_SERVICE_BASE_IMAGE=python:3.10-bookworm
FROM ${REGISTER_SERVICE_BASE_IMAGE} AS runtime
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    REGISTER_OUTPUT_ROOT=/shared/register-output \
    PYTHONPATH=/app/server/services/orchestration_service/src:/app/server/services/python_shared/src

COPY server ./server
COPY README.md ./README.md

RUN python -c "import curl_cffi" \
    || (apt-get -o Acquire::Retries=5 update \
        && apt-get -o Acquire::Retries=5 install -y --fix-missing --no-install-recommends ca-certificates \
        && rm -rf /var/lib/apt/lists/* \
        && python -m pip install --no-cache-dir --retries 10 --timeout 300 curl-cffi)

CMD ["python", "-m", "infinite_runner"]

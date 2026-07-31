FROM python:3.12.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN pip install --no-cache-dir uv==0.8.15 \
    && groupadd --system sentinelstream \
    && useradd --system --gid sentinelstream --create-home sentinelstream

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --group database --group api --group monitoring

COPY src ./src
COPY migrations ./migrations
COPY scripts ./scripts
COPY alembic.ini configs ./
COPY docker/api-entrypoint.sh ./docker/api-entrypoint.sh
RUN uv sync --frozen --no-dev --group database --group api --group monitoring \
    && chmod 0755 ./docker/api-entrypoint.sh \
    && mkdir -p data reports \
    && chown -R sentinelstream:sentinelstream /app

USER sentinelstream

EXPOSE 8000
HEALTHCHECK --interval=20s --timeout=5s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/live', timeout=3)"

ENTRYPOINT ["/app/docker/api-entrypoint.sh"]
CMD ["uvicorn", "sentinelstream.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

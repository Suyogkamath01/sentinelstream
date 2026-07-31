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
RUN uv sync --frozen --no-dev --no-install-project --group dashboard --group api
COPY src ./src
COPY configs ./configs
RUN uv sync --frozen --no-dev --group dashboard --group api \
    && mkdir -p data reports \
    && chown -R sentinelstream:sentinelstream /app

USER sentinelstream

EXPOSE 8501
HEALTHCHECK --interval=20s --timeout=5s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=3)"

CMD ["streamlit", "run", "src/sentinelstream/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

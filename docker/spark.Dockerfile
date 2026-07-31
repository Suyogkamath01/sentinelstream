FROM apache/spark:4.2.0-python3

USER root
WORKDIR /app

RUN python -m pip install --no-cache-dir \
    "numpy>=2.0,<3" \
    "pandas>=2.2,<3" \
    "pyarrow>=16,<23" \
    "pydantic>=2.8,<3" \
    "pydantic-settings>=2.4,<3" \
    "python-dotenv>=1.0,<2" \
    "pyyaml>=6.0,<7" \
    "scikit-learn>=1.5,<2" \
    "confluent-kafka>=2.5,<3"

COPY src ./src
COPY scripts ./scripts
COPY configs ./configs
COPY pyproject.toml README.md LICENSE ./
RUN groupadd --system sentinelstream \
    && useradd --system --gid sentinelstream --create-home sentinelstream \
    && mkdir -p data/checkpoints data/streaming \
    && chown -R sentinelstream:sentinelstream /app

USER sentinelstream
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

CMD ["python", "scripts/run_spark_stream.py"]

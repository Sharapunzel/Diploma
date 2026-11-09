FROM eclipse-temurin:17.0.17_10-jre-jammy@sha256:e071b8279f46ec62d7d9edcb254fc16589f74219b0808032981aa23f48a36c15 AS jre
FROM python:3.12-slim AS ml
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/opt/java/openjdk RCF_CLASSPATH=/opt/rcf
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
WORKDIR /app
COPY ml /opt/rcf
RUN python /opt/rcf/fetch.py && python -m pip install --no-index /opt/rcf/*.whl
COPY pyproject.toml ./
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
RUN python -m pip install '.[test]' && useradd --system --uid 10001 appuser
COPY tests ./tests
COPY scripts ./scripts
USER 10001:10001
CMD ["python", "-m", "app.detector_worker"]

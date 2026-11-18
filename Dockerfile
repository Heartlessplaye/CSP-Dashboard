# syntax=docker/dockerfile:1

# ---- base: runtime dependencies only ---------------------------------------
FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# ---- dev: adds test tooling; source is bind-mounted by docker compose -------
FROM base AS dev
COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt
COPY . .
CMD ["pytest", "-q"]

# ---- runtime: processor / dashboard / loadgen image (default target) --------
FROM base AS runtime
COPY csp_traffic ./csp_traffic
COPY dashboard ./dashboard
COPY tools ./tools
COPY plugins ./plugins
COPY config ./config
COPY samples ./samples
RUN useradd --uid 1000 --create-home app && mkdir -p input output state && chown app:app input output state
USER app
CMD ["python", "-m", "csp_traffic", "run"]

# Multi-stage Docker build for Finviz Scraper
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy application code
COPY src/ ./src/
COPY pyproject.toml .
COPY README.md .
COPY config/ ./config/

# Install the package with all dependencies
RUN pip install --no-cache-dir -e .[viz]

# Create data directories
RUN mkdir -p /app/data/latest /app/data/history /app/data/cache

# Production stage
FROM base as production

# Run as non-root user
RUN useradd -m -u 1000 scraper && \
    chown -R scraper:scraper /app
USER scraper

# Default command
CMD ["python", "-m", "finviz_weekly", "run", "--mode", "universe", "--ticker-limit", "500", "--out", "/app/data"]

# Development stage
FROM base as development

# Install dev dependencies
RUN pip install --no-cache-dir pytest pytest-cov pre-commit black isort flake8 mypy

# Keep container running for development
CMD ["/bin/bash"]

# Dashboard stage - runs Streamlit dashboard
FROM base as dashboard

COPY dashboard/ ./dashboard/
COPY examples/ ./examples/

EXPOSE 8501

# Run as non-root user
RUN useradd -m -u 1000 scraper && \
    chown -R scraper:scraper /app
USER scraper

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# API stage - runs FastAPI server
FROM base as api

# Install API dependencies
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    python-jose[cryptography] \
    passlib[bcrypt] \
    python-multipart \
    sqlalchemy \
    psycopg2-binary \
    alembic

COPY api/ ./api/

EXPOSE 8000

# Run as non-root user
RUN useradd -m -u 1000 api && \
    chown -R api:api /app
USER api

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

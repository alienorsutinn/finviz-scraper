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
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY pyproject.toml .
COPY README.md .

# Install the package
RUN pip install --no-cache-dir -e .

# Create data directory
RUN mkdir -p /app/data

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

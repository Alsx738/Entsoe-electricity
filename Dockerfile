FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy dependency files first (leverages Docker layer cache)
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtualenv
RUN uv sync --frozen

# Copy the rest of the project (src/, borders.json, countries.json, etc.)
COPY . .

# Point uv to the pre-built virtualenv
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Run the ingestion script
ENTRYPOINT ["python"]
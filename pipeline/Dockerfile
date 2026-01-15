FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# Install dependencies
WORKDIR /tmp

ENV PATH="/tmp/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./

RUN uv sync --locked
COPY pipeline.py .

# # Set entry point
ENTRYPOINT ["python", "pipeline.py"]
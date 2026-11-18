FROM python:3.13-slim
WORKDIR /app
# Print as it happens, so docker compose logs shows the exchange at once.
ENV PYTHONUNBUFFERED=1
# The connector comes only from the network (no PyPI fallback anyone could publish to), then its dependencies from
# PyPI. It brings the MCP SDK the seller's server is written with.
ARG EKL_INDEX=https://eukollab.ai/simple/
RUN pip install --no-cache-dir --no-deps --index-url "$EKL_INDEX" ekl-provider-adapter \
    && pip install --no-cache-dir ekl-provider-adapter "uvicorn>=0.32"
COPY seller/server.py ./

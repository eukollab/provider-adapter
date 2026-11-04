FROM python:3.13-slim
WORKDIR /app
# The adapter comes only from the network (no PyPI fallback anyone could publish to), then its dependencies from PyPI.
ARG EKL_INDEX=https://eukollab.ai/simple/
RUN pip install --no-cache-dir --no-deps --index-url "$EKL_INDEX" ekl-provider-adapter \
    && pip install --no-cache-dir ekl-provider-adapter "uvicorn>=0.32"
COPY seller.py buyer.py ./

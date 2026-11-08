# The provider connector: the adapter as its own container next to an existing MCP server.
#   docker build -t ekl-provider-connector .
#   docker run -e EKL_AGENT_JSON="$(cat agent.json)" -e EKL_PROVIDER_CREDENTIAL=… -e EKL_NETWORK_URL=… \
#     -e EKL_UPSTREAM_MCP_URL=… -e EKL_ADAPTER_ENDPOINT=https://…/ekl -p 8000:8000 ekl-provider-connector
# One image serves every publisher: it is configured by environment variables alone, with no build per agent.
#   EKL_AGENT_JSON           agent.json's content, or base64 of it (or mount the file and set EKL_AGENT_FILE)
#   EKL_PROVIDER_CREDENTIAL  the agent key
#   EKL_NETWORK_URL          the network
#   EKL_UPSTREAM_MCP_URL     the existing server's MCP address, which may carry the server's own token
#   EKL_ADAPTER_ENDPOINT     this container's public address of /ekl, announced and proved to the network
#   EKL_AGENT_CREDENTIAL, EKL_CALLBACK_URL  optional: the agent's own side and the publisher's callback route
#   EKL_ADAPTER_STORAGE      durable mode's journal store: a postgresql:// URL, or a SQLite file on a volume (/data)
#   PORT                     default 8000
# GET /healthz answers 200 for load balancers and container health checks.
FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[postgres]"
RUN useradd --create-home connector && mkdir /data && chown connector /data
USER connector
# Its own writable folder: an agent that takes part keeps its journal in .ekl/ there.
WORKDIR /home/connector
ENV PORT=8000 PYTHONUNBUFFERED=1
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD python -c \
    "import os, urllib.request as u; u.urlopen('http://127.0.0.1:' + os.environ['PORT'] + '/healthz', timeout=4)"
CMD ["ekl-provider-connector"]

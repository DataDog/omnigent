# Conductor builds this exact checkout; it must never clone or fetch Omnigent.
ARG PYTHON_IMAGE=registry.ddbuild.io/images/python:3.12.8
ARG NODE_IMAGE=registry.ddbuild.io/images/nodejs:24.13.1-gbi-noble

FROM ${NODE_IMAGE} AS web-builder
USER root
WORKDIR /src
COPY pnpm-workspace.yaml pnpm-lock.yaml ./
COPY package.json ./
COPY web/package.json web/package.json
COPY web/electron/package.json web/electron/package.json
COPY web/ web/
RUN rm -rf web/node_modules \
 && npm install -g --force pnpm@11.15.1 \
 && pnpm install --frozen-lockfile --filter web... \
 && pnpm --filter web run build

FROM ${PYTHON_IMAGE} AS python-builder
USER root
ARG UV_VERSION=0.7.19
ARG HAB_LAUNCHER_WHEEL_URL=https://binaries.ddbuild.io/dd-source/python/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl
ARG HAB_LAUNCHER_WHEEL_SHA256=375bffc76904a919bb6953333909535ba94e131b20842a8132d56dbd23d339ef
WORKDIR /src
COPY pyproject.toml setup.py uv.lock README.md LICENSE NOTICE ./
COPY sdks/ sdks/
COPY omnigent/ omnigent/
COPY examples/ examples/
COPY --from=web-builder /src/omnigent/server/static/web-ui omnigent/server/static/web-ui
# uv installs the local SDKs as editable path dependencies. Keep this tree in
# the runtime image and install exactly the checked-in lockfile.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && python -m pip install --no-cache-dir "uv==${UV_VERSION}" \
 && uv venv /opt/venv \
 && UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-dev \
 && uv pip install --python /opt/venv/bin/python 'psycopg[binary]>=3.1,<4' \
 && curl --fail --location --retry 3 --output /tmp/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl "${HAB_LAUNCHER_WHEEL_URL}" \
 && echo "${HAB_LAUNCHER_WHEEL_SHA256}  /tmp/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl" | sha256sum --check \
 && uv pip install --python /opt/venv/bin/python --no-deps /tmp/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl \
 && rm /tmp/omnigent_hab_launcher-0.0.132434155-py3-none-any.whl

FROM ${PYTHON_IMAGE}
USER root
ARG SOURCE_URL
ARG VCS_REF
ARG VERSION
LABEL org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.vendor="Datadog" \
      com.datadoghq.team="workspaces"
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl openssh-client \
 && rm -rf /var/lib/apt/lists/* \
 && install -d -o 501 -g 0 /data/artifacts
COPY --from=python-builder --chown=501:0 /opt/venv /opt/venv
COPY --from=python-builder --chown=501:0 /src /src
ENV PATH=/opt/venv/bin:${PATH} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    ARTIFACT_DIR=/data/artifacts \
    OMNIGENT_SOURCE_URL=${SOURCE_URL} \
    OMNIGENT_SOURCE_REVISION=${VCS_REF} \
    OMNIGENT_BUILD_VERSION=${VERSION}
USER 501
WORKDIR /src
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1
CMD ["omnigent", "server", "--host", "0.0.0.0", "--port", "8000"]

# ------------ Build stage ------------
FROM "docker.io/vernemq/vernemq:2.1.2-alpine" AS build

USER root
WORKDIR /src

RUN apk upgrade --no-cache

# Toolchain + git/curl for rebar3/hex
RUN apk add --no-cache build-base git curl bash ca-certificates

# Use the VerneMQ Erlang/OTP toolchain from the base image
ENV PATH="/vernemq/erts-15.2.6/bin:/vernemq/bin:${PATH}" \
    ERL_LIBS=/vernemq/lib \
    HOME=/tmp \
    VERNEMQ_VERSION=2.1.2 \
    REBAR3_VERSION=3.25.1 \
    ERTS_VERSION=15.2.6

# Rebar3
ADD https://github.com/erlang/rebar3/releases/download/${REBAR3_VERSION}/rebar3 /usr/local/bin/rebar3
RUN chmod +x /usr/local/bin/rebar3

# Fetch vmq_commons headers so -include_lib("vmq_commons/include/vmq_types.hrl") works
RUN set -eux; \
  mkdir -p /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/include; \
  mkdir -p /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/src; \
  curl -fsSL \
    https://raw.githubusercontent.com/vernemq/vernemq/${VERNEMQ_VERSION}/apps/vmq_commons/include/vmq_types.hrl \
    -o /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/include/vmq_types.hrl; \
  curl -fsSL \
    https://raw.githubusercontent.com/vernemq/vernemq/${VERNEMQ_VERSION}/apps/vmq_commons/src/vmq_types_mqtt.hrl \
    -o /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/src/vmq_types_mqtt.hrl; \
  curl -fsSL \
    https://raw.githubusercontent.com/vernemq/vernemq/${VERNEMQ_VERSION}/apps/vmq_commons/src/vmq_types_mqtt5.hrl \
    -o /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/src/vmq_types_mqtt5.hrl; \
  curl -fsSL \
    https://raw.githubusercontent.com/vernemq/vernemq/${VERNEMQ_VERSION}/apps/vmq_commons/src/vmq_types_common.hrl \
    -o /vernemq/lib/vmq_commons-${VERNEMQ_VERSION}/src/vmq_types_common.hrl

# Source – only what we need
COPY rebar.config .
COPY src ./src

# Compile with prod profile
RUN rebar3 as prod compile

# ------------ Runtime stage -----------
FROM "docker.io/vernemq/vernemq:2.1.2-alpine"

USER root

RUN apk upgrade --no-cache

# Ensure target dir exists
RUN mkdir -p /opt/vmq-enrich/lib

# Copy compiled apps into /opt, then into proper APP-VSN dirs under /vernemq/lib
COPY --from=build /src/_build/prod/lib /opt/vmq-enrich/lib

RUN set -eux; \
  for APPDIR in /opt/vmq-enrich/lib/*; do \
    [ -d "$APPDIR/ebin" ] || { echo "Skipping $(basename "$APPDIR") (no ebin)"; continue; }; \
    APPFILE="$(find "$APPDIR/ebin" -maxdepth 1 -name '*.app' | head -n1)"; \
    [ -n "$APPFILE" ] || { echo "Skipping $(basename "$APPDIR") (no .app)"; continue; }; \
    APP="$(basename "$APPFILE" .app)"; \
    VSN="$(sed -n 's/.*{vsn,[[:space:]]*\"\([^\"]\+\)\".*/\1/p' "$APPFILE" | head -n1)"; \
    [ -n "$VSN" ] || VSN="0.0.0"; \
    DEST="/vernemq/lib/${APP}-${VSN}"; \
    mkdir -p "$DEST"; \
    cp -a "$APPDIR/ebin" "$DEST/"; \
    if [ -d "$APPDIR/priv" ]; then cp -a "$APPDIR/priv" "$DEST/"; fi; \
  done; \
  chown -R 10000:10000 /vernemq/lib; \
  rm -rf /opt/vmq-enrich

USER 10000

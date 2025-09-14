# syntax=docker/dockerfile:1.7-labs

FROM ghcr.io/astral-sh/uv:python3.13-alpine AS base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONHASHSEED=random \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    UV_SYSTEM_PYTHON=1

# Pillow runtime dependencies
# TODO: remove testing repository when alpine 3.22 is released (libraqm is only on edge for now)
RUN apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community libraqm-dev && \
    apk add --no-cache tiff-dev jpeg-dev openjpeg-dev zlib-dev freetype-dev \
    lcms2-dev libwebp-dev tcl-dev tk-dev harfbuzz-dev fribidi-dev \
    libimagequant-dev libxcb-dev libpng-dev libavif-dev

ARG UID GID
RUN addgroup -S ballsdex -g ${GID:-1000} && adduser -S ballsdex -G ballsdex -u ${UID:-1000}
WORKDIR /code

FROM base AS builder-uv

# Pillow build dependencies
RUN apk add --no-cache gcc libc-dev

# Bring in project sources for editable install during uv sync --active
COPY . /code/
# Create a dedicated virtual environment and install locked dependencies into it
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/ \
    UV_ACTIVE=1 uv sync --frozen --active

FROM base AS production
COPY --from=builder-uv /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
USER ballsdex

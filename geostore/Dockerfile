FROM ubuntu:20.04 as build

RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    curl python-is-python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3059
RUN curl --location --show-error --silent --output install-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py \
    && echo '9c6e45c9ed5c572e129417b9b626f16cc87e6dd6632647463c2cce4bd6038f8a install-poetry.py' > install-poetry.py.sha256 \
    && sha256sum --check install-poetry.py.sha256 \
    && python3 install-poetry.py \
    && rm install-poetry.py install-poetry.py.sha256
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN ~/.local/bin/poetry install --extras=${task} --no-dev --no-root


FROM ubuntu:20.04

ENTRYPOINT ["/opt/.venv/bin/python", "-bb", "-m", "src.task.task"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

USER 10000:10000

COPY --from=build /opt/.venv /opt/.venv

COPY geostore/*.py /src/
ARG task
COPY geostore/${task} /src/task/

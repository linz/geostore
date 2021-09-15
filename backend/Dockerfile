FROM ubuntu:20.04 as build

RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    curl python-is-python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3059
RUN curl --location --show-error --silent --output install-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py \
    && echo 'daad01ac0c1636f1c0154575c6b3b37a0867e9cedd67d1224fc4259c07b03a86 install-poetry.py' > install-poetry.py.sha256 \
    && sha256sum --check install-poetry.py.sha256 \
    && python3 install-poetry.py \
    && rm install-poetry.py install-poetry.py.sha256
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN ~/.local/bin/poetry install --extras=${task} --no-dev --no-root


FROM ubuntu:20.04

ENTRYPOINT ["/opt/.venv/bin/python", "-m", "src.task.task"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

USER 10000:10000

COPY --from=build /opt/.venv /opt/.venv

COPY backend/*.py /src/
ARG task
COPY backend/${task} /src/task/

FROM ubuntu:20.04 as build

RUN apt-get update --quiet --quiet \
    && apt-get install --quiet --quiet --assume-yes --no-install-recommends \
    curl python3-pip > /dev/null \
    && rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3059
RUN curl --location --show-error --silent --output get-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py \
    && echo '6e0b57c76d27e3bea0ad12db7bb8cc4c6eabb7dad361bf6180441253d6bd383c get-poetry.py' > get-poetry.py.sha256 \
    && sha256sum --check get-poetry.py.sha256 \
    && python3 get-poetry.py > /dev/null \
    && rm get-poetry.py get-poetry.py.sha256
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN ~/.poetry/bin/poetry install --quiet --extras=${task} --no-dev


FROM ubuntu:20.04

ENTRYPOINT ["/opt/.venv/bin/python", "-m", "src.task.task"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update --quiet --quiet \
    && apt-get install --quiet --quiet --assume-yes --no-install-recommends \
    ca-certificates python3 > /dev/null \
    && rm -rf /var/lib/apt/lists/*

USER 10000:10000

COPY --from=build /opt/.venv /opt/.venv

COPY backend/*.py /src/
ARG task
COPY backend/${task} /src/task/

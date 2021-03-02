FROM ubuntu:20.04 as build

RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends curl python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN curl --location --show-error --silent --output get-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py \
    && echo 'bfc42cff9cb49bb74f6a1fe12c37fb08bcf7a49245a441c272dfd25254d8ae39 get-poetry.py' > get-poetry.py.sha256 \
    && sha256sum --check get-poetry.py.sha256 \
    && python3 get-poetry.py \
    && rm get-poetry.py get-poetry.py.sha256
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN ~/.poetry/bin/poetry install --extras=${task} --no-dev


FROM ubuntu:20.04

ENTRYPOINT ["/opt/.venv/bin/python", "-m", "src.task.task"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*

USER 10000:10000

COPY --from=build /opt/.venv /opt/.venv

COPY backend/*.py /src/
ARG task
COPY backend/${task} /src/task/

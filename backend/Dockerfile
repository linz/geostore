FROM ubuntu:20.04 as build

RUN apt-get update --quiet --quiet \
    && apt-get install --quiet --quiet --assume-yes --no-install-recommends \
    curl python3-pip > /dev/null \
    && rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3059
RUN curl --location --show-error --silent --output install-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py \
    && echo 'b35d059be6f343ac1f05ae56e8eaaaebb34da8c92424ee00133821d7f11e3a9c install-poetry.py' > install-poetry.py.sha256 \
    && sha256sum --check install-poetry.py.sha256 \
    && python3 install-poetry.py > /dev/null \
    && rm install-poetry.py install-poetry.py.sha256
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN ~/.local/bin/poetry install --quiet --extras=${task} --no-dev


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

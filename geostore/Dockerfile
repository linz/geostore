FROM ubuntu:20.04 as build

RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    curl python-is-python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
# hadolint ignore=DL3059
RUN curl --location --show-error --silent --output install-poetry.py https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py \
    && echo 'e8c7f91d5bbf4d0795919cd062eafbb350bec0c7579cb61d05c2112f224935a2 install-poetry.py' > install-poetry.py.sha256 \
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

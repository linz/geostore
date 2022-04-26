FROM ubuntu:22.04 as build

RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends \
    curl python-is-python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip==21.3.1 \
    && pip install --no-cache-dir poetry==1.1.11
COPY poetry.lock poetry.toml pyproject.toml /opt/
WORKDIR /opt
ARG task
RUN poetry install --extras=${task} --no-dev --no-root


FROM ubuntu:22.04

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

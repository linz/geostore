FROM ubuntu:20.04 as build

RUN apt-get update \
    && apt-get install -y curl python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -
COPY poetry.lock poetry.toml pyproject.toml /opt/
RUN cd /opt && ~/.poetry/bin/poetry install --extras=check-stac-metadata-job


FROM ubuntu:20.04

ENTRYPOINT ["/opt/task.py"]

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y python3 && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/.venv /opt/.venv

COPY datalake/backend/processing/check_stac_metadata/stac-spec/collection-spec/json-schema/collection.json /opt/stac-spec/collection-spec/json-schema/collection.json
COPY datalake/backend/processing/check_stac_metadata/task.py /opt/

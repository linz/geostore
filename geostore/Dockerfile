ARG python_version
FROM python:${python_version}-slim as build

ARG task
ARG packaging

RUN python -m venv /opt/.venv

COPY poetry.lock poetry.toml pyproject.toml /opt/
COPY ${packaging}/${task}.txt /opt/

RUN /opt/.venv/bin/pip install --no-cache-dir --no-deps --requirement=/opt/${task}.txt


ARG python_version
FROM python:${python_version}-slim

ENTRYPOINT ["/opt/.venv/bin/python", "-bb", "-m", "src.task.task"]

USER 10000:10000

COPY --from=build /opt/.venv /opt/.venv

COPY geostore/*.py /src/
ARG task
COPY geostore/${task} /src/task/

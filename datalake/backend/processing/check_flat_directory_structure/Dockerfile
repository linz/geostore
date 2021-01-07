FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install --assume-yes --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY datalake/backend/processing/check_flat_directory_structure/task.py /opt/task.py
RUN chmod +x /opt/task.py
ENTRYPOINT ["/opt/task.py"]

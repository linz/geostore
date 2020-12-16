FROM ubuntu:20.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install -y python3 && rm -rf /var/lib/apt/lists/*
COPY backend/processing/check_flat_directory_structure/task.py /opt/task.py
RUN chmod +x /opt/task.py
ENTRYPOINT ["/opt/task.py"]

FROM ubuntu:18.04
ENV DEBIAN_FRONTEND noninteractive
RUN apt update && apt install -y python3
COPY task.py /opt/task.py
RUN chmod +x /opt/task.py
ENTRYPOINT ["python3", "/opt/task.py"]

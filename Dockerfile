FROM python:3.6.6

RUN apt-get update && apt-get install -y \
    ca-certificates \
    git \
    ssh \
    tar \
    gzip \
    make \
    netcat \
    postgresql-common

ADD requirements.txt /

RUN pip install -r requirements.txt

CMD ["/bin/sh"]
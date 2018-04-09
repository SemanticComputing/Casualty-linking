FROM python:alpine3.7

RUN echo "https://dl-3.alpinelinux.org/alpine/v3.7/main" >> /etc/apk/repositories \
    && echo "https://dl-3.alpinelinux.org/alpine/v3.7/community" >> /etc/apk/repositories
RUN apk add gcc gfortran python-dev build-base --no-cache --virtual .build-deps \
    && apk add git curl raptor2 libreoffice openjdk8-jre ruby openblas-dev --no-cache --update

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN apk del .build-deps

COPY . /app/

ARG warsa_endpoint_url
ARG arpa_url

ENV WARSA_ENDPOINT_URL=${warsa_endpoint_url}
ENV ARPA_URL=${arpa_url}

CMD ["./process.sh"]

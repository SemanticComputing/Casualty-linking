FROM python:3.6.5-stretch

RUN echo deb http://http.debian.net/debian stretch-backports main >> /etc/apt/sources.list

RUN apt-get update && apt-get -t stretch-backports install -y git curl raptor2-utils libreoffice openjdk-8-jre

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . /app/

ARG warsa_endpoint_url
ARG arpa_url

ENV WARSA_ENDPOINT_URL=${warsa_endpoint_url}
ENV ARPA_URL=${arpa_url}

CMD ["./process.sh"]

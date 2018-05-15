#!/usr/bin/env bash

export WARSA_ENDPOINT_URL=${WARSA_ENDPOINT_URL:-http://localhost:3030/warsa}

curl -f --data-urlencode "query=$(cat SPARQL/person_links.sparql)" $WARSA_ENDPOINT_URL -v > input/person_links.json
sed -r -i 's;narc-menehtyneet1939-45;warsa/casualties;g' input/person_links.json

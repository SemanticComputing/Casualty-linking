#!/usr/bin/env bash

curl -f --data-urlencode "query=$(cat SPARQL/person_links.sparql)" http://ldf.fi/warsa/sparql -v > output/person_links.json
sed -r -i 's;narc-menehtyneet1939-45;warsa/casualties;g' output/person_links.json

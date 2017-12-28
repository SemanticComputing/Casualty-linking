#!/usr/bin/env bash

mkdir -p output
mkdir -p data/new

command -v rapper >/dev/null 2>&1 || { echo >&2 "rapper is not available, aborting"; exit 1; }

echo "Converting to csv" &&
libreoffice --headless --convert-to csv:"Text - txt - csv (StarCalc)":44,34,76,1,1,11,true data/casualties.xlsx --outdir data &&

echo "Converting to ttl" &&
python csv_to_rdf.py data/casualties.csv --outdata=output/new/surma.ttl --outschema=output/new/schema.ttl &&

echo "Done"

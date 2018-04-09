#!/usr/bin/env bash

mkdir -p output

echo "Converting to csv" &&
libreoffice --headless --convert-to csv:"Text - txt - csv (StarCalc)":44,34,76,1,1,11,true data/casualties.xlsx --outdir data &&

echo "Converting to ttl" &&
python src/csv_to_rdf.py data/casualties.csv --outdata=output/surma.ttl --outschema=output/schema.ttl &&

echo "Done"

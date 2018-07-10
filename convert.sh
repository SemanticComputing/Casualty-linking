#!/usr/bin/env bash

set -eo pipefail

mkdir -p output

echo "Converting to csv"
libreoffice --headless --convert-to csv:"Text - txt - csv (StarCalc)":44,34,76,1,1,11,true data/casualties.xlsx --outdir data

if [ "$1" ]
then
    echo "Using only topmost $1 rows"
    mv data/casualties.csv data/casualties_full.csv
    head -n $1 data/casualties_full.csv > data/casualties.csv
fi

echo "Converting to ttl"
python src/csv_to_rdf.py data/casualties.csv --outdata=output/_casualties_initial.ttl --outschema=output/_schema.ttl

echo "Finalizing schema"
cat input/schema_base.ttl output/_schema.ttl | rapper - $BASE_URI -i turtle -o turtle > output/casualties_schema.ttl

echo "Done"

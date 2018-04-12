#!/bin/sh

mkdir -p output

echo "Converting to csv" &&
libreoffice --headless --convert-to csv:"Text - txt - csv (StarCalc)":44,34,76,1,1,11,true data/casualties.xlsx --outdir data &&

if [ "$1" ]
then
    echo "Using only topmost $1 rows"
    mv data/casualties.csv data/casualties_full.csv &&
    head -n $1 data/casualties_full.csv > data/casualties.csv
fi

echo "Converting to ttl" &&
python src/csv_to_rdf.py data/casualties.csv --outdata=output/casualties_initial.ttl --outschema=output/schema.ttl &&

echo "Done"

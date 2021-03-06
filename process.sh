#!/usr/bin/env bash

set -eo pipefail

mkdir -p output/logs

command -v rapper >/dev/null 2>&1 || { echo >&2 "rapper is not available, aborting"; exit 1; }

export WARSA_ENDPOINT_URL=${WARSA_ENDPOINT_URL:-http://localhost:3030/warsa}
export ARPA_URL=${ARPA_URL:-http://demo.seco.tkk.fi/arpa}
export BASE_URI="http://ldf.fi/"
export LOG_LEVEL="DEBUG"

./convert.sh $1

python src/process.py output/_casualties_initial.ttl output/_casualties_processed.ttl --arpa_pnr $ARPA_URL/pnr_municipality
cat input/cas_additions.ttl >> output/_casualties_processed.ttl

echo "Linking ranks"
python src/linker.py ranks output/_casualties_processed.ttl output/_rank_links.ttl --endpoint $WARSA_ENDPOINT_URL/sparql \
    --logfile output/logs/linker.log --loglevel $LOG_LEVEL

echo "Linking units"
python src/linker.py units output/_casualties_processed.ttl output/_unit_links.ttl --endpoint $WARSA_ENDPOINT_URL/sparql \
    --arpa $ARPA_URL/warsa_casualties_actor_units --logfile output/logs/linker.log --loglevel $LOG_LEVEL

echo "Linking occupations"
python src/linker.py occupations output/_casualties_processed.ttl output/_occupation_links.ttl --endpoint $WARSA_ENDPOINT_URL/sparql \
    --logfile output/logs/linker.log --loglevel $LOG_LEVEL

echo "Linking municipalities"
python src/linker.py municipalities input/old_municipalities.ttl output/_munics.ttl --endpoint $WARSA_ENDPOINT_URL/sparql \
    --arpa $ARPA_URL/pnr_municipality --logfile output/logs/linker.log --loglevel $LOG_LEVEL

rapper -i turtle output/_munics.ttl -o turtle > output/municipalities.ttl

echo "Linking persons"
cat output/_rank_links.ttl output/_occupation_links.ttl output/_unit_links.ttl output/_casualties_processed.ttl > output/_casualties_with_links.ttl
python src/linker.py persons output/_casualties_with_links.ttl output/_documents_links.ttl --endpoint $WARSA_ENDPOINT_URL/sparql \
    --munics output/municipalities.ttl --logfile output/logs/linker.log --loglevel $LOG_LEVEL

cat output/_documents_links.ttl output/_casualties_with_links.ttl | rapper - $BASE_URI -i turtle -o turtle > output/_casualties_linked.ttl

echo "Generating persons"
python src/person_generator.py output/_casualties_linked.ttl output/municipalities.ttl $WARSA_ENDPOINT_URL output/cas_person_ \
    --logfile output/logs/person_generator.log --loglevel $LOG_LEVEL

mv output/cas_person_documents_links.ttl output/_generated_documents_links.ttl

cat output/_generated_documents_links.ttl output/_casualties_linked.ttl | rapper - $BASE_URI -i turtle -o turtle > output/casualties.ttl

echo "Finished"

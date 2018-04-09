#!/bin/sh

mkdir -p output/logs

command -v rapper >/dev/null 2>&1 || { echo >&2 "rapper is not available, aborting"; exit 1; }

export WARSA_ENDPOINT_URL=${WARSA_ENDPOINT_URL:-http://localhost:3030/warsa}
export ARPA_URL=${ARPA_URL:-http://demo.seco.tkk.fi/arpa}

./convert.sh &&

python src/process.py output/surma.ttl data/kunnat.ttl output/surma_processed.ttl &&
cat data/surma_additions.ttl >> output/surma_processed.ttl &&

echo "Linking ranks" &&

python src/linker.py ranks output/surma_processed.ttl output/rank_links.ttl --endpoint $WARSA_ENDPOINT_URL/sparql &&

echo "Linking persons" &&

python src/sotasampo_helpers/arpa.py persons join output/surma_processed.ttl output/person_candidates_combined.ttl http://candidates/person $WARSA_ENDPOINT_URL/sparql --prop http://ldf.fi/schema/narc-menehtyneet1939-45/sukunimi -n -w 3 -r 3 &&
sed -i 's;narcs:sukunimi;<http://candidates/person>;' output/person_candidates_combined.ttl &&
cat output/surma_processed.ttl output/rank_links.ttl output/person_candidates_combined.ttl > output/person_full_combined.ttl &&

curl -f --data-urlencode "query=$(cat SPARQL/rank_labels.sparql)" $WARSA_ENDPOINT_URL/sparql -v > output/rank_labels.ttl &&
python src/sotasampo_helpers/arpa.py persons disambiguate SPARQL/arpa_menehtyneet_persons.sparql output/rank_labels.ttl output/person_full_combined.ttl output/person_linked.ttl http://www.cidoc-crm.org/cidoc-crm/P70_documents $WARSA_ENDPOINT_URL/sparql --prop http://candidates/person -n -w 3 -r 3 &&

cat output/surma_processed.ttl output/person_linked.ttl > output/surma_with_links.ttl &&

python src/tasks.py documents_links output/surma_with_links.ttl output/surma_wo_units.ttl --endpoint $WARSA_ENDPOINT_URL/sparql &&

echo "Linking units" &&

sed -r -i '/\s+.+:person\s+.+/d' output/surma_wo_units.ttl &&

python src/tasks.py link_units output/surma_wo_units.ttl output/surma_units.ttl --endpoint $WARSA_ENDPOINT_URL/sparql --arpa_unit $ARPA_URL/warsa_casualties_actor_units

# TODO: Combine created and existing schema

# TODO: Use Warsa URIs

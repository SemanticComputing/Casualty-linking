#!/usr/bin/env bash

mkdir -p output

command -v rapper >/dev/null 2>&1 || { echo >&2 "rapper is not available, aborting"; exit 1; }

python process.py output/surma.ttl data/munics.ttl output/surma_processed.ttl &&
cat data/surma_additions.ttl >> output/surma_processed.ttl &&

echo "Linking ranks" &&

python linker.py ranks output/surma_processed.ttl output/rank_links.ttl --endpoint "http://localhost:3030/warsa/sparql" &&

echo "Linking persons" &&

python sotasampo_helpers/arpa.py persons join output/surma_processed.ttl output/person_candidates_combined.ttl http://candidates/person http://localhost:3030/warsa/sparql --prop http://ldf.fi/schema/narc-menehtyneet1939-45/sukunimi -n -w 3 -r 3 &&
sed -i 's;narcs:sukunimi;<http://candidates/person>;' output/person_candidates_combined.ttl &&
cat output/surma_processed.ttl output/rank_links.ttl output/person_candidates_combined.ttl > output/person_full_combined.ttl &&
python sotasampo_helpers/arpa.py persons disambiguate SPARQL/arpa_menehtyneet_persons.sparql data/surma_ranks.ttl output/person_full_combined.ttl output/person_linked.ttl http://www.cidoc-crm.org/cidoc-crm/P70_documents http://localhost:3030/warsa/sparql --prop http://candidates/person -n -w 3 -r 3 &&

cat output/surma_processed.ttl output/person_linked.ttl > output/surma_with_links.ttl &&

python tasks.py documents_links output/surma_with_links.ttl output/surma_wo_units.ttl --endpoint http://localhost:3030/warsa/sparql &&

echo "Linking units" &&

sed -r -i '/\s+.+:person\s+.+/d' output/surma_wo_units.ttl &&

python tasks.py link_units output/surma_wo_units.ttl output/surma_units.ttl --endpoint http://localhost:3030/warsa/sparql

# TODO: Combine created and existing schema

# TODO: Use Warsa URIs

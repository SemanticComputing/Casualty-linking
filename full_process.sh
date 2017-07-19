#!/usr/bin/env bash

# python update_ranks.py  # This is obsolete as this is done in process.py

python process.py

python sotasampo_helpers/arpa.py persons candidates data/new/surma.ttl data/new/surma_person_candidates.ttl http://candidates/person http://demo.seco.tkk.fi/arpa/menehtyneet_persons -n -w 3 -r 10 --log_level DEBUG --prop http://ldf.fi/schema/narc-menehtyneet1939-45/sukunimi
python sotasampo_helpers/arpa.py persons join data/new/surma_person_candidates.ttl data/new/person_candidates_combined.ttl http://candidates/person http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3
cat data/new/surma.ttl data/new/person_candidates_combined.ttl > data/new/person_full_combined.ttl
python sotasampo_helpers/arpa.py persons disambiguate SPARQL/arpa_menehtyneet_persons.sparql data/surma_ranks.ttl data/new/person_full_combined.ttl data/new/person_linked.ttl  http://www.cidoc-crm.org/cidoc-crm/P70_documents http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3

cat data/new/surma.ttl data/new/person_linked.ttl > data/new/surma_with_links.ttl

python tasks.py documents_links data/new/surma_with_links.ttl data/new/surma_wo_units.ttl --endpoint http://localhost:3030/warsa/sparql

sed -r -i '/\s+ns1:person\s+.+/d' data/new/surma_wo_units.ttl

python tasks.py link_units data/new/surma_wo_units.ttl data/new/surma_units.ttl --endpoint http://localhost:3030/warsa/sparql

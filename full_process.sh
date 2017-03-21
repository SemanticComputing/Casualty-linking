#!/usr/bin/env bash

python update_ranks.py

python process.py

python sotasampo_helpers/arpa.py persons candidates data/new/surma.ttl data/new/surma_person_candidates.ttl http://candidates/person http://demo.seco.tkk.fi/arpa/menehtyneet_persons -n -w 3 -r 10 --log_level DEBUG --prop http://ldf.fi/schema/narc-menehtyneet1939-45/sukunimi
python sotasampo_helpers/arpa.py persons join data/new/surma_person_candidates.ttl data/new/person_candidates_combined.ttl http://candidates/person http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3
cat surma.ttl person_candidates_combined.ttl > person_full_combined.ttl
python sotasampo_helpers/arpa.py persons disambiguate SPARQL/arpa_menehtyneet_persons.sparql data/surma_ranks.ttl data/new/person_full_combined.ttl data/new/person_linked.ttl  http://www.cidoc-crm.org/cidoc-crm/P70_documents http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3

cd data/new

# cp person_linked.ttl person_linked_manual_fixes.ttl  # for manual adjusting if needed
# cat surma.ttl person_linked_manual_fixes.ttl surma_units.ttl > surma_with_links_2.ttl

cat surma.ttl person_linked.ttl > surma_with_links.ttl

python tasks.py documents_links surma_with_links.ttl surma_wo_units.ttl --endpoint http://localhost:3030/warsa/sparql

echo 'query=' | cat - unit_construct.sparql | sed 's/&/%26/g' | curl -d @- http://localhost:3030/warsa/sparql > surma_units.ttl

cat surma_wo_units.ttl surma_units.ttl > surma_final.ttl


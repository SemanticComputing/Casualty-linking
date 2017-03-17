#!/usr/bin/env bash

python update_ranks.py

python process.py

python sotasampo_helpers/arpa.py persons candidates data/new/surma.ttl data/new/surma_person_candidates.ttl http://candidates/person http://demo.seco.tkk.fi/arpa/menehtyneet_persons -n -w 3 -r 10 --log_level DEBUG --prop http://ldf.fi/schema/narc-menehtyneet1939-45/sukunimi
python sotasampo_helpers/arpa.py persons join data/new/surma_person_candidates.ttl data/new/person_candidates_combined.ttl http://candidates/person http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3
cat surma.ttl person_candidates_combined.ttl > person_full_combined.ttl
python sotasampo_helpers/arpa.py persons disambiguate SPARQL/arpa_menehtyneet_persons.sparql data/surma_ranks.ttl data/new/person_full_combined.ttl data/new/person_linked.ttl  http://www.cidoc-crm.org/cidoc-crm/P70_documents http://localhost:3030/warsa/sparql  --prop http://candidates/person -n -w 3 -r 3

python sotasampo_helpers/arpa.py units candidates data/new/surma.ttl data/new/surma_unit_candidates.ttl http://candidates/unit http://demo.seco.tkk.fi/arpa/menehtyneet_units  --prop http://ldf.fi/schema/narc-menehtyneet1939-45/joukko_osasto -n -w 3 -r 3
python sotasampo_helpers/arpa.py units join data/new/surma_unit_candidates.ttl data/new/unit_candidates_combined.ttl http://candidates/unit http://localhost:3030/warsa/sparql  --prop http://candidates/unit -n -w 3 -r 3
python sotasampo_helpers/arpa.py units disambiguate SPARQL/arpa_menehtyneet_units.sparql data/surma_ranks.ttl data/new/unit_full_combined.ttl data/new/surma_units.ttl  http://ldf.fi/schema/narc-menehtyneet1939-45/osasto http://localhost:3030/warsa/sparql  --prop http://candidates/unit -n -w 3 -r 3

cd data/new

# cp person_linked.ttl person_linked_manual_fixes.ttl  # for manual adjusting if needed
# cat surma.ttl person_linked_manual_fixes.ttl surma_units.ttl > surma_with_links_2.ttl

cat surma.ttl person_linked.ttl surma_units.ttl > surma_with_links.ttl

python tasks.py documents_links data/new/surma_with_links.ttl data/new/surma_final.ttl --endpoint http://localhost:3030/warsa/sparql
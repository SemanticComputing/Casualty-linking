Code for converting
"Casualties during the Finnish wars 1939–1945" (Suomen sodissa 1939–1945 menehtyneet) into RDF, and integrating it to the WarSampo domain ontologies.

## Conversion

Requires Docker and Docker Compose. 

Create directories `./data/` and `./output/`.
The initial files (`casualties.xlsx`, `cemeteries.ttl`) should be placed in `./data/`. 
The `cemeteries.ttl` should refer to the current cemeteries domain ontology. 

Build the conversion pipeline and start conversion:

`docker-compose up -d --build`

Follow the logs to see what is happening:

`docker-compose logs -f tasks`

To convert only the 50 top rows of the CSV:

`docker-compose up --build -d && docker-compose run --rm tasks ./process.sh 50`

The output files will be written to `./output/`, and logs to `./output/logs/`.

## Tests

Nose can be used to run both normal tests (src/tests.py) and doctests in the data conversion environment.

`docker-compose up --build -d && docker-compose run --rm tasks nosetests --with-doctest`

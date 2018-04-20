Code for converting 
"Casualties during the Finnish wars 1939–1945" (Suomen sodissa 1939–1945 menehtyneet) to RDF.

Validates, improves and automatically links to other WarSampo datasets.

## Conversion

Requires Docker and Docker Compose.

Create directories `./data/` and `./output/`.
The initial Excel files (`prisoners.xls`, `hospitals.xlsx`, and `camps.xlsx`) should be placed in `./data/`.

Build the conversion pipeline:

`docker-compose build`

Start the required services:

`docker-compose up -d las arpa fuseki`

Run the conversion process:

`docker-compose run --rm tasks`

The output files will be written to `./output/`, and logs to `./output/logs/`.

## Tests

`docker-compose run --rm tasks python -m doctest -v src/linker.py`
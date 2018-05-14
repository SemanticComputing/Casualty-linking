Code for converting
"Casualties during the Finnish wars 1939–1945" (Suomen sodissa 1939–1945 menehtyneet) to RDF, and integrating it to WarSampo domain ontologies.

## Conversion

Requires Docker and Docker Compose.

Create directories `./data/` and `./output/`.
The initial files (`casualties.xlsx`, `municipalities.ttl`) should be placed in `./data/`.

Build the conversion pipeline and start conversion:

`docker-compose up -d --build`

Follow the logs to see what is happening:

`docker-compose logs -f tasks`

The output files will be written to `./output/`, and logs to `./output/logs/`.

## Tests

`docker-compose run --rm tasks python -m doctest -v src/linker.py`

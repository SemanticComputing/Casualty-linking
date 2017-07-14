"""
Stand-alone tasks for casualties dataset
"""

import argparse
import logging
from io import StringIO
from time import sleep

from rdflib import *
from SPARQLWrapper import SPARQLWrapper, JSON
from arpa_linker.arpa import Arpa, ArpaMimic, process_graph, arpafy
from warsa_linkers.units import get_query_template, preprocessor, Validator

ns_skos = Namespace('http://www.w3.org/2004/02/skos/core#')
ns_dct = Namespace('http://purl.org/dc/terms/')
ns_schema = Namespace('http://ldf.fi/schema/narc-menehtyneet1939-45/')
ns_crm = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
ns_foaf = Namespace('http://xmlns.com/foaf/0.1/')
ns_owl = Namespace('http://www.w3.org/2002/07/owl#')

ns_hautausmaat = Namespace('http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/')
ns_kansalaisuus = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/')
ns_kansallisuus = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/')
ns_kunnat = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kunnat/')
ns_sotilasarvo = Namespace('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/')
ns_menehtymisluokka = Namespace('http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/')


argparser = argparse.ArgumentParser(description="Stand-alone tasks for casualties dataset", fromfile_prefix_chars='@')

argparser.add_argument("task", help="Which task to run", choices=["documents_links", "link_units", "test"])
argparser.add_argument("input", help="Input RDF data file")
argparser.add_argument("output", help="Output RDF data file")

argparser.add_argument("--endpoint", default='http://ldf.fi/warsa/sparql', type=str, help="SPARQL endpoint")
argparser.add_argument("--format", default='turtle', type=str, help="Format of RDF files [default: turtle]")
argparser.add_argument("--loglevel", default='INFO',
                       choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       help="Logging level, default is INFO.")

args = argparser.parse_args()

logging.basicConfig(filename='tasks.log', filemode='a', level=getattr(logging, args.loglevel.upper()),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)
log.info('Starting to run tasks with arguments: {args}'.format(args=args))


def _query_sparql(sparql_obj):
    """
    Query SPARQL with retry functionality

    :type sparql_obj: SPARQLWrapper
    :return: SPARQL query results
    """
    results = None
    retry = 0
    while not results:
        try:
            results = sparql_obj.query().convert()
        except ValueError:
            if retry < 50:
                log.error('Malformed result for query {p_uri}, retrying in 10 seconds...'.format(
                    p_uri=sparql_obj.queryString))
                retry += 1
                sleep(10)
            else:
                raise
    log.debug('Got results {res} for query {q}'.format(res=results, q=sparql_obj.queryString))
    return results


def documents_links(data_graph, endpoint):
    """
    Create crm:P70_documents links between death records and person instances.
    """
    sparql = SPARQLWrapper(endpoint)
    persons = list(data_graph[:RDF.type:ns_schema.DeathRecord])
    log.debug('Finding links for {len} death records'.format(len=len(persons)))

    for person in persons:
        if len(list(data_graph[person:ns_crm.documents:])):
            log.debug('Skipping already linked death record {uri}'.format(uri=person))
            continue

        sparql.setQuery("""
                        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
                        SELECT * WHERE {{ ?sub crm:P70i_is_documented_in <{person_uri}> . }}
                        """.format(person_uri=person))
        sparql.setReturnFormat(JSON)

        results = _query_sparql(sparql)

        warsa_person = None
        for result in results["results"]["bindings"]:
            warsa_person = result["sub"]["value"]
            log.info('{pers} matches person instance {warsa_pers}'.format(pers=person, warsa_pers=warsa_person))
            data_graph.add((person, ns_crm.P70_documents, URIRef(warsa_person)))

        if not warsa_person:
            log.warning('{person} didn\'t match any person instance.'.format(person=person))


def link_units(graph: Graph, endpoint: str):
    """
    :param graph: Data graph object
    :param endpoint: SPARQL endpoint
    :return: Graph with links
    """
    sparql = SPARQLWrapper(endpoint)
    query_template = """
                    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
                    SELECT * WHERE {{ ?sub crm:P70i_is_documented_in <{person_uri}> . }}
                    """
    temp_graph = Graph()

    ngram_arpa = Arpa('http://demo.seco.tkk.fi/arpa/warsa_actor_units')

    for person in graph[:RDF.type:ns_crm.E31_Document]:
        sparql.setQuery(query_template.format(person_uri=person))
        sparql.setReturnFormat(JSON)
        results = _query_sparql(sparql)

        warsa_unit = None
        for result in results["results"]["bindings"]:
            warsa_unit = result["sub"]["value"]
            log.info('Found unit {unit} for {pers} by cover number.'.format(pers=person, unit=warsa_unit))
            graph.add((person, ns_schema.osasto, URIRef(warsa_unit)))

        if not warsa_unit:
            # Add related_period for linking with warsa-linkers
            death_time = str(graph.value(person, ns_schema.kuolinaika))
            if death_time < '1941-06-25':
                temp_graph.add((person, URIRef('http://ldf.fi/schema/warsa/events/related_period'), URIRef('http://ldf.fi/warsa/conflicts/WinterWar')))

            ngrams = ngram_arpa.get_candidates(str(graph.value(person, ns_schema.joukko_osasto)))
            for ngram in ngrams:
                temp_graph.add((person, ns_schema.candidate, ngram))

    arpa = ArpaMimic(get_query_template(), endpoint)
    new_graph = process_graph(temp_graph, arpa, preprocessor=preprocessor, validator_class=Validator, log_level='INFO', new_graph=True)
    return new_graph + graph


def load_input_file(filename):
    """
    >>> load_input_file(StringIO('<http://example.com/res> a <http://example.com/class> .'))  #doctest: +ELLIPSIS
    <Graph identifier=...(<class 'rdflib.graph.Graph'>)>
    """
    return Graph().parse(filename, format=args.format)


if __name__ == '__main__':
    if args.task == 'documents_links':
        log.info('Loading input file...')
        death_records = load_input_file(args.input)
        log.info('Creating links...')
        documents_links(death_records, args.endpoint)
        log.info('Serializing output file...')
        death_records.serialize(format=args.format, destination=args.output)

    elif args.task == 'link_units':
        log.info('Loading input file...')
        death_records = load_input_file(args.input)
        log.info('Creating links...')
        death_records = link_units(death_records, args.endpoint)
        log.info('Serializing output file...')
        death_records.serialize(format=args.format, destination=args.output)

    elif args.task == 'test':
        print('Running doctests')
        import doctest

        res = doctest.testmod()
        if not res[0]:
            print('Doctests OK!')
        exit()

    log.info('All done.')

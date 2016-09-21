"""
Stand-alone tasks for casualties dataset
"""

import argparse
import logging
from io import StringIO
from time import sleep

from rdflib import *
from SPARQLWrapper import SPARQLWrapper, JSON

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

argparser.add_argument("task", help="Which task to run", choices=["documents_links", "test"])
argparser.add_argument("input", help="Input RDF data file")
argparser.add_argument("output", help="Output RDF data file")

argparser.add_argument("--format", default='turtle', type=str, help="Format of RDF files [default: turtle]")

args = argparser.parse_args()

logging.basicConfig(filename='tasks.log', filemode='a', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)
log.info('Starting to run tasks with arguments: {args}'.format(args=args))


def _query_sparql(sparql_obj):
    """
    Query SPARQL with retry functionality

    :param sparql_obj: SPARQLWrapper object
    :return: result
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

def documents_links(data_graph):
    """
    Create crm:P70_documents links between death records and person instances.
    """
    sparql = SPARQLWrapper('http://ldf.fi/warsa/sparql')
    for person in list(data_graph[:RDF.type:ns_crm.E31_Document]):
        if data_graph[person:ns_crm.documents:]:
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
            log.debug('{pers} matches person instance {warsa_pers}'.format(pers=person, warsa_pers=warsa_person))
            data_graph.add((person, ns_crm.P70_documents, URIRef(warsa_person)))

        if not warsa_person:
            log.warning('{person} didn\'t match any person instance.'.format(person=person))


def load_death_records(filename):
    """
    >>> load_death_records(StringIO('<http://example.com/res> a <http://example.com/class> .'))  #doctest: +ELLIPSIS
    <Graph identifier=...(<class 'rdflib.graph.Graph'>)>
    """
    return Graph().parse(filename, format=args.format)

if args.task == 'documents_links':
    death_records = load_death_records(args.input)
    documents_links(death_records)
    death_records.serialize(format=args.format, destination=args.output)

elif args.task == 'test':
    print('Running doctests')
    import doctest

    res = doctest.testmod()
    if not res[0]:
        print('OK!')
    exit()

log.info('All done.')
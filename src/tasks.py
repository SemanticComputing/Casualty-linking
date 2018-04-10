"""
Stand-alone tasks for casualties dataset
"""

import argparse
import logging
from io import StringIO
from time import sleep

from fuzzywuzzy import fuzz
from rdflib import *
from SPARQLWrapper import SPARQLWrapper, JSON
from arpa_linker.arpa import Arpa, ArpaMimic, process_graph, arpafy, combine_values, log_to_file

from linker import _query_sparql
from namespaces import WARSA_NS, CRM, SCHEMA_NS

log = logging.getLogger(__name__)


def documents_links(data_graph, endpoint):
    """
    Create crm:P70_documents links between death records and person instances.
    """
    sparql = SPARQLWrapper(endpoint)
    persons = list(data_graph[:RDF.type:WARSA_NS.DeathRecord])
    log.debug('Finding links for {len} death records'.format(len=len(persons)))
    links = Graph()

    for person in persons:
        if len(list(data_graph[person:CRM.documents:])):
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
            links.add((person, CRM.P70_documents, URIRef(warsa_person)))

        if not warsa_person:
            log.warning('{person} didn\'t match any person instance.'.format(person=person))

    return links


def load_input_file(filename):
    """
    >>> load_input_file(StringIO('<http://example.com/res> a <http://example.com/class> .'))  #doctest: +ELLIPSIS
    <Graph identifier=...(<class 'rdflib.graph.Graph'>)>
    """
    return Graph().parse(filename, format=args.format)


# TODO: Serialize only new information to allow base information to change without having to do linking again

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Stand-alone tasks for casualties dataset",
                                        fromfile_prefix_chars='@')

    argparser.add_argument("task", help="Which task to run", choices=["documents_links", "link_units", "test"])
    argparser.add_argument("input", help="Input RDF data file")
    argparser.add_argument("output", help="Output RDF data file")

    argparser.add_argument("--endpoint", default='http://ldf.fi/warsa/sparql', type=str, help="SPARQL endpoint")
    argparser.add_argument("--arpa_unit", default='http://demo.seco.tkk.fi/arpa/warsa_casualties_actor_units', type=str,
                           help="ARPA instance URL for unit linking")
    argparser.add_argument("--format", default='turtle', type=str, help="Format of RDF files [default: turtle]")
    argparser.add_argument("--loglevel", default='INFO',
                           choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                           help="Logging level, default is INFO.")
    argparser.add_argument("--logfile", default='tasks.log', help="Logfile")

    args = argparser.parse_args()

    logging.basicConfig(filename=args.logfile, filemode='a', level=getattr(logging, args.loglevel.upper()),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log.info('Starting to run tasks with arguments: {args}'.format(args=args))

    if args.task == 'documents_links':
        log.info('Loading input file...')
        death_records = load_input_file(args.input)
        log.info('Creating links...')
        death_records = documents_links(death_records, args.endpoint)
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

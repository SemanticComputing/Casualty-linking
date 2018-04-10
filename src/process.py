#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing, validating and fixing some errors in the original RDF version of
"Casualties during the Finnish wars 1939–1945" (Suomen sodissa 1939–1945 menehtyneet) RDF dataset.

Validates, improves and automatically links to other WarSampo datasets.

Copyright (c) 2017 Mikko Koho
"""

import argparse
import logging
import re

from rdflib import *
import rdflib

from namespaces import WARSA_NS, KUNNAT, CEMETERY_NS, MENEHTYMISLUOKKA, SCHEMA_NS, SKOS

URI_MAPPINGS = {
    # MANUAL FIXES TO SOME URI'S USED AS TRIPLE OBJECTS
    Literal('Alipuseeri'): Literal('Aliupseeri'),
    Literal('Alikers'): Literal('Alikersantti'),
    CEMETERY_NS.x: CEMETERY_NS.hx_0,
    KUNNAT.kx: KUNNAT.k,
    MENEHTYMISLUOKKA: MENEHTYMISLUOKKA.Tuntematon,
}


def fix_by_direct_uri_mappings(graph: Graph):
    """
    Fix faulty uri references by direct mapping
    """
    for map_from, map_to in URI_MAPPINGS.items():
        for s, p in list(graph[::map_from]):
            graph.remove((s, p, map_from))
            graph.add((s, p, map_to))

        log.info('Applied mapping %s  -->  %s' % (map_from, map_to))

    return graph


# def validate(surma, surma_onto):
#     """
#     Do some validation to find remaining errors
#     """
#
#     log.info('Combining data and ontology graphs')
#     full_rdf = surma + surma_onto
#     log.info('Starting validation')
#
#     unknown_links = r.get_unknown_links(full_rdf)
#
#     log.warning('Found {num} unknown URI references'.format(num=len(unknown_links)))
#     for o in sorted(unknown_links):
#         if not str(o).startswith('http://ldf.fi/warsa/'):
#             log.warning('Unknown URI references: {uri}  (referenced {num} times)'
#                         .format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))
#         else:
#             log.debug('Unknown URI references: {uri}  (referenced {num} times)'
#                       .format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))
#
#     unlinked_subjects = [uri for uri in r.get_unlinked_uris(full_rdf)
#                          if not (str(uri).startswith('http://ldf.fi/narc-menehtyneet1939-45/p')
#                                  or str(uri).startswith('http://ldf.fi/warsa/'))
#                          ]
#
#     if len(unlinked_subjects):
#         log.warning('Found {num} unlinked subjects'.format(num=len(unlinked_subjects)))
#     for s in sorted(unlinked_subjects):
#         log.warning('Unlinked subject {uri}'.format(uri=str(s)))
#
# TODO: Do the above as a SPARQL query

def harmonize_names(surma: Graph):
    """
    Link death records to WARSA persons, unify and stylize name representations, fix some errors.
    """
    # Unify previous last names to same format as WARSA actors: LASTNAME (ent PREVIOUS)
    for lbl_pred in [SCHEMA_NS.family_name, SKOS.prefLabel]:
        for (person, lname) in list(surma[:lbl_pred:]):
            new_name = re.sub(r'(\w)0(\w)', r'\1O\2', lname)
            new_name = re.sub('%', '/', new_name)
            new_lname = Literal(re.sub(r'(\w\w +)(E(?:NT)?\.)\s?(\w+)', r'\1(ent. \3)', str(new_name)))
            if new_lname and new_lname != lname:
                log.debug('Unifying lastname {ln} to {nln}'.format(ln=lname, nln=new_lname))
                surma.add((person, lbl_pred, new_lname))
                surma.remove((person, lbl_pred, lname))

    # Change names from all uppercase to capitalized
    for lbl_pred in [SCHEMA_NS.given_names, SCHEMA_NS.family_name, SKOS.prefLabel]:
        for (sub, obj) in list(surma[:lbl_pred:]):
            name = str(obj)
            new_name = str.title(name)
            if name != new_name:
                surma.remove((sub, lbl_pred, obj))
                surma.add((sub, lbl_pred, Literal(new_name)))
                log.debug('Changed name {orig} to {new}'.format(orig=name, new=new_name))

    return surma


#######
# MAIN


def main(args):

    surma = rdflib.Graph()

    ##################
    # READ IN RDF DATA

    # Read RDF graph from TTL files
    print('Processing death records...')

    surma.parse(args.input, format='turtle')

    print('Parsed {len} data triples.'.format(len=len(surma)))
    print('Writing graphs to pickle objects...')

    #####################################
    # FIX KNOWN ISSUES IN DATA AND SCHEMA

    print('Applying direct URI mapping fixes...')
    surma = fix_by_direct_uri_mappings(surma)

    print('Handling persons...')

    surma = harmonize_names(surma)

    ##################
    # SERIALIZE GRAPHS

    # TODO: Fix this
    # print('Validating graphs for unknown link targets and unlinked subjects...')
    # validate(surma, Graph())

    print('Serializing graphs...')
    surma.bind("crm", "http://www.cidoc-crm.org/cidoc-crm/")
    surma.bind("skos", "http://www.w3.org/2004/02/skos/core#")
    surma.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
    surma.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")
    surma.bind("wsc", WARSA_NS)

    surma.serialize(format="turtle", destination=args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Casualties of war')
    parser.add_argument("input", help="Input turtle file")
    parser.add_argument("output", help="Output data file")
    parser.add_argument("--endpoint", default='http://ldf.fi/warsa/sparql', type=str, help="WarSampo SPARQL endpoint")
    parser.add_argument("--arpa_pnr", default='http://demo.seco.tkk.fi/arpa/pnr_municipality', type=str,
                           help="ARPA instance URL PNR linking")
    parser.add_argument("--loglevel", default='INFO',
                        choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level, default is INFO.")

    args = parser.parse_args()

    logging.basicConfig(filename='output/logs/casualties.log',
                        filemode='a',
                        level=getattr(logging, args.loglevel.upper()),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log = logging.getLogger(__name__)

    main(args)

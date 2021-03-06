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

from namespaces import SCHEMA_WARSA, MUNICIPALITIES, CEMETERIES, SCHEMA_CAS, SKOS, PERISHING_CLASSES, GENDERS, \
    CITIZENSHIPS, NATIONALITIES, MOTHER_TONGUES, MARITAL_STATUSES, bind_namespaces

URI_MAPPINGS = {
    # MANUAL FIXES TO SOME URI'S USED AS TRIPLE OBJECTS
    Literal('Alipuseeri'): Literal('Aliupseeri'),
    Literal('Alikers'): Literal('Alikersantti'),
    CEMETERIES.x: CEMETERIES.hx_0,
    MUNICIPALITIES.kx: MUNICIPALITIES.k,
    PERISHING_CLASSES: PERISHING_CLASSES.Tuntematon,
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

def unify_names(casualties: Graph):
    """
    Unify and stylize name representations
    """
    def unify_family_name(family: str):
        new_fam = re.sub(r'(\w)0(\w)', r'\1O\2', family)
        new_fam = re.sub(r'\s+', ' ', new_fam)
        new_fam = re.sub('%', '/', new_fam)  # Väinö Jaakkola%Jakkola
        new_fam = re.sub(r'(\w\w\s+)(E(?:NT)?\.)\s?(\w+)', r'\1(ent. \3)', new_fam)
        new_fam = new_fam.title().replace('(Ent.', '(ent.').replace('Von', 'von')
        log.debug('Unifying family name "{ln}" to "{nln}"'.format(ln=family, nln=new_fam))
        return new_fam

    def unify_given_name(given: str):
        new_giv = str(given).title()
        new_giv = re.sub('%', '/', new_giv)
        log.debug('Unifying given names "{orig}" to "{new}"'.format(orig=given, new=new_giv))
        return new_giv

    # Unify previous last names to same format as WARSA actors: LASTNAME (ent PREVIOUS)
    for (person, lname) in list(casualties[:SCHEMA_WARSA.family_name:]):
        new_fam_lit = Literal(unify_family_name(lname))
        casualties.remove((person, SCHEMA_WARSA.family_name, lname))
        casualties.add((person, SCHEMA_WARSA.family_name, new_fam_lit))

        given = casualties.value(person, SCHEMA_WARSA.given_names)
        new_giv_lit = Literal(unify_given_name(given))
        casualties.remove((person, SCHEMA_WARSA.given_names, given))
        casualties.add((person, SCHEMA_WARSA.given_names, new_giv_lit))

        full_name = '{family}, {given}'.format(family=new_fam_lit, given=new_giv_lit)
        casualties.add((person, SKOS.prefLabel, Literal(full_name)))

    return casualties


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

    #####################################
    # FIX KNOWN ISSUES IN DATA AND SCHEMA

    print('Applying direct URI mapping fixes...')
    surma = fix_by_direct_uri_mappings(surma)

    print('Handling persons...')

    surma = unify_names(surma)

    # print('Validating graphs for unknown link targets and unlinked subjects...')
    # validate(surma, Graph())

    print('Serializing graphs...')
    bind_namespaces(surma).serialize(format="turtle", destination=args.output)


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

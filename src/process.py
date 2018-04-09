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
from time import sleep
from urllib.error import HTTPError

from arpa_linker.arpa import Arpa
import iso8601
from rdflib import *
import rdflib
import rdf_dm as r

from namespaces import WARSA_NS
from sotasampo_helpers.arpa import link_to_pnr

SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')
DCT = Namespace('http://purl.org/dc/terms/')
NARCS = Namespace('http://ldf.fi/schema/narc-menehtyneet1939-45/')
CRM = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
FOAF = Namespace('http://xmlns.com/foaf/0.1/')
OWL = Namespace('http://www.w3.org/2002/07/owl#')

HAUTAUSMAAT = Namespace('http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/')
KANSALAISUUS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/')
KANSALLISUUS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/')
KUNNAT = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kunnat/')
SOTILASARVO = Namespace('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/')
MENEHTYMISLUOKKA = Namespace('http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/')

URI_MAPPINGS = {
    # MANUAL FIXES TO SOME URI'S USED AS TRIPLE OBJECTS
    SOTILASARVO.Alipuseeri: SOTILASARVO.Aliupseeri,
    SOTILASARVO.Alikers__: SOTILASARVO.Alikersantti,
    SOTILASARVO.Jaeaek_: SOTILASARVO.Jaeaekaeri,
    SOTILASARVO.Miehisto: SOTILASARVO.Miehistoe,
    SOTILASARVO.Stm__________: SOTILASARVO.Sotamies,
    SOTILASARVO._stm_: SOTILASARVO.Sotamies,
    SOTILASARVO.Matr__: SOTILASARVO.Matruusi,
    SOTILASARVO.Muu: SOTILASARVO.Muu_arvo,
    KANSALAISUUS.Fi_: KANSALAISUUS.Suomi,
    KANSALLISUUS.Fi_: KANSALLISUUS.Suomi,
    HAUTAUSMAAT.x___: HAUTAUSMAAT.hx_0,
    KUNNAT.kx: KUNNAT.k,
    MENEHTYMISLUOKKA._: MENEHTYMISLUOKKA.Tuntematon,
}


def fix_by_direct_uri_mappings(graph: Graph):
    """
    Fix faulty uri references by direct mapping
    """
    for map_from, map_to in URI_MAPPINGS.items():
        for s, p in list(graph[::map_from]):
            graph.remove((s, p, map_from))
            graph.add((s, p, map_to))

        # for s, p in list(surma_onto[::map_from]):
        #     surma_onto.remove((s, p, map_from))
        #     surma_onto.add((s, p, map_to))

        log.info('Applied mapping %s  -->  %s' % (map_from, map_to))

    return graph


def link_to_municipalities(surma: Graph, schema: Graph):
    """
    Link to Warsa municipalities.
    """
    munics = r.helpers.read_graph_from_sparql("http://ldf.fi/warsa/sparql",
                                              graph_name='http://ldf.fi/warsa/places/municipalities')

    log.info('Using Warsa municipalities with {n} triples'.format(n=len(munics)))

    pnr_arpa = Arpa('http://demo.seco.tkk.fi/arpa/pnr_municipality')
    pnr_links = link_to_pnr(schema, NARCS.pnr_link, None, pnr_arpa)

    for kunta in list(schema[:RDF.type:NARCS.Kunta]):
        label = next(schema[kunta:SKOS.prefLabel:])

        warsa_matches = []

        labels = str(label).strip().split('/')
        for lbl in labels:
            if lbl == 'Pyhäjärvi Ol':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_75')]
            elif lbl == 'Pyhäjärvi Ul.':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_543')]
            elif lbl == 'Pyhäjärvi Vl':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_586')]
            elif lbl == 'Koski Tl.':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_291')]
            elif lbl == 'Koski Hl.':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_391')]
            elif lbl == 'Uusikirkko Vl':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_609')]
            elif lbl == 'Oulun mlk':
                warsa_matches = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_65')]

            if not warsa_matches:
                warsa_matches = list(munics[:SKOS.prefLabel:Literal(lbl)])
            if not warsa_matches:
                warsa_matches = list(munics[:SKOS.prefLabel:Literal(lbl.replace(' kunta', ' mlk'))])

        if not warsa_matches:
            pass
            # TODO: Get pnr links and add them to warsa_matches

        #     for lbl in labels:
        #         retry = 0
        #         while not warsa_matches:
        #             try:
        #                 warsa_matches = pnr_arpa.get_uri_matches(lbl)['results']  # Link to PNR
        #                 break
        #             except (HTTPError, ValueError):
        #                 if retry < 50:
        #                     log.error('Error getting "PNR" matches from ARPA, waiting 10 seconds before retrying...')
        #                     retry += 1
        #                     sleep(10)
        #                 else:
        #                     raise

        if len(warsa_matches) == 0:
            if set(surma.subjects(None, kunta)):
                log.warning("Couldn't find URIs for municipality {lbl}".format(lbl=label))
        elif len(warsa_matches) == 1:
            match = warsa_matches[0]
            log.info('Found {lbl} municipality URI {s}'.format(lbl=label, s=match))

            for prop in [WARSA_NS.birth_place,
                         WARSA_NS.home_place,
                         WARSA_NS.residence_place,
                         WARSA_NS.municipality_of_death,
                         WARSA_NS.wounding_municipality,
                         WARSA_NS.municipality_gone_missing]:
                for subj in list(surma[:prop:kunta]):
                    surma.add((subj, prop, match))
                    surma.add((subj, URIRef('{prop}_label'.format(prop=str(prop))), label))
                    surma.remove((subj, prop, kunta))

        else:
            log.warning('Found multiple URIs for municipality {lbl}: {s}'.format(lbl=label, s=warsa_matches))

        # TODO: Link burial municipalities to pnr (also to warsa if no pnr hit)

    return surma


def validate(surma, surma_onto):
    """
    Do some validation to find remaining errors
    """

    log.info('Combining data and ontology graphs')
    full_rdf = surma + surma_onto
    log.info('Starting validation')

    unknown_links = r.get_unknown_links(full_rdf)

    log.warning('Found {num} unknown URI references'.format(num=len(unknown_links)))
    for o in sorted(unknown_links):
        if not str(o).startswith('http://ldf.fi/warsa/'):
            log.warning('Unknown URI references: {uri}  (referenced {num} times)'
                        .format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))
        else:
            log.debug('Unknown URI references: {uri}  (referenced {num} times)'
                      .format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))

    unlinked_subjects = [uri for uri in r.get_unlinked_uris(full_rdf)
                         if not (str(uri).startswith('http://ldf.fi/narc-menehtyneet1939-45/p')
                                 or str(uri).startswith('http://ldf.fi/warsa/'))
                         ]

    if len(unlinked_subjects):
        log.warning('Found {num} unlinked subjects'.format(num=len(unlinked_subjects)))
    for s in sorted(unlinked_subjects):
        log.warning('Unlinked subject {uri}'.format(uri=str(s)))


def harmonize_names(surma: Graph):
    """
    Link death records to WARSA persons, unify and stylize name representations, fix some errors.
    """
    # Unify previous last names to same format as WARSA actors: LASTNAME (ent PREVIOUS)
    for lbl_pred in [NARCS.sukunimi, SKOS.prefLabel]:
        for (person, lname) in list(surma[:lbl_pred:]):
            new_name = re.sub(r'(\w)0(\w)', r'\1O\2', lname)
            new_name = re.sub('%', '/', new_name)
            new_lname = Literal(re.sub(r'(\w\w +)(E(?:NT)?\.)\s?(\w+)', r'\1(ent. \3)', str(new_name)))
            if new_lname and new_lname != lname:
                log.debug('Unifying lastname {ln} to {nln}'.format(ln=lname, nln=new_lname))
                surma.add((person, lbl_pred, new_lname))
                surma.remove((person, lbl_pred, lname))

    # Change names from all uppercase to capitalized
    for lbl_pred in [NARCS.etunimet, NARCS.sukunimi, SKOS.prefLabel]:
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
    municipalities = rdflib.Graph()

    ##################
    # READ IN RDF DATA

    # Read RDF graph from TTL files
    print('Processing Sotasurma RDF files...')

    surma.parse(args.input, format='turtle')
    municipalities.parse(args.munics, format='turtle')

    print('Parsed {len} data triples.'.format(len=len(surma)))
    print('Writing graphs to pickle objects...')

    #####################################
    # FIX KNOWN ISSUES IN DATA AND SCHEMA

    print('Applying direct URI mapping fixes...')
    surma = fix_by_direct_uri_mappings(surma)

    ############################################
    # LINK TO OTHER SOTASAMPO DATASETS AND STUFF

    print('Linking to municipalities...')

    surma = link_to_municipalities(surma, municipalities)

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
    surma.bind("narc-kieli", "http://ldf.fi/narc-menehtyneet1939-45/aeidinkieli/")
    surma.bind("narc-kansalaisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/")
    surma.bind("narc-kansallisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/")
    surma.bind("narc-menehtymisluokka", "http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/")
    surma.bind("narc-siviilisaeaety", "http://ldf.fi/narc-menehtyneet1939-45/siviilisaeaety/")
    surma.bind("narc-kunta", "http://ldf.fi/narc-menehtyneet1939-45/kunnat/")
    surma.bind("narc-sukupuoli", "http://ldf.fi/narc-menehtyneet1939-45/sukupuoli/")

    surma.bind("warsa-kunta", "http://ldf.fi/warsa/places/municipalities/")
    surma.bind("warsa-toimija", "http://ldf.fi/warsa/actors/")

    surma.serialize(format="turtle", destination=args.output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Casualties of war')
    parser.add_argument("input", help="Input turtle file")
    parser.add_argument("munics", help="Municipalities turtle file")
    parser.add_argument("output", help="Output turtle file")
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

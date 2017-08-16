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
import os
import re
from time import sleep
from urllib.error import HTTPError

from arpa_linker.arpa import Arpa
import iso8601
import joblib
from rdflib import *
import rdflib
import rdf_dm as r
from sotasampo_helpers.arpa import link_to_pnr

INPUT_FILE_DIRECTORY = 'data/'
OUTPUT_FILE_DIRECTORY = 'data/new/'

DATA_FILE = 'surma.ttl'

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

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-reload', action='store_true', help='Reload RDF graphs, instead of using pickle object')
parser.add_argument('-generated', action='store_true', help='Use previously generated new data files as input')
parser.add_argument('-generated_pkl', action='store_true', help='Use previously generated new data pickles')
parser.add_argument('-skip_validation', action='store_true', help='Skip validation')
parser.add_argument('-skip_ranks', action='store_true', help='Skip linking to Warsa military ranks')
parser.add_argument('-skip_municipalities', action='store_true', help='Skip linking to municipalities')
parser.add_argument('-d', action='store_true', help='Dry run, don\'t serialize created graphs')
parser.add_argument("--loglevel", default='INFO',
                    choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    help="Logging level, default is INFO.")

args = parser.parse_args()

reload = args.reload
USE_GENERATED_FILES = args.generated
USE_GENERATED_PKL = args.generated_pkl
DRYRUN = args.d
SKIP_VALIDATION = args.skip_validation
SKIP_MUNICIPALITIES = args.skip_municipalities
SKIP_RANKS = args.skip_ranks

surma = rdflib.Graph()
surma_onto = rdflib.Graph()
old_ranks = rdflib.Graph()

logging.basicConfig(filename='Sotasurma.log',
                    filemode='a',
                    level=getattr(logging, args.loglevel.upper()),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)


def fix_by_direct_uri_mappings():
    """
    Fix faulty uri references by direct mapping
    """
    for map_from, map_to in URI_MAPPINGS.items():
        for s, p in list(surma[::map_from]):
            surma.remove((s, p, map_from))
            surma.add((s, p, map_to))

        for s, p in list(surma_onto[::map_from]):
            surma_onto.remove((s, p, map_from))
            surma_onto.add((s, p, map_to))

        log.info('Applied mapping %s  -->  %s' % (map_from, map_to))


def fix_cemetery_links():
    """
    Fix errors in cemetery links.
    """
    p = NARCS.hautausmaa

    for s, o in list(surma[:p:]):
        assert str(o).startswith(HAUTAUSMAAT)

        new_o = URIRef(re.sub(r'(/hautausmaat/)(\d+)', r'\1h\2', str(o)))
        if not list(surma_onto[new_o::]):
            # Burial place is not in the cemeteries list, so just skip it as it is unknown
            new_o = None

        surma.remove((s, p, o))
        if new_o:
            surma.add((s, p, new_o))


def link_to_municipalities():
    """
    Link to Warsa municipalities used in SotaSampo project.
    """
    munics = r.helpers.read_graph_from_sparql("http://ldf.fi/warsa/sparql",
                                              graph_name='http://ldf.fi/warsa/places/municipalities')

    log.info('Using Warsa municipalities with {n} triples'.format(n=len(munics)))

    pnr_arpa = Arpa('http://demo.seco.tkk.fi/arpa/pnr_municipality')

    # Find
    log.info('Hautauskuntia: {h}'.format(h=len(list(surma_onto[NARCS.hautauskunta::]))))
    log.info('Hautausmaakuntia: {h}'.format(h=len(list(surma_onto[NARCS.hautausmaakunta::]))))

    for (sub, obj) in list(surma_onto[:NARCS.hautauskunta:]):  # hautauskunta --> hautausmaakunta
        surma_onto.remove((sub, NARCS.hautauskunta, obj))
        surma_onto.add((sub, NARCS.hautausmaakunta, obj))

    link_to_pnr(surma_onto, surma_onto, NARCS.hautausmaakunta_pnr, NARCS.hautausmaakunta, pnr_arpa)
    for (sub, obj) in list(surma_onto[:NARCS.hautausmaakunta_pnr:]):
        surma_onto.remove((sub, NARCS.hautausmaakunta_pnr, obj))
        surma_onto.remove((sub, NARCS.hautausmaakunta, None))
        surma_onto.add((sub, NARCS.hautausmaakunta, obj))

    for kunta in list(surma_onto[:RDF.type:NARCS.Kunta]):
        label = next(surma_onto[kunta:SKOS.prefLabel:])

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
            for lbl in labels:
                retry = 0
                while not warsa_matches:
                    try:
                        warsa_matches = pnr_arpa.get_uri_matches(lbl)['results']  # Link to PNR
                        break
                    except (HTTPError, ValueError):
                        if retry < 50:
                            log.error('Error getting "PNR" matches from ARPA, waiting 10 seconds before retrying...')
                            retry += 1
                            sleep(10)
                        else:
                            raise

        # NOTE! hautausmaakunta refers to current municipalities, unlike the rest

        if len(warsa_matches) == 0:
            if set(surma.subjects(None, kunta)) - set(surma[:NARCS.hautausmaakunta:kunta]):
                log.warning("Couldn't find URIs for municipality {lbl}".format(lbl=label))
        elif len(warsa_matches) == 1:
            match = warsa_matches[0]
            log.info('Found {lbl} municipality URI {s}'.format(lbl=label, s=match))

            for subj in list(surma[:NARCS.synnyinkunta:kunta]):
                surma.add((subj, NARCS.synnyinkunta, match))
                surma.remove((subj, NARCS.synnyinkunta, kunta))

            for subj in list(surma[:NARCS.kotikunta:kunta]):
                surma.add((subj, NARCS.kotikunta, match))
                surma.remove((subj, NARCS.kotikunta, kunta))

            for subj in list(surma[:NARCS.asuinkunta:kunta]):
                surma.add((subj, NARCS.asuinkunta, match))
                surma.remove((subj, NARCS.asuinkunta, kunta))

            for subj in list(surma[:NARCS.kuolinkunta:kunta]):
                surma.add((subj, NARCS.kuolinkunta, match))
                surma.remove((subj, NARCS.kuolinkunta, kunta))

            for subj in list(surma[:NARCS.haavoittumiskunta:kunta]):
                surma.add((subj, NARCS.haavoittumiskunta, match))
                surma.remove((subj, NARCS.haavoittumiskunta, kunta))

            for subj in list(surma[:NARCS.katoamiskunta:kunta]):
                surma.add((subj, NARCS.katoamiskunta, match))
                surma.remove((subj, NARCS.katoamiskunta, kunta))

            for subj in list(surma_onto[:NARCS.hautausmaakunta:kunta]):
                # Fixes cemetery municipalities
                surma_onto.remove((subj, NARCS.hautausmaakunta, kunta))
                surma_onto.add((subj, NARCS.hautausmaakunta, match))
        else:
            log.warning('Found multiple URIs for municipality {lbl}: {s}'.format(lbl=label, s=warsa_matches))


def validate():
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


def link_to_military_ranks(ranks):
    """
    Link casualties to their ranks in Warsa
    """
    p = NARCS.sotilasarvo
    for s, o in list(surma[:p:]):
        rank_label = next(old_ranks[o:SKOS.prefLabel:], '')
        rank_label = Literal(str(rank_label).capitalize())  # Strip lang attribute and capitalize
        if not rank_label:
            # This happens when military ranks are already linked
            continue

        found_ranks = list(ranks[:SKOS.prefLabel:rank_label]) + list(ranks[:SKOS.altLabel:rank_label])

        new_o = None
        if len(found_ranks) == 1:
            new_o = found_ranks[0]
        elif len(found_ranks) > 1:
            log.warning('Found multiple ranks for {rank}'.format(rank=rank_label))
        else:
            log.warning('Couldn\'t find military rank for {rank}'.format(rank=rank_label))

        if new_o:
            surma.remove((s, p, o))
            surma.add((s, p, new_o))


def handle_persons(ranks):
    """
    Link death records to WARSA persons, unify and stylize name representations, fix some errors.

    :param ranks: military ranks
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

    dateset = set()
    date_props = [NARCS.haavoittumisaika, NARCS.katoamisaika, NARCS.kuolinaika, NARCS.syntymaeaika]
    for date_prop in date_props:
        dateset |= set(surma.objects(None, date_prop))

    for date in dateset:
        try:
            parsed_date = iso8601.parse_date(str(date))
            if parsed_date.year < 1840:
                raise TypeError
        except (iso8601.ParseError, TypeError):
            if str(date).startswith('09') or str(date).startswith('10'):
                new_date = '19' + str(date)[2:]
            elif str(date).startswith('1940-93'):
                new_date = '1940-03' + str(date)[7:]
            elif str(date).startswith('0041'):
                new_date = '1941' + str(date)[4:]
            else:
                new_date = None

            fixed = False
            if str(date)[1:] != 'XXX-XX-XX':
                for date_prop in date_props:
                    if new_date:
                        log.debug('Fixing invalid date: {date}  to {date2}'.format(date=str(date), date2=new_date))
                        for triple in surma.triples((None, date_prop, date)):
                            log.debug('Adding a reference to fixed date: {date}'.format(date=str(new_date)))
                            surma.add((triple[0], date_prop, Literal(new_date, datatype=XSD.date)))

                        surma.remove((None, date_prop, date))
                        fixed = True
            else:
                log.info('Removing all references to invalid date: {date}'.format(date=str(date)))
                for date_prop in date_props:
                    surma.remove((None, date_prop, date))

            if not fixed:
                log.info('Not able to fix invalid date: {date}'.format(date=str(date)))


#######
# MAIN

if __name__ == "__main__":

    ranks = None

    ##################
    # READ IN RDF DATA

    if USE_GENERATED_FILES:
        # Read RDF graph from TTL files
        print('Processing previously generated RDF files...')
        surma.parse(OUTPUT_FILE_DIRECTORY + "surma.ttl", format='turtle')
        surma_onto.parse(OUTPUT_FILE_DIRECTORY + "surma_onto.ttl", format='turtle')
        print('Parsed {len} data triples.'.format(len=len(surma)))
        print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
    elif USE_GENERATED_PKL:
        # Read RDF graph from generated output pickles
        print('Processing previously generated pickles...')
        surma = joblib.load(OUTPUT_FILE_DIRECTORY + 'surma.pkl')
        surma_onto = joblib.load(OUTPUT_FILE_DIRECTORY + 'surma_onto.pkl')
        print('Parsed {len} data triples.'.format(len=len(surma)))
        print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
    else:
        if not reload:
            # Read RDF graph from pickle
            try:
                surma = joblib.load(INPUT_FILE_DIRECTORY + 'surma.pkl')
                surma_onto = joblib.load(INPUT_FILE_DIRECTORY + 'surma_onto.pkl')
                print('Parsed {len} data triples from pickle object.'.format(len=len(surma)))
                print('Parsed {len} ontology triples from pickle object.'.format(len=len(surma_onto)))
            except IOError:
                reload = True

        if reload:
            # Read RDF graph from TTL files
            print('Processing Sotasurma RDF files...')

            surma.parse(INPUT_FILE_DIRECTORY + DATA_FILE, format='turtle')

            input_dir = '{base}/{dir}'.format(base=os.getcwd(), dir=INPUT_FILE_DIRECTORY)
            for f in ['aidinkieli.ttl', 'hautausmaat.ttl', 'kansalaisuus.ttl', 'kansallisuus.ttl',
                      'kunnat.ttl', 'menehtymisluokka.ttl', 'narc-schema.ttl', 'siviilisaaty.ttl',
                      'sukupuoli.ttl']:
                surma_onto.parse(input_dir + f, format='turtle')
                log.debug('Parsed schema file %s' % f)

            print('Parsed {len} data triples.'.format(len=len(surma)))
            print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
            print('Writing graphs to pickle objects...')
            joblib.dump(surma, INPUT_FILE_DIRECTORY + 'surma.pkl')
            joblib.dump(surma_onto, INPUT_FILE_DIRECTORY + 'surma_onto.pkl')

    old_ranks.parse(INPUT_FILE_DIRECTORY + 'old_ranks.ttl', format='turtle')
    ranks = r.read_graph_from_sparql("http://ldf.fi/warsa/sparql", 'http://ldf.fi/warsa/ranks')
    if len(list(ranks)) == 0:
        print('Unable to read military ranks from SPARQL endpoint')
        quit()

    #####################################
    # FIX KNOWN ISSUES IN DATA AND SCHEMA

    print('Applying direct URI mapping fixes...')
    fix_by_direct_uri_mappings()

    # FOAF Person instances to DeathRecord instances
    for (sub, pred) in surma[::FOAF.Person]:
        surma.add((sub, pred, NARCS.DeathRecord))
        surma.remove((sub, pred, FOAF.Person))

    for (sub, pred) in surma_onto[::FOAF.Person]:
        surma_onto.add((sub, pred, NARCS.DeathRecord))
        surma_onto.remove((sub, pred, FOAF.Person))

    # Additional fixes to schema
    UNIT_LINK_URI = NARCS.osasto

    surma_onto.add((UNIT_LINK_URI, RDF.type, OWL.ObjectProperty))
    surma_onto.add((UNIT_LINK_URI, RDFS.label, Literal('Tunnettu joukko-osasto', lang='fi')))
    surma_onto.add((UNIT_LINK_URI, RDFS.label, Literal('Military unit', lang='en')))
    surma_onto.add((UNIT_LINK_URI, RDFS.domain, NARCS.DeathRecord))
    surma_onto.add((UNIT_LINK_URI, RDFS.range, URIRef('http://ldf.fi/schema/warsa/MilitaryUnit')))
    surma_onto.add((UNIT_LINK_URI, SKOS.prefLabel, Literal('Tunnettu joukko-osasto', lang='fi')))
    surma_onto.add((UNIT_LINK_URI, SKOS.prefLabel, Literal('Military unit', lang='en')))

    surma_onto.add((NARCS.DeathRecord, RDFS.subClassOf, CRM.E31_Document))
    surma_onto.add((NARCS.DeathRecord, SKOS.prefLabel, Literal('Death Record', lang='en')))
    surma_onto.add((NARCS.DeathRecord, SKOS.prefLabel, Literal('Kuolinasiakirja', lang='fi')))

    surma_onto.add((NARCS.hautausmaakunta, RDF.type, OWL.ObjectProperty))
    surma_onto.add((NARCS.hautausmaakunta, RDFS.label, Literal('Hautausmaan kunta', lang='fi')))
    surma_onto.add((NARCS.hautausmaakunta, RDFS.domain, NARCS.Hautausmaa))
    surma_onto.add((NARCS.hautausmaakunta, RDFS.range, NARCS.Kunta))
    surma_onto.add((NARCS.hautausmaakunta, SKOS.prefLabel, Literal('Hautausmaan kunta', lang='fi')))

    surma_onto.remove((NARCS.hautausmaa, RDF.type, OWL.DatatypeProperty))
    surma_onto.add((NARCS.hautausmaa, RDF.type, OWL.ObjectProperty))

    surma_onto.remove((NARCS.hautausmaa, RDFS.range, XSD.string))
    surma_onto.add((NARCS.hautausmaa, RDFS.range, NARCS.Hautausmaa))

    surma_onto.remove((NARCS.sotilasarvo, RDFS.range, None))
    surma_onto.add((NARCS.sotilasarvo, RDFS.range, URIRef('http://ldf.fi/schema/warsa/Rank')))

    ############################################
    # LINK TO OTHER SOTASAMPO DATASETS AND STUFF

    print('Fixing cemeteries...')
    fix_cemetery_links()

    if not SKIP_MUNICIPALITIES:
        print('Linking to municipalities...')

        link_to_municipalities()

    if not SKIP_RANKS:
        print('Linking to military ranks...')
        link_to_military_ranks(ranks)

        surma_onto.remove((NARCS.sotilasarvo, RDFS.range, None))
        surma_onto.add((NARCS.sotilasarvo, RDFS.range, URIRef('http://ldf.fi/schema/warsa/Rank')))

    print('Handling persons...')

    handle_persons(ranks)

    print('Handling military units...')
    unit_link_uri = NARCS.osasto

    surma_onto.add((unit_link_uri, RDF.type, OWL.ObjectProperty))
    surma_onto.add((unit_link_uri, RDFS.label, Literal('Tunnettu joukko-osasto', lang='fi')))
    surma_onto.add((unit_link_uri, RDFS.label, Literal('Military unit', lang='en')))
    surma_onto.add((unit_link_uri, RDFS.domain, NARCS.DeathRecord))
    surma_onto.add((unit_link_uri, RDFS.range, URIRef('http://ldf.fi/schema/warsa/MilitaryUnit')))

    for (sub, obj) in surma_onto[:RDFS.label:]:
        surma_onto.add((sub, SKOS.prefLabel, obj))

    surma.parse(INPUT_FILE_DIRECTORY + 'surma_additions.ttl', format='turtle')  # Add handmade annotations

    # Take cemeteries into a separate file and change their URIs to general WARSA URIs

    C_SCHEMA = Namespace('http://ldf.fi/schema/warsa/places/cemeteries/')
    CEMETERIES = Namespace('http://ldf.fi/warsa/places/cemeteries/')

    # Modify predicate in casualties schema

    surma_onto.remove((NARCS.hautausmaa, RDFS.range, None))
    surma_onto.add((NARCS.hautausmaa, RDFS.range, C_SCHEMA.Cemetery))

    # Move cemeteries to own graph and namespace

    cemetery_schema = Graph()
    cemeteries = Graph()

    for s in list(surma_onto[:RDF.type:NARCS.Hautausmaa]):
        for (p, o) in list(surma_onto[s::]):
            new_p = None
            new_o = None
            if p == NARCS.hautausmaakunta:
                new_p = C_SCHEMA.former_municipality
                # if 'http://ldf.fi/pnr/' in str(o):
                # elif 'http://ldf.fi/narc-menehtyneet1939-45/kunnat/' in str(o):
                #     new_p = C_SCHEMA.municipality_wartime

            if p == RDF.type:
                new_o = C_SCHEMA.Cemetery

            surma_onto.remove((s, p, o))

            references = list(surma.subject_predicates(s))
            if references:
                new_s = CEMETERIES[str(s).split('/')[-1]]
                cemeteries.add((new_s, new_p or p, new_o or o))

                for (cem_s, cem_p) in references:
                    surma.remove((cem_s, cem_p, s))
                    surma.add((cem_s, cem_p, new_s))

    # Remove predicates from schema

    surma_onto.remove((NARCS.hautausmaakunta, None, None))
    surma_onto.remove((NARCS.hautauskunta, None, None))

    # Move cemetery class to own graph, add schema stuff

    surma_onto.remove((NARCS.Hautausmaa, None, None))
    cemetery_schema.add((C_SCHEMA.Cemetery, SKOS.prefLabel, Literal('Hautausmaa', lang='fi')))
    cemetery_schema.add((C_SCHEMA.Cemetery, SKOS.prefLabel, Literal('Cemetery', lang='en')))
    cemetery_schema.add((C_SCHEMA.Cemetery, RDFS.subClassOf, CRM.E27_Site))
    cemetery_schema.add((C_SCHEMA.Cemetery, RDFS.subClassOf, CRM.E53_Place))

    # From CIDOC CRM documentation:
    # In the case of an E26 Physical Feature the default reference space is the
    # one in which the object that bears the feature or at least the surrounding matter of the feature is at rest.
    # In this case there is a 1:1 relation of E26 Feature and E53 Place. For simplicity of implementation
    # multiple inheritance (E26 Feature IsA E53 Place) may be a practical approach.

    cemetery_schema.add((C_SCHEMA.municipality_2016, RDFS.subPropertyOf, CRM.P89_falls_within))
    cemetery_schema.add((C_SCHEMA.municipality_2016, SKOS.prefLabel, Literal('Hautausmaan kunta 2016', lang='fi')))
    cemetery_schema.add((C_SCHEMA.municipality_2016, SKOS.prefLabel, Literal('Municipality of cemetery in 2016', lang='en')))

    cemetery_schema.add((C_SCHEMA.municipality_90, RDFS.subPropertyOf, CRM.P89_falls_within))
    cemetery_schema.add((C_SCHEMA.municipality_90, SKOS.prefLabel, Literal('Hautausmaan kunta 1990-luvulla', lang='fi')))
    cemetery_schema.add((C_SCHEMA.municipality_90, SKOS.prefLabel, Literal('Municipality of cemetery in 1990\'s', lang='en')))

    cemetery_schema.add((C_SCHEMA.municipality_wartime, RDFS.subPropertyOf, CRM.P89_falls_within))
    cemetery_schema.add((C_SCHEMA.municipality_wartime, SKOS.prefLabel, Literal('Hautausmaan kunta toisen maailman sodan aikana', lang='fi')))
    cemetery_schema.add((C_SCHEMA.municipality_wartime, SKOS.prefLabel, Literal('Municipality of cemetery during the second world war', lang='en')))

    ##################
    # SERIALIZE GRAPHS

    if not SKIP_VALIDATION:
        print('Validating graphs for unknown link targets and unlinked subjects...')
        validate()

    if not DRYRUN:
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
        surma.bind("warsa-arvo", "http://ldf.fi/warsa/actors/ranks/")
        surma.bind("warsa-toimija", "http://ldf.fi/warsa/actors/")

        surma_onto.bind("crm", "http://www.cidoc-crm.org/cidoc-crm/")
        surma_onto.bind("skos", "http://www.w3.org/2004/02/skos/core#")

        surma_onto.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
        surma_onto.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")
        surma_onto.bind("narc-kieli", "http://ldf.fi/narc-menehtyneet1939-45/aeidinkieli/")
        surma_onto.bind("narc-kansalaisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/")
        surma_onto.bind("narc-kansallisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/")
        surma_onto.bind("narc-menehtymisluokka", "http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/")
        surma_onto.bind("narc-siviilisaeaety", "http://ldf.fi/narc-menehtyneet1939-45/siviilisaeaety/")
        surma_onto.bind("narc-sotilasarvo", "http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/")
        surma_onto.bind("narc-kunta", "http://ldf.fi/narc-menehtyneet1939-45/kunnat/")

        surma_onto.bind("warsa-kunta", "http://ldf.fi/warsa/places/municipalities/")
        surma_onto.bind("warsa-arvo", "http://ldf.fi/warsa/actors/ranks/")
        surma_onto.bind("warsa-toimija", "http://ldf.fi/warsa/actors/")

        surma_onto.bind("geo", "http://www.georss.org/georss/")
        surma_onto.bind("dct", "http://purl.org/dc/terms/")

        surma.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma.ttl")
        surma_onto.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma_onto.ttl")

        cemeteries.bind("wc", 'http://ldf.fi/warsa/places/cemeteries/')
        cemeteries.bind("wcs", 'http://ldf.fi/schema/warsa/places/cemeteries/')

        cemetery_schema.bind("wcs", 'http://ldf.fi/schema/warsa/places/cemeteries/')

        cemeteries.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "cemeteries.ttl")
        cemetery_schema.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "cemetery_schema.ttl")

        print('Saving pickles...')
        joblib.dump(surma, OUTPUT_FILE_DIRECTORY + 'surma.pkl')
        joblib.dump(surma_onto, OUTPUT_FILE_DIRECTORY + 'surma_onto.pkl')

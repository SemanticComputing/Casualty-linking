#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing the original RDF version of
"Casualties during the Finnish wars 1939–1945" (Suomen sodissa 1939–1945 menehtyneet) RDF dataset.

Validates, improves and automatically links to other WarSampo datasets.

Input CSV data used is not publicly available.

Copyright (c) 2016 Mikko Koho
"""

import argparse
import logging
import os
import re
from time import sleep
from requests import HTTPError

from arpa_linker.arpa import Arpa
import iso8601
import joblib
import pandas as pd
from rdflib import *
import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON
from sotasampo_helpers import arpa
import rdf_dm as r
from sotasampo_helpers.arpa import link_to_pnr

INPUT_FILE_DIRECTORY = 'data/'
OUTPUT_FILE_DIRECTORY = 'data/new/'

DATA_FILE = 'surma.ttl'

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

URI_MAPPINGS = {
    # MANUAL FIXES TO SOME URI'S USED AS TRIPLE OBJECTS
    ns_sotilasarvo.Alipuseeri: ns_sotilasarvo.Aliupseeri,
    ns_sotilasarvo.Alikers__: ns_sotilasarvo.Alikersantti,
    ns_sotilasarvo.Jaeaek_: ns_sotilasarvo.Jaeaekaeri,
    ns_sotilasarvo.Miehisto: ns_sotilasarvo.Miehistoe,
    ns_sotilasarvo.Stm__________: ns_sotilasarvo.Sotamies,
    ns_sotilasarvo._stm_: ns_sotilasarvo.Sotamies,
    ns_sotilasarvo.Matr__: ns_sotilasarvo.Matruusi,
    ns_sotilasarvo.Muu: ns_sotilasarvo.Muu_arvo,
    ns_kansalaisuus.Fi_: ns_kansalaisuus.Suomi,
    ns_kansallisuus.Fi_: ns_kansallisuus.Suomi,
    ns_hautausmaat.x___: ns_hautausmaat.hx_0,
    ns_kunnat.kx: ns_kunnat.k,
    ns_menehtymisluokka._ : ns_menehtymisluokka.Tuntematon,
}

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-reload', action='store_true', help='Reload RDF graphs, instead of using pickle object')
parser.add_argument('-generated', action='store_true', help='Use previously generated new data files as input')
parser.add_argument('-skip_cemeteries', action='store_true', help='Skip cemetery fixing')
parser.add_argument('-skip_validation', action='store_true', help='Skip validation')
parser.add_argument('-skip_units', action='store_true', help='Skip linking to Warsa military units')
parser.add_argument('-skip_ranks', action='store_true', help='Skip linking to Warsa military ranks')
parser.add_argument('-skip_municipalities', action='store_true', help='Skip linking to municipalities')
parser.add_argument('-skip_persons', action='store_true', help='Skip linking to Warsa persons')
parser.add_argument('-skip_occupations', action='store_true', help='Skip creation of occupation ontology')
parser.add_argument('-d', action='store_true', help='Dry run, don\'t serialize created graphs')
args = parser.parse_args()

reload = args.reload
USE_GENERATED_FILES = args.generated
DRYRUN = args.d
SKIP_CEMETERIES = args.skip_cemeteries
SKIP_VALIDATION = args.skip_validation
SKIP_UNITS = args.skip_units
SKIP_MUNICIPALITIES = args.skip_municipalities
SKIP_RANKS = args.skip_ranks
SKIP_PERSONS = args.skip_persons
SKIP_OCCUPATIONS = args.skip_occupations

surma = rdflib.Graph()
surma_onto = rdflib.Graph()

logging.basicConfig(filename='Sotasurma.log',
                    filemode='a',
                    # level=logging.DEBUG,
                    level=logging.INFO,
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
    Fix errors in cemetery links and add missing cemetery instances.
    """
    p = ns_schema.hautausmaa

    for s, o in list(surma[:p:]):
        assert str(o).startswith(ns_hautausmaat)

        new_o = URIRef(re.sub(r'(/hautausmaat/)(\d+)', r'\1h\2', str(o)))
        if not list(surma_onto[new_o::]):
            new_o = None

        cemetery_id = str(o)[50:]  # Should be like '0243_3'
        k_id, h_id = cemetery_id[:4], cemetery_id[5:].replace('_', '')

        k_name = ''
        h_name = ''
        try:
            h_name = hmaat[hmaat['kunta_id'] == k_id][hmaat['hmaa_id'] == (int(h_id) if h_id.isnumeric() else 0)]['hmaa_name'].iloc[0]
        except IndexError:
            pass

        try:
            k_name = kunta[kunta['kunta_id'] == k_id]['kunta_name'].iloc[0]
            h_name = h_name or '{kunta} {id}'.format(kunta=k_name, id=h_id)
        except IndexError:
            pass

        if not new_o:
            try:
                new_o = ns_hautausmaat['h{cem_id}'.format(cem_id=cemetery_id)]
            except ValueError:
                log.error('Invalid cemetery id: {id}'.format(id=cemetery_id))
                continue

            assert h_name, s    # Should have a name to use as prefLabel
            assert k_name, s    # Should have a name for municipality

            kunta_node = next(surma_onto[:ns_skos.prefLabel:Literal(k_name, lang='fi')])

            surma_onto.add((new_o, RDF.type, ns_schema.Hautausmaa))
            surma_onto.add((new_o, ns_skos.prefLabel, Literal(h_name)))
            surma_onto.add((new_o, ns_schema.hautausmaakunta, kunta_node))

            log.info('New cemetery %s : %s' % (new_o, h_name))

        else:
            h_name_onto = str(next(surma_onto[new_o:ns_skos.prefLabel:]))
            if h_name:
                assert h_name == h_name_onto or h_name == h_name_onto.split()[0], '%s  !=  %s' % (h_name, h_name_onto)

        if new_o:
            surma.remove((s, p, o))
            surma.add((s, p, new_o))


def link_to_warsa_municipalities():
    """
    Link to Warsa municipalities used in SotaSampo project.
    """
    munics = r.helpers.read_graph_from_sparql("http://ldf.fi/warsa/sparql",
                                              graph_name='http://ldf.fi/warsa/places/municipalities')

    pnr_arpa = Arpa('http://demo.seco.tkk.fi/arpa/pnr_municipality')

    for s in list(surma_onto[:RDF.type:ns_schema.Kunta]):
        label = next(surma_onto[s:ns_skos.prefLabel:])

        warsa_s = []

        for lbl in str(label).strip().split('/'):
            if lbl == 'Pyhäjärvi Ol':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_75')]
            elif lbl == 'Pyhäjärvi Ul.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_543')]
            elif lbl == 'Pyhäjärvi Vl':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_586')]
            elif lbl == 'Koski Tl.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_291')]
            elif lbl == 'Koski Hl.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_391')]
            elif lbl == 'Uusikirkko Vl':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_609')]

            if not warsa_s:
                warsa_s = list(munics[:ns_skos.prefLabel:Literal(lbl)])
            if not warsa_s:
                warsa_s = list(munics[:ns_skos.prefLabel:Literal(lbl.replace(' kunta', ' mlk'))])

            if not warsa_s:
                retry = 0
                while not warsa_s:
                    try:
                        warsa_s = [URIRef(uri) for uri in pnr_arpa.get_uri_matches(lbl)]  # Link to Paikannimirekisteri
                        break
                    except (HTTPError, ValueError):
                        if retry < 50:
                            log.error('Error getting "Paikannimirekisteri" matches from ARPA, waiting 10 seconds before retrying...')
                            retry += 1
                            sleep(10)
                        else:
                            raise

        # NOTE! hautausmaakunta refers to current municipalities, unlike the rest

        if len(warsa_s) == 0:
            if set(surma.subjects(None, s)) - set(surma[:ns_schema.hautauskunta:s]):
                log.warning("Couldn't find URIs for municipality {lbl}".format(lbl=label))
        elif len(warsa_s) == 1:
            log.info('Found {lbl} municipality URI {s}'.format(lbl=label, s=warsa_s[0]))
            for subj in list(surma[:ns_schema.synnyinkunta:s]):
                surma.add((subj, ns_schema.synnyinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.synnyinkunta, s))
            for subj in list(surma[:ns_schema.kotikunta:s]):
                surma.add((subj, ns_schema.kotikunta, warsa_s[0]))
                surma.remove((subj, ns_schema.kotikunta, s))
            for subj in list(surma[:ns_schema.asuinkunta:s]):
                surma.add((subj, ns_schema.asuinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.asuinkunta, s))
            for subj in list(surma[:ns_schema.kuolinkunta:s]):
                surma.add((subj, ns_schema.kuolinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.kuolinkunta, s))
            for subj in list(surma[:ns_schema.haavoittumiskunta:s]):
                surma.add((subj, ns_schema.haavoittumiskunta, warsa_s[0]))
                surma.remove((subj, ns_schema.haavoittumiskunta, s))
            for subj in list(surma[:ns_schema.katoamiskunta:s]):
                surma.add((subj, ns_schema.katoamiskunta, warsa_s[0]))
                surma.remove((subj, ns_schema.katoamiskunta, s))

            for subj in list(surma_onto[:ns_schema.hautauskunta:s]):
                # Fixes cemetery municipalities
                surma_onto.remove((subj, ns_schema.hautauskunta, s))
                surma_onto.add((subj, ns_schema.hautausmaakunta, warsa_s[0]))
        else:
            log.warning('Found multiple URIs for municipality {lbl}: {s}'.format(lbl=label, s=warsa_s))

    link_to_pnr(surma_onto, surma_onto, ns_schema.hautausmaakunta_pnr, ns_schema.hautausmaakunta)
    for (sub, obj) in list(surma_onto[:ns_schema.hautausmaakunta_pnr:]):
        surma_onto.remove((sub, ns_schema.hautausmaakunta_pnr, obj))
        surma_onto.remove((sub, ns_schema.hautausmaakunta, None))
        surma_onto.add((sub, ns_schema.hautausmaakunta, obj))

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
            log.warning('Unknown URI references: {uri}  (referenced {num} times)'.format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))

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
    p = ns_schema.sotilasarvo
    for s, o in list(surma[:p:]):
        rank_label = next(surma_onto[o:ns_skos.prefLabel:], '')
        rank_label = Literal(str(rank_label).capitalize())  # Strip lang attribute and capitalize
        found_ranks = list(ranks[:ns_skos.prefLabel:rank_label])

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


def link_persons(ranks):
    """
    Link death records to WARSA persons, unify and stylize name representations, fix some errors.

    :param ranks: military ranks
    """
    # Unify previous last names to same format as WARSA actors: LASTNAME (ent PREVIOUS)
    for lbl_pred in [ns_schema.sukunimi, ns_skos.prefLabel]:
        for (person, lname) in list(surma[:lbl_pred:]):
            new_name = re.sub(r'(\w)0(\w)', r'\1O\2', lname)
            new_name = re.sub('%', '/', new_name)
            new_lname = Literal(re.sub(r'(\w\w +)(E(?:NT)?\.)\s?(\w+)', r'\1(ent \3)', str(new_name)))
            if new_lname and new_lname != lname:
                log.info('Unifying lastname {ln} to {nln}'.format(ln=lname, nln=new_lname))
                surma.add((person, lbl_pred, new_lname))
                surma.remove((person, lbl_pred, lname))

    # Change names from all uppercase to capitalized
    for lbl_pred in [ns_schema.etunimet, ns_schema.sukunimi, ns_skos.prefLabel]:
        for (sub, obj) in list(surma[:lbl_pred:]):
            name = str(obj)
            new_name = str.title(name)
            surma.remove((sub, lbl_pred, obj))
            surma.add((sub, lbl_pred, Literal(new_name)))
            log.debug('Changed name {orig} to {new}'.format(orig=name, new=new_name))

    # Link to WARSA actor persons
    log.debug(arpa.link_to_warsa_persons(surma, ranks, ns_crm.P70_documents, ns_schema.sotilasarvo,
                                         ns_schema.etunimet, ns_schema.sukunimi,
                                         ns_schema.syntymaeaika, ns_schema.kuolinaika,
                                         endpoint='http://demo.seco.tkk.fi/arpa/menehtyneet_persons'))

    for s, o in surma[:ns_crm.P70_documents:]:
        log.info('ARPA found that {s} is the death record of person {o}'.format(s=s, o=o))

    # Create person links based on inverse links that have been made when creating the person instances from casualties.
    sparql = SPARQLWrapper('http://ldf.fi/warsa/sparql')
    for person in list(surma[:RDF.type:ns_foaf.Person]):
        sparql.setQuery("""
                        PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
                        SELECT * WHERE {{ ?sub crm:P70i_is_documented_in <{person_uri}> . }}
                        """.format(person_uri=person))
        sparql.setReturnFormat(JSON)

        results = None
        retry = 0
        while not results:
            try:
                results = sparql.query().convert()
            except ValueError:
                if retry < 50:
                    log.error('Malformed result from SPARQL endpoint for person {p_uri}, waiting 10 seconds before retrying...'.format(p_uri=person))
                    retry += 1
                    sleep(10)
                else:
                    raise

        warsa_person = None
        for result in results["results"]["bindings"]:
            warsa_person = result["sub"]["value"]
            log.debug('{pers} matches WARSA person {warsa_pers}'.format(pers=person, warsa_pers=warsa_person))
            surma.add((person, ns_crm.P70_documents, URIRef(warsa_person)))

        if not warsa_person:
            log.warning('{person} didn\'t match any WARSA persons.'.format(person=person))

    for (sub, pred) in surma[::ns_foaf.Person]:
        surma.add((sub, pred, ns_schema.DeathRecord))
        # surma.add((sub, pred, ns_crm.E31_Document))
        surma.remove((sub, pred, ns_foaf.Person))

    for (sub, pred) in surma_onto[::ns_foaf.Person]:
        surma_onto.add((sub, pred, ns_schema.DeathRecord))
        surma_onto.remove((sub, pred, ns_foaf.Person))

    # Remove invalid dates

    dateset = set()
    date_props = [ns_schema.haavoittumisaika, ns_schema.katoamisaika, ns_schema.kuolinaika, ns_schema.syntymaeaika]
    for date_prop in date_props:
        dateset |= set(surma.objects(None, date_prop))

    for date in dateset:
        try:
            parsed_date = iso8601.parse_date(str(date))
            if parsed_date.year < 1840:
                raise TypeError
        except (iso8601.ParseError, TypeError):
            log.info('Removing references to invalid date: {date}'.format(date=str(date)))
            for date_prop in date_props:
                surma.remove((None, date_prop, date))



#######
# MAIN

if __name__ == "__main__":

    ranks = None

    if not SKIP_CEMETERIES:
        ##################
        # READ IN CSV DATA

        print('Reading CSV data for cemeteries...')

        hmaat = pd.read_csv(INPUT_FILE_DIRECTORY + 'csv/MEN_HMAAT.CSV', encoding='latin_1', header=None, index_col=False,
                            names=['kunta_id', 'hmaa_id', 'hmaa_name'], sep=',', quotechar='"', na_values=['  '])
        """:type : pd.DataFrame"""  # for PyCharm type hinting

        kunta = pd.read_csv(INPUT_FILE_DIRECTORY + 'csv/MEN_KUNTA.CSV', encoding='latin_1', header=None, index_col=False,
                            names=['kunta_id', 'kunta_name'], sep=',', quotechar='"', na_values=['  '])
        """:type : pd.DataFrame"""  # for PyCharm type hinting

        # Strip whitespace from cemetery names
        # hmaat.hmaa_name = hmaat.hmaa_name.map(lambda x: x.strip())
        hmaat = hmaat.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        kunta = kunta.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    ##################
    # READ IN RDF DATA

    if USE_GENERATED_FILES:
            # Read RDF graph from TTL files
            print('Processing previously generated RDF files...')
            surma.parse(OUTPUT_FILE_DIRECTORY + "surma.ttl", format='turtle')
            surma_onto.parse(OUTPUT_FILE_DIRECTORY + "surma_onto.ttl", format='turtle')
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
            for f in os.listdir(input_dir):
                if f not in [DATA_FILE, "void.ttl"] and f.endswith('.ttl'):
                    surma_onto.parse(input_dir + f, format='turtle')

            print('Parsed {len} data triples.'.format(len=len(surma)))
            print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
            print('Writing graphs to pickle objects...')
            joblib.dump(surma, INPUT_FILE_DIRECTORY + 'surma.pkl')
            joblib.dump(surma_onto, INPUT_FILE_DIRECTORY + 'surma_onto.pkl')

    ##########################################################
    # FIX KNOWN ISSUES AND ADD LINKS TO OTHER SOTASAMPO GRAPHS

    print('Applying direct URI mapping fixes...')
    fix_by_direct_uri_mappings()

    # TODO: Add military rank group ontology description
    # TODO: Add english ontology descriptions?

    if not SKIP_CEMETERIES:
        print('Fixing cemeteries...')
        fix_cemetery_links()

        # TODO: Add graveyard ontology description

        surma_onto.add((ns_schema.hautausmaakunta, RDF.type, OWL.ObjectProperty))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.label, Literal('Hautausmaan kunta', lang='fi')))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.domain, ns_schema.Hautausmaa))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.range, ns_schema.Kunta))
        surma_onto.add((ns_schema.hautausmaakunta, ns_skos.prefLabel, Literal('Hautausmaan kunta', lang='fi')))

        surma_onto.remove((ns_schema.hautausmaa, RDF.type, ns_owl.DatatypeProperty))
        surma_onto.add((ns_schema.hautausmaa, RDF.type, ns_owl.ObjectProperty))

        surma_onto.remove((ns_schema.hautausmaa, RDFS.range, XSD.string))
        surma_onto.add((ns_schema.hautausmaa, RDFS.range, ns_schema.Hautausmaa))

    if not SKIP_MUNICIPALITIES:
        print('Linking to municipalities...')

        for p in list(surma[:RDF.type:ns_schema.DeathRecord]):
            # Removing hautauskunta from death records
            surma.remove((p, ns_schema.hautauskunta, None))

        link_to_warsa_municipalities()

    if not SKIP_RANKS:
        print('Linking to military ranks...')
        ranks = r.read_graph_from_sparql("http://ldf.fi/warsa/sparql", 'http://ldf.fi/warsa/actors/ranks')
        if len(list(ranks)) == 0:
            log.error('Unable to read military ranks from SPARQL endpoint')
        link_to_military_ranks(ranks)

        surma_onto.remove((ns_schema.sotilasarvo, RDFS.range, None))
        surma_onto.add((ns_schema.sotilasarvo, RDFS.range, URIRef('http://ldf.fi/warsa/actors/ranks/Rank')))

    if not SKIP_PERSONS:
        print('Finding links for WARSA persons...')

        # Note: Requires updated military ranks
        if not ranks:
            ranks = r.read_graph_from_sparql("http://ldf.fi/warsa/sparql", 'http://ldf.fi/warsa/actors/actor_types')

        link_persons(ranks)

    if not SKIP_UNITS:
        print('Finding links for military units...')
        unit_link_uri = ns_schema.osasto

        log.debug(arpa.link_to_military_units(surma, unit_link_uri, ns_schema.joukko_osasto))

        # surma_onto.remove((ns_schema.osasto, None, None))
        # surma.remove((None, ns_schema.osasto, None))

        surma_onto.add((unit_link_uri, RDF.type, OWL.ObjectProperty))
        surma_onto.add((unit_link_uri, RDFS.label, Literal('Tunnettu joukko-osasto', lang='fi')))
        surma_onto.add((unit_link_uri, RDFS.label, Literal('Military unit', lang='en')))
        surma_onto.add((unit_link_uri, RDFS.domain, ns_schema.DeathRecord))
        # surma_onto.add((unit_link_uri, RDFS.domain, ns_crm.E31_Document))
        surma_onto.add((unit_link_uri, RDFS.range, URIRef('http://ldf.fi/warsa/actors/actor_types/MilitaryUnit')))
        surma_onto.add((unit_link_uri, ns_skos.prefLabel, Literal('Tunnettu joukko-osasto', lang='fi')))
        surma_onto.add((unit_link_uri, ns_skos.prefLabel, Literal('Military unit', lang='en')))

    if not SKIP_OCCUPATIONS:
        for s, o in list(surma[:ns_schema.ammatti:]):
            # TODO: Filter some o values
            occupation_uri = ns_schema[str(o)]
            surma.remove((s, ns_schema.ammatti, o))
            surma.add((s, ns_schema.ammatti, occupation_uri))

            surma_onto.add((occupation_uri, ns_skos.prefLabel, o))
            surma_onto.add((occupation_uri, RDF.type, ns_schema.Ammatti))

    surma_onto.add((ns_schema.DeathRecord, RDFS.subClassOf, ns_crm.E31_Document))
    surma_onto.add((ns_schema.DeathRecord, ns_skos.prefLabel, Literal('Death Record', lang='en')))
    surma_onto.add((ns_schema.DeathRecord, ns_skos.prefLabel, Literal('Kuolinasiakirja', lang='fi')))

    ##################
    # SERIALIZE GRAPHS

    if not SKIP_VALIDATION:
        print('Validating graphs for unknown link targets and unlinked subjects...')
        validate()

    if not DRYRUN:
        print('Serializing graphs...')
        surma.bind("crm", "http://www.cidoc-crm.org/cidoc-crm/")

        surma.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
        surma.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")
        surma.bind("narc-kieli", "http://ldf.fi/narc-menehtyneet1939-45/aeidinkieli/")
        surma.bind("narc-hautausmaa", "http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/")
        surma.bind("narc-kansalaisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/")
        surma.bind("narc-kansallisuus", "http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/")
        surma.bind("narc-menehtymisluokka", "http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/")
        surma.bind("narc-siviilisaeaety", "http://ldf.fi/narc-menehtyneet1939-45/siviilisaeaety/")
        surma.bind("narc-kunta", "http://ldf.fi/narc-menehtyneet1939-45/kunnat/")
        surma.bind("narc-sukupuoli", "http://ldf.fi/narc-menehtyneet1939-45/sukupuoli/")

        surma.bind("warsa-kunta", "http://ldf.fi/warsa/places/municipalities/")
        surma.bind("warsa-arvo", "http://ldf.fi/warsa/actors/ranks/")
        surma.bind("warsa-toimija", "http://ldf.fi/warsa/actors/")

        # TODO: Move schema stuff to schema namespace? (e.g. skos:ConceptSchemes)

        surma_onto.bind("crm", "http://www.cidoc-crm.org/cidoc-crm/")

        surma_onto.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
        surma_onto.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")
        surma_onto.bind("narc-kieli", "http://ldf.fi/narc-menehtyneet1939-45/aeidinkieli/")
        surma_onto.bind("narc-hautausmaa", "http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/")
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

#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""Casualty linking tasks"""

import argparse
import logging
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from json import load

import rdf_dm as r
from SPARQLWrapper import SPARQLWrapper, JSON
from arpa_linker.arpa import ArpaMimic, process_graph, Arpa, combine_values
from dedupe import RecordLink, trainingDataLink
from fuzzywuzzy import fuzz
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.exceptions import UniquenessError
from rdflib.util import guess_format

from mapping import CASUALTY_MAPPING
from namespaces import SKOS, CRM, BIOC, SCHEMA_CAS, SCHEMA_WARSA, bind_namespaces, SCHEMA_ACTORS
from sotasampo_helpers.arpa import link_to_pnr
from warsa_linkers.utils import query_sparql
from warsa_linkers.person_record_linkage import link_persons, get_date_value
from warsa_linkers.occupations import link_occupations
from warsa_linkers.ranks import link_ranks
from warsa_linkers.units import preprocessor, Validator

# TODO: Write some tests using responses

log = logging.getLogger(__name__)

MUNICIPALITY_MAPPING = {
    'Kemi': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_20'),
    'Pyhäjärvi Ol': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_75'),
    'Pyhäjärvi Ul.': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_543'),
    'Pyhäjärvi Vl': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_586'),
    'Koski Tl.': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_291'),
    'Koski Hl.': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_391'),
    'Koski Vl.': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_609'),
    'Oulun mlk': URIRef('http://ldf.fi/warsa/places/municipalities/m_place_65'),
}


def _preprocess(literal, prisoner, subgraph):
    """Default preprocess implementation for link function"""
    return str(literal).strip()


def link(graph, arpa, source_prop, target_graph, target_prop, preprocess=_preprocess, validator=None):
    """
    Link entities with ARPA based on parameters

    :return: target_graph with found links
    """
    prop_str = str(source_prop).split('/')[-1]  # Used for logging

    for (prisoner, value_literal) in list(graph[:source_prop:]):
        value = preprocess(value_literal, prisoner, graph)

        log.debug('Finding links for %s (originally %s)' % (value, value_literal))

        if value:
            arpa_result = arpa.query(value)
            if arpa_result:
                res = arpa_result[0]['id']

                if validator:
                    res = validator.validate(arpa_result, value_literal, prisoner)
                    if not res:
                        log.info('Match {res} failed validation for {val}, skipping it'.
                                 format(res=res, val=value_literal))
                        continue

                log.info('Accepted a match for property {ps} with original value {val} : {res}'.
                         format(ps=prop_str, val=value_literal, res=res))

                target_graph.add((prisoner, target_prop, URIRef(res)))
            else:
                log.warning('No match found for %s: %s' % (prop_str, value))

    return target_graph


def _generate_casualties_dict(graph: Graph, ranks: Graph, munics: Graph):
    """
    Generate a persons dict from death records
    """
    casualties = {}
    for person in graph[:RDF.type:SCHEMA_WARSA.DeathRecord]:
        rank_uri = graph.value(person, SCHEMA_CAS.rank)

        given = str(graph.value(person, SCHEMA_WARSA.given_names, any=False))
        family = str(graph.value(person, SCHEMA_WARSA.family_name, any=False))
        rank = str(rank_uri) if rank_uri else None
        birth_place_uri = graph.value(person, SCHEMA_CAS.municipality_of_birth, any=False)

        cur_mun = str(munics.value(birth_place_uri, SCHEMA_CAS.current_municipality, any=False, default='')) or None

        war_mun = str(munics.value(birth_place_uri, SCHEMA_CAS.wartime_municipality, any=False, default='')) or None

        datebirth = get_date_value(graph.value(person, SCHEMA_WARSA.date_of_birth, default=''))
        datedeath = get_date_value(graph.value(person, SCHEMA_WARSA.date_of_death, default=''))

        try:
            rank_level = int(ranks.value(rank_uri, SCHEMA_ACTORS.level, any=False))
        except (TypeError, UniquenessError):
            rank_level = None

        casualty = {'person': None,
                    'rank': rank,
                    'rank_level': rank_level,
                    'given': given,
                    'family': re.sub(r'\(Ent\.\s*(.+)\)', r'\1', family),
                    'birth_place': list({cur_mun, war_mun} - {None}),
                    'birth_begin': datebirth,
                    'birth_end': datebirth,
                    'death_begin': datedeath,
                    'death_end': datedeath,
                    'activity_end': datedeath,
                    }
        casualties[str(person)] = casualty

    log.debug('Casualty person: {}'.format(casualty))

    return casualties


def link_warsa_municipality(warsa_munics: Graph, labels: list):
    '''
    Link municipality to Warsa

    >>> warsa_munics = Graph()
    >>> warsa_munics.add((URIRef('http://muni/Espoo'), SKOS.prefLabel, Literal("Espoo", lang='fi')))
    >>> warsa_munics.add((URIRef('http://muni/Turku'), SKOS.prefLabel, Literal("Turku")))
    >>> warsa_munics.add((URIRef('http://muni/Uusik'), SKOS.prefLabel, Literal("Uusikaarlepyyn mlk", lang='fi')))
    >>> link_warsa_municipality(warsa_munics, ['Espoo'])
    [rdflib.term.URIRef('http://muni/Espoo')]
    >>> link_warsa_municipality(warsa_munics, ['Turku'])
    [rdflib.term.URIRef('http://muni/Turku')]
    >>> link_warsa_municipality(warsa_munics, ['Uusikaarlepyyn kunta'])
    [rdflib.term.URIRef('http://muni/Uusik')]
    '''
    warsa_matches = []

    for lbl in labels:
        log.debug('Finding Warsa matches for municipality {}'.format(lbl))
        lbl = str(lbl).strip()
        munmap_match = MUNICIPALITY_MAPPING.get(lbl)
        if munmap_match:
            log.debug('Found predefined mapping for {}: {}'.format(lbl, munmap_match))
            warsa_matches += [munmap_match]
        else:
            warsa_matches += list(warsa_munics[:SKOS.prefLabel:Literal(lbl)])
            warsa_matches += list(warsa_munics[:SKOS.prefLabel:Literal(lbl, lang='fi')])

        if not warsa_matches:
            log.debug('Trying with mlk: {}'.format(lbl))
            warsa_matches += list(warsa_munics[:SKOS.prefLabel:Literal(lbl.replace(' kunta', ' mlk'))])
            warsa_matches += list(warsa_munics[:SKOS.prefLabel:Literal(lbl.replace(' kunta', ' mlk'), lang='fi')])

    if len(warsa_matches) == 0:
        log.info("Couldn't find Warsa URI for municipality {lbl}".format(lbl=labels))
    elif len(warsa_matches) == 1:
        match = warsa_matches[0]
        log.info('Found {lbl} municipality Warsa URI {s}'.format(lbl=labels, s=match))
    else:
        log.warning('Found multiple Warsa URIs for municipality {lbl}: {s}'.format(lbl=labels, s=warsa_matches))
        warsa_matches = []

    return warsa_matches


def link_municipalities(municipalities: Graph, warsa_endpoint: str, arpa_endpoint: str):
    """
    Link to Warsa municipalities.
    """
    warsa_munics = r.helpers.read_graph_from_sparql(warsa_endpoint,
                                                    graph_name='http://ldf.fi/warsa/places/municipalities')

    log.info('Using Warsa municipalities with {n} triples'.format(n=len(warsa_munics)))

    municipalities.remove((None, SCHEMA_CAS.current_municipality, None))
    municipalities.remove((None, SCHEMA_CAS.wartime_municipality, None))

    pnr_arpa = Arpa(arpa_endpoint)
    municipalities = link_to_pnr(municipalities, SCHEMA_CAS.current_municipality, None, pnr_arpa)['graph']

    for casualty_munic in list(municipalities[:RDF.type:SCHEMA_CAS.Municipality]):
        labels = list(municipalities[casualty_munic:SKOS.prefLabel:])

        warsa_match = None
        for warsa_match in link_warsa_municipality(warsa_munics, labels):
            municipalities.add((casualty_munic, SCHEMA_CAS.wartime_municipality, warsa_match))

        preferred = warsa_match or \
                    municipalities.value(casualty_munic, SCHEMA_CAS.current_municipality) or \
                    casualty_munic
        if preferred:
            municipalities.add((casualty_munic, SCHEMA_CAS.preferred_municipality, preferred))

    return municipalities


def link_units(graph: Graph, endpoint: str, arpa_url: str):
    """
    :param graph: Data graph object
    :param endpoint: SPARQL endpoint
    :param arpa_url: Arpa URL
    :return: Graph with links
    """

    def get_query_template():
        with open('SPARQL/units.sparql') as f:
            return f.read()

    COVER_NUMBER_SCORE_LIMIT = 20

    sparql = SPARQLWrapper(endpoint)
    query_template_unit_code = """
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?sub ?cover (GROUP_CONCAT(?label; separator=" || ") as ?labels) WHERE
        {{
            VALUES ?cover {{ "{cover}" }}
            ?sub <http://ldf.fi/schema/warsa/actors/covernumber> ?cover .
            ?sub skos:prefLabel|skos:altLabel ?label .
        }} GROUP BY ?sub ?cover
    """
    temp_graph = Graph()
    unit_code_links = Graph()

    ngram_arpa = Arpa(arpa_url, retries=10, wait_between_tries=6)

    unit_codes = graph.objects(None, SCHEMA_CAS.unit_code)

    sparql.method = 'POST'
    sparql.setQuery(query_template_unit_code.format(cover='" "'.join(unit_codes)))
    sparql.setReturnFormat(JSON)
    results = query_sparql(sparql)

    units = defaultdict(list)
    for unit in results['results']['bindings']:
        units[unit['cover']['value']].append(unit)

    for person in graph[:RDF.type:SCHEMA_WARSA.DeathRecord]:
        cover = graph.value(person, SCHEMA_CAS.unit_code)

        best_score = -1
        # LINK DEATH RECORDS BASED ON COVER NUMBER IF IT EXISTS
        if cover:
            cover = str(cover)
            person_unit = str(graph.value(person, SCHEMA_CAS.unit_literal))
            best_unit = None
            best_labels = None

            for result in units[cover]:
                if 'sub' not in result:
                    # This can happen because of GROUP_CONCAT
                    log.warning('Unknown cover number {cover}.'.format(cover=cover))
                    continue
                warsa_unit = result["sub"]["value"]
                unit_labels = result["labels"]["value"].split(' || ')
                score = max(fuzz.ratio(unit, person_unit) for unit in unit_labels)
                if score > best_score:
                    best_score = score
                    best_labels = unit_labels
                    best_unit = warsa_unit

            if best_score >= COVER_NUMBER_SCORE_LIMIT and best_unit:
                log.info('Found unit {unit} for {pers} by cover number with score {score}.'.
                         format(pers=person, unit=best_unit, score=best_score))
                unit_code_links.add((person, SCHEMA_CAS.unit, URIRef(best_unit)))

            else:
                log.warning('Skipping suspected erroneus unit for {unit}/{cover} with labels {lbls} and score {score}.'.
                            format(unit=person_unit, cover=cover, lbls=sorted(set(best_labels or [])), score=best_score))

        # NO COVER NUMBER, ADD RELATED_PERIOD FOR LINKING WITH WARSA-LINKERS
        if not cover or best_score < COVER_NUMBER_SCORE_LIMIT:
            death_time = str(graph.value(person, SCHEMA_WARSA.date_of_death))
            if death_time < '1941-06-25':
                temp_graph.add((person, URIRef('http://ldf.fi/schema/warsa/events/related_period'),
                                URIRef('http://ldf.fi/warsa/conflicts/WinterWar')))

            unit = preprocessor(str(graph.value(person, SCHEMA_CAS.unit_literal)))
            ngrams = ngram_arpa.get_candidates(unit)
            combined = combine_values(ngrams['results'])
            temp_graph.add((person, SCHEMA_CAS.candidate, Literal(combined)))

    # LINK DEATH RECORDS WITHOUT COVER NUMBER

    log.info('Linking the found candidates')
    arpa = ArpaMimic(get_query_template(), endpoint, retries=10, wait_between_tries=6)
    unit_links = process_graph(temp_graph, SCHEMA_CAS.unit, arpa,
                               progress=True,
                               validator=Validator(temp_graph),
                               new_graph=True,
                               source_prop=SCHEMA_CAS.candidate)['graph']
    return unit_links + unit_code_links


def link_casualties(input_graph, endpoint, munics):
    ranks = r.read_graph_from_sparql(endpoint, "http://ldf.fi/warsa/ranks")
    munics = Graph().parse(munics, format=guess_format(munics))

    random.seed(42)  # Initialize randomization to create deterministic results

    return link_persons(input_graph, endpoint, _generate_casualties_dict(input_graph, ranks, munics))


def main():
    argparser = argparse.ArgumentParser(description="Casualty linking tasks", fromfile_prefix_chars='@')

    argparser.add_argument("task", help="Linking task to perform",
                           choices=["ranks", "persons", "municipalities", "units", "occupations"])
    argparser.add_argument("input", help="Input RDF file")
    argparser.add_argument("output", help="Output file location")
    argparser.add_argument("--loglevel", default='INFO', help="Logging level, default is INFO.",
                           choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    argparser.add_argument("--logfile", default='tasks.log', help="Logfile")
    argparser.add_argument("--endpoint", default='http://ldf.fi/warsa/sparql', help="SPARQL Endpoint")
    argparser.add_argument("--munics", default='output/municipalities.ttl', help="Municipalities RDF file")
    argparser.add_argument("--arpa", type=str, help="ARPA instance URL for linking")

    args = argparser.parse_args()

    log_handler = logging.FileHandler(args.logfile)
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    log.addHandler(log_handler)
    log.setLevel(args.loglevel)

    input_graph = Graph()
    input_graph.parse(args.input, format=guess_format(args.input))

    if args.task == 'ranks':
        log.info('Linking ranks')
        bind_namespaces(link_ranks(input_graph, args.endpoint, CASUALTY_MAPPING['SOTARVO']['uri'], SCHEMA_CAS.rank,
                                   SCHEMA_WARSA.DeathRecord)).serialize(args.output, format=guess_format(args.output))

    elif args.task == 'persons':
        log.info('Linking persons')
        bind_namespaces(link_casualties(input_graph, args.endpoint, args.munics)) \
            .serialize(args.output, format=guess_format(args.output))

    elif args.task == 'municipalities':
        log.info('Linking municipalities')
        bind_namespaces(link_municipalities(input_graph, args.endpoint, args.arpa)) \
            .serialize(args.output, format=guess_format(args.output))

    elif args.task == 'units':
        log.info('Linking units')
        bind_namespaces(link_units(input_graph, args.endpoint, args.arpa)) \
            .serialize(args.output, format=guess_format(args.output))

    elif args.task == 'occupations':
        log.info('Linking occupations')
        bind_namespaces(link_occupations(input_graph, args.endpoint, CASUALTY_MAPPING['AMMATTI']['uri'],
                                         BIOC.has_occupation, SCHEMA_WARSA.DeathRecord)) \
            .serialize(args.output, format=guess_format(args.output))


if __name__ == '__main__':
    main()

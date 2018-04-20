#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""Casualty linking tasks"""

import argparse
import logging
import re
import time
from datetime import datetime
from collections import defaultdict
from json import load

from SPARQLWrapper import SPARQLWrapper, JSON
from dedupe import RecordLink, StaticRecordLink, trainingDataLink, consoleLabel
from dedupe.api import RecordLinkMatching
from jellyfish import jaro_winkler
from fuzzywuzzy import fuzz
from rdflib import Graph, URIRef, Literal, BNode, RDF
from rdflib.exceptions import UniquenessError
from rdflib.util import guess_format

from arpa_linker.arpa import ArpaMimic, process_graph, Arpa, combine_values
from namespaces import SKOS, CRM, BIOC, SCHEMA_CAS, SCHEMA_WARSA, bind_namespaces, FOAF, SCHEMA_ACTORS
import rdf_dm as r
from sotasampo_helpers.arpa import link_to_pnr
from warsa_linkers.units import preprocessor, Validator
from warsa_linkers.occupations import link_occupations

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

                # TODO: Update reifications
            else:
                log.warning('No match found for %s: %s' % (prop_str, value))

    return target_graph


def link_ranks(graph, endpoint):
    """
    Link military ranks in graph.

    :param graph: Data in RDFLib Graph object
    :param endpoint: Endpoint to query military ranks from
    :return: RDFLib Graph with updated links
    """

    # TODO: Move to Warsa-linkers

    def preprocess(literal):
        value = str(literal).strip()
        return rank_mapping[value] if value in rank_mapping else value

    rank_mapping = {
        'aliluutn.': 'aliluutnantti',
        'alisot.ohj.': 'Alisotilasohjaaja',
        'alisot.virk.': 'Alisotilasvirkamies',
        'asemest.': 'asemestari',
        'au.opp.': 'aliupseerioppilas',
        'el.lääk.ev.luutn.': 'Eläinlääkintäeverstiluutnantti',
        'el.lääk.kapt.': 'Eläinlääkintäkapteeni',
        'el.lääk.maj.': 'Eläinlääkintämajuri',
        'GRUF': 'Gruppenführer',
        'II lk. nstm.': 'Toisen luokan nostomies',
        'ins.kapt.': 'Insinöörikapteeni',
        'ins.kapt.luutn.': 'Insinöörikapteeniluutnantti',
        'ins.luutn.': 'Insinööriluutnantti',
        'ins.maj.': 'Insinöörimajuri',
        'is-mies': 'Ilmasuojelumies',
        'is.stm.': 'Ilmasuojelusotamies',
        'kapt.luutn.': 'kapteeniluutnantti',
        'kom.kapt.': 'komentajakapteeni',
        'lääk.alikers.': 'Lääkintäalikersantti',
        'lääk.kapt.': 'Lääkintäkapteeni',
        'lääk.kers.': 'Lääkintäkersantti',
        'lääk.korpr.': 'Lääkintäkorpraali',
        'lääk.lotta': 'Lääkintälotta',
        'lääk.maj.': 'Lääkintämajuri',
        'lääk.stm.': 'Lääkintäsotamies',
        'lääk.vääp.': 'Lääkintävääpeli',
        'lääk.virk.': 'Lääkintävirkamies',
        'lentomek.': 'Lentomekaanikko',
        'linn.työnjoht.': 'Linnoitustyönjohtaja',
        'merivart.': 'Merivartija',
        'mus.luutn.': 'Musiikkiluutnantti',
        'OSTUF': 'Obersturmführer',
        'paik.pääll.': 'Paikallispäällikkö',
        'pans.jääk.': 'Panssarijääkäri',
        'pursim.': 'pursimies',
        'rajavääp.': 'rajavääpeli',
        'RTTF': 'Rottenführer',
        'sair.hoit.': 'Sairaanhoitaja',
        'sair.hoit.opp.': 'Sairaanhoitajaoppilas',
        'SCHTZ': 'Schütze',
        'sivili': 'siviili',
        'sk.korpr.': 'Suojeluskuntakorpraali',
        'sot.alivirk.': 'Sotilasalivirkamies',
        'sot.inval.': 'Sotainvalidi',
        'sot.kotisisar': 'Sotilaskotisisar',
        'sot.past.': 'Sotilaspastori',
        'sot.pka': 'Sotilaspoika',
        'sot.poika': 'Sotilaspoika',
        'sotilasmest.': 'Sotilasmestari',
        'STRM': 'Sturmmann',
        'ups.kok.': 'Upseerikokelas',
        'ups.opp.': 'Upseerioppilas',
        'USCHA': 'Unterscharführer',
        'USTUF': 'Untersturmführer',
        'ylihoit.': 'Ylihoitaja',
        'ylivääp.': 'Ylivääpeli',
    }

    # Works in Fuseki because SAMPLE returns the first value and text:query sorts by score
    query = """
        PREFIX text: <http://jena.apache.org/text#>
        SELECT ?rank (SAMPLE(?id_) AS ?id) {{
            VALUES ?rank {{ "{ranks}" }}
            GRAPH <http://ldf.fi/warsa/ranks> {{
                ?id_ text:query ?rank .
                ?id_ a <http://ldf.fi/schema/warsa/Rank> .
            }}
        }} GROUP BY ?rank
    """

    rank_literals = set(map(preprocess, graph.objects(None, SCHEMA_CAS.rank_literal)))

    sparql = SPARQLWrapper(endpoint)
    sparql.method = 'POST'
    sparql.setQuery(query.format(ranks='" "'.join(rank_literals)))
    sparql.setReturnFormat(JSON)
    results = _query_sparql(sparql)

    rank_links = Graph()
    ranks = {}
    for rank in results['results']['bindings']:
        ranks[rank['rank']['value']] = rank['id']['value']

    for person in graph[:RDF.type:SCHEMA_WARSA.DeathRecord]:
        rank_literal = str(graph.value(person, SCHEMA_CAS.rank_literal))
        if rank_literal in ranks:
            rank_links.add((person, SCHEMA_CAS.rank, URIRef(ranks[rank_literal])))

    return rank_links


def _generate_casualties_dict(graph: Graph, ranks: Graph, munics: Graph):
    '''
    Generate a persons dict from death records
    '''
    casualties = {}
    for person in graph[:RDF.type:SCHEMA_WARSA.DeathRecord]:
        rank_uri = graph.value(person, SCHEMA_CAS.rank)

        given = str(graph.value(person, SCHEMA_WARSA.given_names, any=False))
        family = str(graph.value(person, SCHEMA_WARSA.family_name, any=False))
        rank = str(rank_uri) if rank_uri else None
        birth_place_uri = graph.value(person, SCHEMA_CAS.municipality_of_birth, any=False)
        cur_mun = munics.value(birth_place_uri, SCHEMA_CAS.current_municipality, any=False)
        war_mun = munics.value(birth_place_uri, SCHEMA_CAS.wartime_municipality, any=False)

        datebirth = graph.value(person, SCHEMA_WARSA.date_of_birth)
        datedeath = graph.value(person, SCHEMA_WARSA.date_of_death)

        try:
            rank_level = int(ranks.value(rank_uri, SCHEMA_ACTORS.level, any=False))
        except (TypeError, UniquenessError):
            rank_level = None

        casualty = {'person': None,
                    'rank': rank,
                    'rank_level': rank_level,
                    'given': given,
                    'family': family,
                    'birth_place': list({cur_mun, war_mun} - {None}),
                    'birth_begin': datebirth,
                    'birth_end': datebirth,
                    'death_begin': datedeath,
                    'death_end': datedeath,
                    }
        casualties[str(person)] = casualty

    return casualties


def _generate_persons_dict(endpoint):
    '''
    Generate a persons dict from person instances
    '''

    def get_person_query():
        with open('SPARQL/warsa_persons.sparql') as f:
            return f.read()

    sparql = SPARQLWrapper(endpoint)
    sparql.method = 'POST'
    sparql.setQuery(get_person_query())
    sparql.setReturnFormat(JSON)
    results = _query_sparql(sparql)

    persons = defaultdict(dict)
    for person_row in results['results']['bindings']:
        person = person_row['person']['value']
        given = person_row['given']['value']
        family = person_row['family']['value']
        rank = person_row.get('rank', {}).get('value')
        rank_level = person_row.get('rank_level', {}).get('value')
        birth_place = person_row.get('birth_place', {}).get('value')
        birth_begin = person_row.get('birth_begin', {}).get('value')
        birth_end = person_row.get('birth_end', {}).get('value')
        death_begin = person_row.get('death_begin', {}).get('value')
        death_end = person_row.get('death_end', {}).get('value')

        person_dict = {
            'person': person,
            'given': given,
            'family': family,
            'rank': rank,
            'rank_level': int(rank_level) if rank_level else None,
            'birth_place': [birth_place],
            'birth_begin': birth_begin,
            'birth_end': birth_end,
            'death_begin': death_begin,
            'death_end': death_end,
        }
        persons[person] = person_dict

    return persons


def get_person_links(casualties: dict, persons: dict, links_json_file='output/person_links.json'):
    '''
    Read person links from a JSON file generated with generate_training_data.sh
    '''
    with open(links_json_file, 'r') as fp:
        links = load(fp)['results']['bindings']

    num_links = 0

    for link in links:
        cas = link['doc']['value']
        per = link['person']['value']
        if cas in casualties and per in persons:
            casualties[cas].update({'person': per})
            num_links += 1

    return casualties, num_links


def intersection_comparator(field_1, field_2):
    if field_1 and field_2:
        if set(field_1) & set(field_2):
            return 1
        else:
            return 0


def link_persons(graph, endpoint, munics_file):
    """
    Link military persons in graph.

    :param graph: Data in RDFLib Graph object
    :param endpoint: Endpoint to query persons from
    :return: RDFLib Graph with updated links
    """

    data_fields = [
        {'field': 'given', 'type': 'String'},
        {'field': 'family', 'type': 'String'},
        {'field': 'birth_place', 'type': 'Custom', 'comparator': intersection_comparator, 'has missing': True},
        {'field': 'birth_begin', 'type': 'DateTime', 'has missing': True},
        {'field': 'birth_end', 'type': 'DateTime', 'has missing': True},
        {'field': 'death_begin', 'type': 'DateTime', 'has missing': True},
        {'field': 'death_end', 'type': 'DateTime', 'has missing': True},
        {'field': 'rank', 'type': 'Exact', 'has missing': True},
        {'field': 'rank_level', 'type': 'Price', 'has missing': True},
    ]

    ranks = r.read_graph_from_sparql(endpoint, "http://ldf.fi/warsa/ranks")

    munics = Graph().parse(munics_file, format=guess_format(munics_file))

    cas_data = _generate_casualties_dict(graph, ranks, munics)
    print(cas_data['http://ldf.fi/warsa/casualties/p11840'])
    print(len(cas_data))

    per_data = _generate_persons_dict(endpoint)
    print(per_data['http://ldf.fi/warsa/actors/person_50'])
    print(len(per_data))

    cas_data, num_links = get_person_links(cas_data, per_data)

    log.info('Got {} person links as training data'.format(num_links))

    link_graph = Graph()
    if num_links:
        linker = RecordLink(data_fields)
        linker.sample(cas_data, per_data, 15000)
        linker.markPairs(trainingDataLink(cas_data, per_data, common_key='person'))
        linker.train()

        with open('output/training_data.json', 'w') as fp:
            linker.writeTraining(fp)

        links = linker.match(cas_data, per_data)
        log.info('Found {} person links'.format(len(links)))

        for link in links:
            cas = link[0][0]
            per = link[0][1]
            log.debug('Found person link: {}  <-->  {} (confidence: {})'.format(cas, per, link[1]))
            link_graph.add((URIRef(cas), CRM.P70_documents, URIRef(per)))

    return link_graph


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
            if retry < 10:
                log.error('Malformed result for query {p_uri}, retrying in 1 second...'.format(
                    p_uri=sparql_obj.queryString))
                retry += 1
                time.sleep(1)
            else:
                raise
    log.debug('Got results {res} for query {q}'.format(res=results, q=sparql_obj.queryString))
    return results


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
    results = _query_sparql(sparql)

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
                            format(unit=person_unit, cover=cover, lbls=best_labels, score=best_score))

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
        bind_namespaces(link_ranks(input_graph, args.endpoint)).serialize(args.output, format=guess_format(args.output))

    elif args.task == 'persons':
        log.info('Linking persons')
        bind_namespaces(link_persons(input_graph, args.endpoint, args.munics)) \
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
        bind_namespaces(link_occupations(input_graph, args.endpoint, SCHEMA_CAS.occupation_literal,
                                         BIOC.has_occupation, SCHEMA_WARSA.DeathRecord)) \
            .serialize(args.output, format=guess_format(args.output))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""Person generator"""

import argparse
import logging

from rdf_dm import read_graph_from_sparql
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.util import guess_format

from namespaces import SKOS, CRM, SCHEMA_CAS, SCHEMA_WARSA, bind_namespaces, DCT, FOAF

NARC_SOURCE = URIRef('http://ldf.fi/warsa/sources/source9')


def get_local_id(casualty: URIRef):
    return str(casualty).split('/')[-1]


def generate_event(graph: Graph, casualty: URIRef, person: URIRef, event_type: URIRef, event_prefix: str,
                   date_prop: URIRef, place_prop: URIRef, relation_prop: URIRef, munics: Graph):
    log.debug('Generating event {} {} {} {} {} {} {}'.format(casualty, person, event_type, event_prefix,
                                                             date_prop, place_prop, relation_prop))
    event = Graph()
    cas_local_id = get_local_id(casualty)
    event_uri = URIRef('http://ldf.fi/warsa/events/{prefix}{id}'.format(prefix=event_prefix, id=cas_local_id))

    event.add((event_uri, RDF.type, event_type))
    event.add((event_uri, relation_prop, person))
    event.add((event_uri, DCT.source, NARC_SOURCE))

    if place_prop:
        place = graph.value(subject=casualty, predicate=place_prop)

        if place:
            place_ws = munics.value(place, SCHEMA_CAS.preferred_municipality)
            event.add((event_uri, CRM.P7_took_place_at, place_ws))

    if date_prop:
        date = graph.value(subject=casualty, predicate=date_prop)
        if date:
            timespan_uri = URIRef(
                'http://ldf.fi/warsa/events/times/{prefix}{id}'.format(prefix=event_prefix, id=cas_local_id))

            event.add((event_uri, CRM['P4_has_time-span'], timespan_uri))

            # TODO: dateTimes
            event.add((timespan_uri, CRM.P82a_begin_of_the_begin, date))
            event.add((timespan_uri, CRM.P82b_end_of_the_end, date))

    return event, event_uri


def generate_birth(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph):
    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Birth, 'birth_',
                                      SCHEMA_WARSA.date_of_birth, SCHEMA_CAS.municipality_of_birth,
                                      CRM.P98_brought_into_life, munics)

    lbl_fi = Literal('{person} syntyi'.format(person=person_name), lang='fi')
    lbl_en = Literal('{person} was born'.format(person=person_name), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_death(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph):
    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Death, 'death_',
                                      SCHEMA_WARSA.date_of_death, SCHEMA_CAS.municipality_of_death,
                                      CRM.P100_was_death_of, munics)

    lbl_fi = Literal('{person} kuoli'.format(person=person_name), lang='fi')
    lbl_en = Literal('{person} died'.format(person=person_name), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_disappearance(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph):
    date = graph.value(casualty, SCHEMA_CAS.date_of_going_mia)
    mun = graph.value(casualty, SCHEMA_CAS.municipality_of_going_mia)
    place = graph.value(casualty, SCHEMA_CAS.place_of_going_mia_literal)
    if not (date or mun or place):
        return Graph()

    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Disappearing, 'disappear_cas_',
                                      SCHEMA_WARSA.date_of_going_mia, SCHEMA_CAS.municipality_of_going_mia,
                                      CRM.P11_had_participant, munics)

    if place:
        event.add((event_uri, SCHEMA_WARSA.place_string, place))

    lbl_fi = Literal('{person} katosi'.format(person=person_name), lang='fi')
    lbl_en = Literal('{person} went missing in action'.format(person=person_name), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_wounding(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph):
    date = graph.value(casualty, SCHEMA_CAS.date_of_wounding)
    mun = graph.value(casualty, SCHEMA_CAS.municipality_of_wounding)
    place = graph.value(casualty, SCHEMA_CAS.place_of_wounding)
    if not (date or mun or place):
        return Graph()

    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Wounding, 'wound_cas_',
                                      SCHEMA_WARSA.date_of_wounding, SCHEMA_CAS.municipality_of_wounding,
                                      CRM.P11_had_participant, munics)

    if place:
        event.add((event_uri, SCHEMA_WARSA.place_string, place))

    lbl_fi = Literal('{person} haavoittui'.format(person=person_name), lang='fi')
    lbl_en = Literal('{person} was wounded'.format(person=person_name), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_promotion(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph, ranks: Graph):
    rank = graph.value(casualty, SCHEMA_CAS.rank)
    if not rank:
        return Graph()

    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Promotion, 'promotion_cas_',
                                      None, None, CRM.P11_had_participant, munics)

    event.add((event_uri, URIRef('http://ldf.fi/warsa/actors/hasRank'), rank))

    rank_literal = graph.value(casualty, SCHEMA_CAS.rank_literal)
    rank_labels = list(ranks.objects(rank, SKOS.prefLabel))
    rank_fi = next([lit for lit in rank_labels if lit.language == 'fi'], None) or rank_literal
    rank_en = next([lit for lit in rank_labels if lit.language == 'en'], rank_fi) or rank_literal
    lbl_fi = Literal('{person} ylennettiin arvoon {rank}'.format(person=person_name, rank=rank_fi), lang='fi')
    lbl_en = Literal('{person} was promoted to {rank}'.format(person=person_name, rank=rank_en), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_join(graph: Graph, casualty: URIRef, person: URIRef, person_name: str, munics: Graph):
    unit = graph.value(casualty, SCHEMA_CAS.unit)
    if not unit:
        return Graph()

    event, event_uri = generate_event(graph, casualty, person, SCHEMA_WARSA.Promotion, 'joining_cas_',
                                      None, None, CRM.P143_joined, munics)

    event.add((event_uri, CRM.P144_joined_with, unit))

    unit_literal = graph.value(casualty, SCHEMA_CAS.unit_literal)
    lbl_fi = Literal('{person} liittyi joukko-osastoon {unit}'.format(person=person_name, unit=unit_literal), lang='fi')
    lbl_en = Literal('{person} joined {unit}'.format(person=person_name, unit=unit_literal), lang='en')

    event.add((event_uri, SKOS.prefLabel, lbl_fi))
    event.add((event_uri, SKOS.prefLabel, lbl_en))

    return event


def generate_person(graph: Graph, casualty: URIRef):
    person = Graph()

    person_uri = URIRef('http://ldf.fi/warsa/actors/person_{}'.format(get_local_id(casualty)))

    log.debug('Generating person instance for {}'.format(person_uri))

    family_name = graph.value(casualty, SCHEMA_WARSA.family_name)
    given_names = graph.value(casualty, SCHEMA_WARSA.given_names)
    lbl = Literal('{gn} {fn}'.format(gn=given_names, fn=family_name))

    person.add((person_uri, FOAF.familyName, family_name))
    person.add((person_uri, FOAF.firstName, given_names))
    person.add((person_uri, FOAF.givenName, given_names))
    person.add((person_uri, SKOS.prefLabel, lbl))
    person.add((person_uri, DCT.source, NARC_SOURCE))
    person.add((person_uri, CRM.P70i_is_documented_in, casualty))

    return person, person_uri, lbl


def generate_persons(graph: Graph, municipalities: Graph, ranks: Graph):
    persons = Graph()
    promotions = Graph()
    joinings = Graph()
    births = Graph()
    deaths = Graph()
    disappearances = Graph()
    woundings = Graph()

    for casualty in graph.subjects(RDF.type, SCHEMA_WARSA.DeathRecord):
        if graph.value(casualty, CRM.P70_documents):
            continue  # Do not generate if the casualty is already linked to a person instance

        person, person_uri, person_name = generate_person(graph, casualty)

        persons += person

        births += generate_birth(graph, casualty, person_uri, person_name, municipalities)
        deaths += generate_death(graph, casualty, person_uri, person_name, municipalities)
        joinings += generate_join(graph, casualty, person_uri, person_name, municipalities)
        promotions += generate_promotion(graph, casualty, person_uri, person_name, municipalities, ranks)
        woundings += generate_wounding(graph, casualty, person_uri, person_name, municipalities)
        disappearances += generate_disappearance(graph, casualty, person_uri, person_name, municipalities)

    return {
        'persons': persons,
        'promotions': promotions,
        'joinings': joinings,
        'births': births,
        'deaths': deaths,
        'disappearances': disappearances,
        'woundings': woundings
    }


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description=__doc__, fromfile_prefix_chars='@')

    argparser.add_argument("input", help="Input RDF file")
    argparser.add_argument("municipalities", help="Municipalities RDF file")
    argparser.add_argument("endpoint", help="SPARQL endpoint to get ranks graph from")
    argparser.add_argument("output", help="Output file prefix")
    argparser.add_argument("--loglevel", default='INFO', help="Logging level, default is INFO.",
                           choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    argparser.add_argument("--logfile", default='tasks.log', help="Logfile")

    args = argparser.parse_args()

    logging.basicConfig(filename=args.logfile,
                        filemode='a',
                        level=getattr(logging, args.loglevel),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log = logging.getLogger(__name__)

    input_graph = Graph().parse(args.input, format=guess_format(args.input))

    munics = Graph().parse(args.municipalities, format=guess_format(args.input))

    ranks = read_graph_from_sparql(args.endpoint, "http://ldf.fi/warsa/ranks")

    for key, graph in generate_persons(input_graph, munics, ranks).items():
        bind_namespaces(graph).serialize('{prefix}{key}.ttl'.format(prefix=args.output, key=key), format='turtle')

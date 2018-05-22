#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Tests for data conversion

To run all tests (including doctests) you can use for example nose: nosetests --with-doctest
"""
import datetime
import unittest
from pprint import pprint, pformat

from rdflib import Graph, URIRef, Literal, RDF

from linker import _generate_casualties_dict
from namespaces import RANKS_NS, SKOS, SCHEMA_ACTORS, MUNICIPALITIES, SCHEMA_CAS, SCHEMA_WARSA


class TestPersonLinking(unittest.TestCase):
    maxDiff = None

    ranks = Graph()
    ranks.add((RANKS_NS.Korpraali, SKOS.prefLabel, Literal('Korpraali', lang='fi')))
    ranks.add((RANKS_NS.Kapteeni, SKOS.prefLabel, Literal('Kapteeni', lang='fi')))
    ranks.add((RANKS_NS.Korpraali, SCHEMA_ACTORS.level, Literal(3)))
    ranks.add((RANKS_NS.Kapteeni, SCHEMA_ACTORS.level, Literal(11)))

    munics = Graph()
    munics.add((MUNICIPALITIES.k1903, SCHEMA_CAS.current_municipality, URIRef('http://ldf.fi/pnr/P_10746999')))
    munics.add((MUNICIPALITIES.k1903, SCHEMA_CAS.preferred_municipality,
                URIRef('http://ldf.fi/warsa/places/municipalities/m_place_21')))

    def test_generate_prisoners_dict(self):
        expected = {
            'foo': {'activity_end': '1941-12-23',
                    'birth_begin': '1906-12-23',
                    'birth_end': '1906-12-23',
                    'birth_place': ['http://ldf.fi/pnr/P_10746999'],
                    'death_begin': '1941-12-23',
                    'death_end': '1941-12-23',
                    'family': 'Heino',
                    'given': 'Eino Ilmari',
                    'person': None,
                    'rank': 'http://ldf.fi/schema/warsa/actors/ranks/Korpraali',
                    'rank_level': 3,
                    'unit': None}
        }

        g = Graph()
        p = URIRef('foo')
        g.add((p, RDF.type, SCHEMA_WARSA.DeathRecord))
        g.add((p, SCHEMA_CAS.rank, RANKS_NS.Korpraali))
        g.add((p, SCHEMA_WARSA.given_names, Literal("Eino Ilmari")))
        g.add((p, SCHEMA_WARSA.family_name, Literal("Heino")))
        g.add((p, SCHEMA_CAS.municipality_of_birth, MUNICIPALITIES.k1903))
        g.add((p, SCHEMA_WARSA.date_of_birth, Literal(datetime.date(1906, 12, 23))))
        g.add((p, SCHEMA_WARSA.date_of_death, Literal(datetime.date(1941, 12, 23))))
        pd = _generate_casualties_dict(g, self.ranks, self.munics)

        self.assertEqual(expected, pd, pformat(pd))

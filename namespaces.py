#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Define common RDF namespaces
"""
from rdflib import Namespace, RDF, RDFS, XSD, Graph

CIDOC = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
DC = Namespace('http://purl.org/dc/terms/')
FOAF = Namespace('http://xmlns.com/foaf/0.1/')
SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')
BIOC = Namespace('http://ldf.fi/schema/bioc/')

DATA_NS = Namespace('http://ldf.fi/warsa/casualties/')
SCHEMA_NS = Namespace('http://ldf.fi/schema/warsa/casualties/')
WARSA_NS = Namespace('http://ldf.fi/schema/warsa/')
EVENTS_NS = Namespace('http://ldf.fi/warsa/events/')

CEMETERY_NS = Namespace('http://ldf.fi/warsa/places/cemeteries/')
MOTHER_TONGUE_NS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/aeidinkieli/')
MARITAL_NS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/siviilisaeaety/')
GENDER_NS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/sukupuoli/')
PERISHING_CLASSES_NS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/')

KANSALAISUUS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/')
KANSALLISUUS = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/')
KUNNAT = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kunnat/')
SOTILASARVO = Namespace('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/')
MENEHTYMISLUOKKA = Namespace('http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/')
NARCS = Namespace('http://ldf.fi/schema/narc-menehtyneet1939-45/')

def bind_namespaces(graph: Graph):
    graph.bind("c", "http://ldf.fi/warsa/casualties/")
    graph.bind("cs", "http://ldf.fi/schema/warsa/casualties/")
    graph.bind("skos", "http://www.w3.org/2004/02/skos/core#")
    graph.bind("cidoc", 'http://www.cidoc-crm.org/cidoc-crm/')
    graph.bind("bioc", 'http://ldf.fi/schema/bioc/')
    graph.bind("dct", 'http://purl.org/dc/terms/')

    graph.bind("narcs", NARCS)

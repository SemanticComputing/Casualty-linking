#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Define common RDF namespaces
"""
from rdflib import Namespace, Graph

CRM = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
DCT = Namespace('http://purl.org/dc/terms/')
FOAF = Namespace('http://xmlns.com/foaf/0.1/')
SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')
BIOC = Namespace('http://ldf.fi/schema/bioc/')

DATA_CAS = Namespace('http://ldf.fi/warsa/casualties/')
SCHEMA_CAS = Namespace('http://ldf.fi/schema/warsa/casualties/')
SCHEMA_WARSA = Namespace('http://ldf.fi/schema/warsa/')
ACTORS = Namespace('http://ldf.fi/warsa/actors/')
SCHEMA_ACTORS = Namespace('http://ldf.fi/schema/warsa/actors/')

CEMETERIES = Namespace('http://ldf.fi/warsa/places/cemeteries/')
MOTHER_TONGUES = Namespace('http://ldf.fi/warsa/mother_tongues/')
MARITAL_STATUSES = Namespace('http://ldf.fi/warsa/marital_statuses/')
GENDERS = Namespace('http://ldf.fi/warsa/genders/')
PERISHING_CLASSES = Namespace('http://ldf.fi/warsa/perishing_categories/')

CITIZENSHIPS = Namespace('http://ldf.fi/warsa/citizenships/')
NATIONALITIES = Namespace('http://ldf.fi/warsa/nationalities/')
MUNICIPALITIES = Namespace('http://ldf.fi/warsa/casualties/municipalities/')


def bind_namespaces(graph: Graph):
    graph.bind("bioc", BIOC)
    graph.bind("dct", DCT)
    graph.bind("crm", CRM)
    graph.bind("skos", SKOS)
    graph.bind("foaf", FOAF)

    graph.bind("wsch", SCHEMA_WARSA)
    graph.bind("wcsc", SCHEMA_CAS)
    graph.bind("wca", DATA_CAS)
    graph.bind("wac", ACTORS)

    graph.bind("wcp", PERISHING_CLASSES)
    graph.bind("wcm", MUNICIPALITIES)
    graph.bind("wcg", GENDERS)
    graph.bind("wcc", CITIZENSHIPS)
    graph.bind("wcn", NATIONALITIES)
    graph.bind("wct", MOTHER_TONGUES)
    graph.bind("wcs", MARITAL_STATUSES)
    graph.bind("wce", CEMETERIES)

    return graph
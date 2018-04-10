#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Define common RDF namespaces
"""
from rdflib import Namespace, RDF, RDFS, XSD, Graph

CRM = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
DC = Namespace('http://purl.org/dc/terms/')
FOAF = Namespace('http://xmlns.com/foaf/0.1/')
SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')
BIOC = Namespace('http://ldf.fi/schema/bioc/')

DATA_CAS = Namespace('http://ldf.fi/warsa/casualties/')
SCHEMA_CAS = Namespace('http://ldf.fi/schema/warsa/casualties/')
SCHEMA_WARSA = Namespace('http://ldf.fi/schema/warsa/')

CEMETERIES = Namespace('http://ldf.fi/warsa/places/cemeteries/')
MOTHER_TONGUES = Namespace('http://ldf.fi/warsa/mother_tongues/')
MARITAL_STATUSES = Namespace('http://ldf.fi/warsa/marital_statuses/')
GENDERS = Namespace('http://ldf.fi/warsa/genders/')
PERISHING_CLASSES = Namespace('http://ldf.fi/warsa/perishing_classes/')

CITIZENSHIPS = Namespace('http://ldf.fi/warsa/citizenships/')
NATIONALITIES = Namespace('http://ldf.fi/warsa/nationalities/')
MUNICIPALITIES = Namespace('http://ldf.fi/warsa/casualties/municipalities/')


def bind_namespaces(graph: Graph):
    graph.bind("c", "http://ldf.fi/warsa/casualties/")
    graph.bind("cs", "http://ldf.fi/schema/warsa/casualties/")
    graph.bind("skos", "http://www.w3.org/2004/02/skos/core#")
    graph.bind("cidoc", 'http://www.cidoc-crm.org/cidoc-crm/')
    graph.bind("bioc", 'http://ldf.fi/schema/bioc/')
    graph.bind("dct", 'http://purl.org/dc/terms/')

    graph.bind("narcs", SCHEMA_CAS)

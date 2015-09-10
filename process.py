#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing, validating and fixing some errors in "Casualties during the Finnish wars 1939–1945"
(Suomen sodissa 1939–1945 menehtyneet) RDF dataset. Input data downloadable from LDF.FI linked data portal:
http://www.ldf.fi/dataset/narc-menehtyneet1939-45

Copyright (c) 2015 Mikko Koho
"""

import argparse
import os

import re

import joblib
from rdflib import *
import rdflib

import rdf_dm as r

INPUT_FILE_DIRECTORY = 'data/'
OUTPUT_FILE_DIRECTORY = 'data/new/'

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-r', action='store_true', help='Reload RDF graphs, instead of using pickle object')
args = parser.parse_args()

reload = args.r

if not reload:
    try:
        surma = joblib.load(INPUT_FILE_DIRECTORY + 'surma.pkl')
        print('Parsed {len} triples from pickle object.'.format(len=len(surma)))
    except IOError:
        reload = True

if reload:
    print('Processing Sotasurma RDF data.')
    surma = rdflib.Graph()
    input_dir = '{base}/{dir}'.format(base=os.getcwd(), dir=INPUT_FILE_DIRECTORY)
    for f in os.listdir(input_dir):
        if f.endswith('.ttl'):
            surma.parse(input_dir + f, format='turtle')
    print('Parsed {len} triples.'.format(len=len(surma)))
    joblib.dump(surma, INPUT_FILE_DIRECTORY + 'surma.pkl')
    print('Wrote graph to pickle object.')

# TODO: Fix graveyard links using original CSV

# Fix graveyard links
p = URIRef('http://ldf.fi/schema/narc-menehtyneet1939-45/hautausmaa')
for s, o in surma[:p:]:
    surma.remove((s, p, o))
    surma.add((s, p, URIRef(re.sub(r'(\/hautausmaat\/)(\d+)', r'\1h\2', str(o)))))

print('Fixed known issues.')

# Validate all triples' object URIs against known classes and class instances.

classes = r.get_classes(surma)
class_instances = list(r.get_class_instances(surma, None))

known_uris = list(set(classes) | set(class_instances))

links = set(o for o in surma.objects() if type(o) != Literal)

unknown_links = list(set([o for o in links if o not in known_uris]))

# print(len(classes))
# print(len(class_instances))
# print(len(known_uris))

print('#####')
print('{num} unknown URI references found in objects:\n'.format(num=len(unknown_links)))
for o in sorted(unknown_links):
    print('{uri}  ({num})'.format(uri=str(o), num=len(list(surma[::o]))))

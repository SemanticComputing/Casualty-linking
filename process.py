#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing, validating and fixing some errors in "Casualties during the Finnish wars 1939–1945"
(Suomen sodissa 1939–1945 menehtyneet) RDF dataset.

Copyright (c) 2015 Mikko Koho
"""

import re

import joblib
from rdflib import *
import rdflib

import rdf_dm as r

INPUT_FILE_DIRECTORY = '~/RDF-data/sotasurmat/'
OUTPUT_FILE_DIRECTORY = '~/RDF-data/sotasurmat/new/'

try:
    surma = joblib.load('surma.pkl')
    print('Parsed {len} triples from pickle object.'.format(len=len(surma)))
except IOError:
    print('Processing Sotasurma RDF data.')
    surma = rdflib.Graph()
    surma.parse('data.ttl', format='turtle')
    print('Parsed {len} triples.'.format(len=len(surma)))
    joblib.dump(surma, 'surma.pkl')

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

unknown_links = [o for o in links if o not in known_uris]

print('#####')
print('Unknown URI references found in objects:\n\n')
for o in unknown_links:
    print('{uri}  ({num})'.format(uri=str(o), num=len(list(surma[::o]))))

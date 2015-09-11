#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing, validating and fixing some errors in "Casualties during the Finnish wars 1939–1945"
(Suomen sodissa 1939–1945 menehtyneet) RDF dataset. Input RDF data downloadable from LDF.FI linked data portal:
http://www.ldf.fi/dataset/narc-menehtyneet1939-45

Input CSV data is not publicly available (?).

Copyright (c) 2015 Mikko Koho
"""

import argparse
import os

import re

import joblib
import pandas as pd
from rdflib import *
import rdflib

import rdf_dm as r

INPUT_FILE_DIRECTORY = 'data/'
OUTPUT_FILE_DIRECTORY = 'data/new/'

DATA_FILE = 'surma.ttl'

# TODO: All mappings
URI_MAPPINGS = {
    URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Alipuseeri'):
        URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Aliupseeri'),
    URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Alikers__'):
        URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Alikersantti'),
    URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Jaeaek_'):
        URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Jaeaekaeri'),
    URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Miehisto'):
        URIRef('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/Miehistoe'),
}

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-r', action='store_true', help='Reload RDF graphs, instead of using pickle object')
parser.add_argument('-d', action='store_true', help='Dry run, don\'t serialize created graphs')
args = parser.parse_args()

reload = args.r
DRYRUN = args.d

# Read in CSV data
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

if not reload:
    # Read RDF graph from pickle
    try:
        surma = joblib.load(INPUT_FILE_DIRECTORY + 'surma.pkl')
        surma_onto = joblib.load(INPUT_FILE_DIRECTORY + 'surma_onto.pkl')
        print('Parsed {len} data triples from pickle object.'.format(len=len(surma)))
        print('Parsed {len} ontology triples from pickle object.'.format(len=len(surma)))
    except IOError:
        reload = True

if reload:
    # Read RDF graph from TTL files
    print('Processing Sotasurma RDF files.')

    surma = rdflib.Graph()
    surma.parse(INPUT_FILE_DIRECTORY + DATA_FILE, format='turtle')

    surma_onto = rdflib.Graph()
    input_dir = '{base}/{dir}'.format(base=os.getcwd(), dir=INPUT_FILE_DIRECTORY)
    for f in os.listdir(input_dir):
        if f != DATA_FILE and f.endswith('.ttl'):
            surma_onto.parse(input_dir + f, format='turtle')
    print('Parsed {len} data triples.'.format(len=len(surma)))
    print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
    joblib.dump(surma, INPUT_FILE_DIRECTORY + 'surma.pkl')
    joblib.dump(surma_onto, INPUT_FILE_DIRECTORY + 'surma_onto.pkl')
    print('Wrote graphs to pickle objects.')

CEMETERY_PREFIX = 'http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/'

print('Fixing cemetery links')
p = URIRef('http://ldf.fi/schema/narc-menehtyneet1939-45/hautausmaa')
unknown_cemeteries = []

for s, o in surma[:p:]:
    assert str(o).startswith(CEMETERY_PREFIX)

    new_o = URIRef(re.sub(r'(\/hautausmaat\/)(\d+)', r'\1h\2', str(o)))
    if not list(surma_onto[new_o::]):
        new_o = None

    cemetery_id = str(o)[50:]  # Should be like '0243_3'
    k_id, h_id = cemetery_id[:4], cemetery_id[5:].replace('_', '')
    # Links contain duplicate underscores, but not cemetery instances
    h_id = int(h_id) if h_id.isnumeric() else 0
    try:
        h_name = hmaat[hmaat['kunta_id'] == k_id][hmaat['hmaa_id'] == h_id]['hmaa_name'].iloc[0]
    except IndexError:
        if cemetery_id not in unknown_cemeteries:
            print('Unknown cemetery {id} not found in cemeteries CSV'.format(id=cemetery_id))
            try:
                k_name = kunta[kunta['kunta_id'] == k_id]['kunta_name'].iloc[0]
                print('           Found municipality {kunta}'.format(kunta=k_name))
                # TODO: Create new cemeteries using kunta
            except IndexError:
                print('           Municipality {kunta} not found'.format(kunta=k_id))

            unknown_cemeteries.append(cemetery_id)
        continue

    if not new_o:
        try:
            # print(hmaat[hmaat['kunta_id'] == k_id][hmaat['hmaa_id'] == h_id]['hmaa_name'])
            surma_onto.add((URIRef(CEMETERY_PREFIX + cemetery_id),
                            RDF.type,
                            URIRef('http://ldf.fi/schema/narc-menehtyneet1939-45/Hautausmaa')))
            print('New cemetery %s_%s : %s' % (k_id, h_id, h_name))
        except ValueError:
            print('Invalid cemetery id: {id}'.format(id=cemetery_id))
    else:
        h_name_onto = str(next(surma_onto[new_o:URIRef('http://www.w3.org/2004/02/skos/core#prefLabel'):]))
        assert h_name == h_name_onto, '%s ------- %s' % (h_name, h_name_onto)

    if new_o:
        surma.remove((s, p, o))
        surma.add((s, p, new_o))

# Fix faulty URI references
for map_from, map_to in URI_MAPPINGS.items():
    for s, p in surma[::map_from]:
        surma.remove((s, p, map_from))
        surma.add((s, p, map_to))

    for s, p in surma_onto[::map_from]:
        surma_onto.remove((s, p, map_from))
        surma_onto.add((s, p, map_to))


print('Fixed known issues.')

if not DRYRUN:
    surma.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
    surma_onto.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")

    surma.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma.ttl")
    surma_onto.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma_onto.ttl")
    print('Serialized graphs.')

# Validate all triples' object URIs against known classes and class instances.

unknown_links = r.get_unknown_links(surma + surma_onto)

print('Found {num} unknown URI references:\n'.format(num=len(unknown_links)))
for o in sorted(unknown_links):
    print('{uri}  ({num})'.format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))

# TODO: Check also subjects with no references
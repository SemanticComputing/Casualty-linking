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

ns_skos = Namespace('http://www.w3.org/2004/02/skos/core#')
ns_schema = Namespace('http://ldf.fi/schema/narc-menehtyneet1939-45/')

ns_hautausmaat = Namespace('http://ldf.fi/narc-menehtyneet1939-45/hautausmaat/')
ns_kansalaisuus = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansalaisuus/')
ns_kansallisuus = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kansallisuus/')
ns_kunnat = Namespace('http://ldf.fi/narc-menehtyneet1939-45/kunnat/')
ns_sotilasarvo = Namespace('http://ldf.fi/narc-menehtyneet1939-45/sotilasarvo/')
ns_menehtymisluokka = Namespace('http://ldf.fi/narc-menehtyneet1939-45/menehtymisluokka/')

URI_MAPPINGS = {
    # MANUAL FIXES TO SOME URI'S USED AS TRIPLE OBJECTS
    ns_sotilasarvo.Alipuseeri: ns_sotilasarvo.Aliupseeri,
    ns_sotilasarvo.Alikers__: ns_sotilasarvo.Alikersantti,
    ns_sotilasarvo.Jaeaek_: ns_sotilasarvo.Jaeaekaeri,
    ns_sotilasarvo.Miehisto: ns_sotilasarvo.Miehistoe,
    ns_sotilasarvo.Stm__________: ns_sotilasarvo.Sotamies,
    ns_sotilasarvo._stm_: ns_sotilasarvo.Sotamies,
    ns_sotilasarvo.Matr_: ns_sotilasarvo.Matruusi,
    ns_sotilasarvo.Muu: ns_sotilasarvo.Muu_arvo,
    ns_kansalaisuus.Fi_: ns_kansalaisuus.Suomi,
    ns_kansallisuus.Fi_: ns_kansallisuus.Suomi,
    ns_hautausmaat.x___: ns_hautausmaat.hx_0,
    ns_kunnat.kx: ns_kunnat.k,
    ns_menehtymisluokka._ : ns_menehtymisluokka.Tuntematon,
}

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-r', action='store_true', help='Reload RDF graphs, instead of using pickle object')
parser.add_argument('-d', action='store_true', help='Dry run, don\'t serialize created graphs')
args = parser.parse_args()

reload = args.r
DRYRUN = args.d


# READ IN CSV DATA
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


# READ IN RDF DATA
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


# FIX FAULTY URI REFERENCES BY DIRECT MAPPING
for map_from, map_to in URI_MAPPINGS.items():
    print('Fixing %s --> %s' % (map_from, map_to))
    for s, p in list(surma[::map_from]):
        surma.remove((s, p, map_from))
        surma.add((s, p, map_to))

    for s, p in list(surma_onto[::map_from]):
        surma_onto.remove((s, p, map_from))
        surma_onto.add((s, p, map_to))


# FIX CEMETERY LINKS AND INSTANCES
p = ns_schema.hautausmaa

for s, o in list(surma[:p:]):
    assert str(o).startswith(ns_hautausmaat)

    new_o = URIRef(re.sub(r'(/hautausmaat/)(\d+)', r'\1h\2', str(o)))
    if not list(surma_onto[new_o::]):
        new_o = None

    cemetery_id = str(o)[50:]  # Should be like '0243_3'
    k_id, h_id = cemetery_id[:4], cemetery_id[5:].replace('_', '')

    h_name = ''
    try:
        h_name = hmaat[hmaat['kunta_id'] == k_id][hmaat['hmaa_id'] == (int(h_id) if h_id.isnumeric() else 0)]['hmaa_name'].iloc[0]
    except IndexError:
        pass

    try:
        k_name = kunta[kunta['kunta_id'] == k_id]['kunta_name'].iloc[0]
        h_name = h_name or '{kunta} {id}'.format(kunta=k_name, id=h_id)
    except IndexError:
        pass

    if not new_o:
        try:
            new_o = ns_hautausmaat['h{cem_id}'.format(cem_id=cemetery_id)]
        except ValueError:
            print('Invalid cemetery id: {id}'.format(id=cemetery_id))
            continue

        assert h_name, s  # We should have a name to use as prefLabel

        surma_onto.add((new_o, RDF.type, ns_schema.Hautausmaa))
        surma_onto.add((new_o, ns_skos.prefLabel, Literal(h_name)))
        surma_onto.add((new_o, ns_schema.hautauskunta, Literal(h_name)))

        print('New cemetery %s : %s' % (new_o, h_name))

    else:
        h_name_onto = str(next(surma_onto[new_o:ns_skos.prefLabel:]))
        if h_name:
            assert h_name == h_name_onto or h_name == h_name_onto.split()[0], '%s  !=  %s' % (h_name, h_name_onto)

    if new_o:
        surma.remove((s, p, o))
        surma.add((s, p, new_o))


print('Fixed known issues.')


# DO SOME VALIDATION TO FIND REMAINING ERRORS

full_rdf = surma + surma_onto

unknown_links = r.get_unknown_links(full_rdf)

print('Found {num} unknown URI references:\n'.format(num=len(unknown_links)))
for o in sorted(unknown_links):
    print('{uri}  ({num})'.format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))

unlinked_subjects = r.get_unlinked_uris(full_rdf)

print('Found {num} unlinked subjects:\n'.format(num=len(unlinked_subjects)))
for s in sorted(unlinked_subjects):
    print('{uri}'.format(uri=str(s)))

# TODO: Link to Warsa municipalities

# TODO: Link to joukko-osastot


# SERIALIZE GRAPHS

if not DRYRUN:
    surma.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
    surma_onto.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")

    surma.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma.ttl")
    surma_onto.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma_onto.ttl")
    print('Serialized graphs.')

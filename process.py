#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Code for processing, validating and fixing some errors in "Casualties during the Finnish wars 1939–1945"
(Suomen sodissa 1939–1945 menehtyneet) RDF dataset. Input RDF data downloadable from LDF.FI linked data portal:
http://www.ldf.fi/dataset/narc-menehtyneet1939-45

Input CSV data is not publicly available (?).

Copyright (c) 2015 Mikko Koho

Issues remaining:
    - schema misused atleast http://ldf.fi/schema/narc-menehtyneet1939-45/hautauskunta being used against given
      domain from also Hautausmaa instances, but no actual validation against schema done.
"""

import argparse
import os
import pprint

import re

import joblib
import pandas as pd
from rdflib import *
import rdflib

import rdf_dm as r
from arpa_linker.arpa import Arpa, arpafy

INPUT_FILE_DIRECTORY = 'data/'
OUTPUT_FILE_DIRECTORY = 'data/new/'

DATA_FILE = 'surma.ttl'

ns_skos = Namespace('http://www.w3.org/2004/02/skos/core#')
ns_dct = Namespace('http://purl.org/dc/terms/')
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
    ns_sotilasarvo.Matr__: ns_sotilasarvo.Matruusi,
    ns_sotilasarvo.Muu: ns_sotilasarvo.Muu_arvo,
    ns_kansalaisuus.Fi_: ns_kansalaisuus.Suomi,
    ns_kansallisuus.Fi_: ns_kansallisuus.Suomi,
    ns_hautausmaat.x___: ns_hautausmaat.hx_0,
    ns_kunnat.kx: ns_kunnat.k,
    ns_menehtymisluokka._ : ns_menehtymisluokka.Tuntematon,
}

parser = argparse.ArgumentParser(description='Casualties of war')
parser.add_argument('-r', action='store_true', help='Reload RDF graphs, instead of using pickle object')
parser.add_argument('-s', action='store_true', help='Skip cemetery fixing')
parser.add_argument('-v', action='store_true', help='Skip validation')
parser.add_argument('-u', action='store_true', help='Skip linking to military units')
parser.add_argument('-d', action='store_true', help='Dry run, don\'t serialize created graphs')
args = parser.parse_args()

reload = args.r
DRYRUN = args.d
SKIP_CEMETERIES = args.s
SKIP_VALIDATION = args.v
SKIP_UNITS = args.u

surma = rdflib.Graph()
surma_onto = rdflib.Graph()



def fix_by_direct_uri_mappings():
    """
    Fix faulty uri references by direct mapping
    """
    for map_from, map_to in URI_MAPPINGS.items():
        print('Fixing %s --> %s' % (map_from, map_to))
        for s, p in list(surma[::map_from]):
            surma.remove((s, p, map_from))
            surma.add((s, p, map_to))

        for s, p in list(surma_onto[::map_from]):
            surma_onto.remove((s, p, map_from))
            surma_onto.add((s, p, map_to))


def fix_cemetery_links():
    """
    Fix errors in cemetery links and add missing cemetery instances.
    """
    p = ns_schema.hautausmaa

    for s, o in list(surma[:p:]):
        assert str(o).startswith(ns_hautausmaat)

        new_o = URIRef(re.sub(r'(/hautausmaat/)(\d+)', r'\1h\2', str(o)))
        if not list(surma_onto[new_o::]):
            new_o = None

        cemetery_id = str(o)[50:]  # Should be like '0243_3'
        k_id, h_id = cemetery_id[:4], cemetery_id[5:].replace('_', '')

        k_name = ''
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

            assert h_name, s    # Should have a name to use as prefLabel
            assert k_name, s    # Should have a name for municipality

            kunta_node = next(surma_onto[:ns_skos.prefLabel:Literal(k_name, lang='fi')])

            surma_onto.add((new_o, RDF.type, ns_schema.Hautausmaa))
            surma_onto.add((new_o, ns_skos.prefLabel, Literal(h_name)))
            surma_onto.add((new_o, ns_schema.hautausmaakunta, kunta_node))

            print('New cemetery %s : %s' % (new_o, h_name))

        else:
            h_name_onto = str(next(surma_onto[new_o:ns_skos.prefLabel:]))
            if h_name:
                assert h_name == h_name_onto or h_name == h_name_onto.split()[0], '%s  !=  %s' % (h_name, h_name_onto)

        if new_o:
            surma.remove((s, p, o))
            surma.add((s, p, new_o))


def link_to_warsa_municipalities():
    """
    Link to Warsa municipalities used in SotaSampo project.
    """
    munics = r.helpers.read_graph_from_sparql("http://ldf.fi/warsa/sparql",
                                              graph_name='http://ldf.fi/warsa/places/municipalities')

    for s in list(surma_onto[:RDF.type:ns_schema.Kunta]):
        label = next(surma_onto[s:ns_skos.prefLabel:])

        warsa_s = []

        for lbl in str(label).strip().split('/'):
            if lbl == 'Pyhäjärvi Ol':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_75')]
            elif lbl == 'Pyhäjärvi Ul.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_543')]
            elif lbl == 'Pyhäjärvi Vl':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_586')]
            elif lbl == 'Koski Tl.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_291')]
            elif lbl == 'Koski Hl.':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_391')]
            elif lbl == 'Uusikirkko Vl':
                warsa_s = [URIRef('http://ldf.fi/warsa/places/municipalities/m_place_609')]

            if not warsa_s:
                warsa_s = list(munics[:ns_skos.prefLabel:Literal(lbl)])
            if not warsa_s:
                warsa_s = list(munics[:ns_skos.prefLabel:Literal(lbl.replace(' kunta', ' mlk'))])

        # NOTE! hautauskunta seems to refer to current municipalities, unlike the rest

        if len(warsa_s) == 0:
            if set(surma.subjects(None, s)) - set(surma[:ns_schema.hautauskunta:s]):
                print('WARNING: Couldn\'t find Warsa URI for {lbl}'.format(lbl=label))
        elif len(warsa_s) == 1:
            # print('Found {lbl} as Warsa URI {s}'.format(lbl=label, s=warsa_s[0]))
            for subj in list(surma[:ns_schema.synnyinkunta:s]):
                surma.add((subj, ns_schema.synnyinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.synnyinkunta, s))
            for subj in list(surma[:ns_schema.kotikunta:s]):
                surma.add((subj, ns_schema.kotikunta, warsa_s[0]))
                surma.remove((subj, ns_schema.kotikunta, s))
            # for subj in list(surma[:ns_schema.hautauskunta:s]):
                # surma.add((subj, ns_schema.hautauskunta, s))
                # surma.remove((subj, ns_schema.hautauskunta, s))
            for subj in list(surma_onto[:ns_schema.hautauskunta:s]):
                # Fixes cemetery municipalities
                surma_onto.add((subj, ns_schema.hautausmaakunta, s))  # Add hautausmaakunta
                surma_onto.remove((subj, ns_schema.hautauskunta, s))
            for subj in list(surma[:ns_schema.asuinkunta:s]):
                surma.add((subj, ns_schema.asuinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.asuinkunta, s))
            for subj in list(surma[:ns_schema.kuolinkunta:s]):
                surma.add((subj, ns_schema.kuolinkunta, warsa_s[0]))
                surma.remove((subj, ns_schema.kuolinkunta, s))
        else:
            print('WARNING: Found multiple Warsa URIs for {lbl}: {s}'.format(lbl=label, s=warsa_s))


def validate():
    """
    Do some validation to find remaining errors
    """

    full_rdf = surma + surma_onto

    unknown_links = r.get_unknown_links(full_rdf)

    print('\nFound {num} unknown URI references:'.format(num=len(unknown_links)))
    for o in sorted(unknown_links):
        print('{uri}  ({num})'.format(uri=str(o), num=str(len(list(surma[::o])) + len(list(surma_onto[::o])))))

    unlinked_subjects = [uri for uri in r.get_unlinked_uris(full_rdf)
                         if not str(uri).startswith('http://ldf.fi/narc-menehtyneet1939-45/p')]

    print('\nFound {num} unlinked subjects:'.format(num=len(unlinked_subjects)))
    for s in sorted(unlinked_subjects):
        print('{uri}'.format(uri=str(s)))


def link_to_military_ranks():
    """
    Link casualties to their ranks in Warsa
    """
    ranks = r.read_graph_from_sparql("http://ldf.fi/warsa/sparql", 'http://ldf.fi/warsa/actors/actor_types')

    p = ns_schema.sotilasarvo
    for s, o in list(surma[:p:]):
        rank_label = list(surma_onto[o:ns_skos.prefLabel:])[0]
        rank_label = Literal(str(rank_label).capitalize())  # Strip lang attribute and capitalize
        found_ranks = list(ranks[:ns_skos.prefLabel:rank_label])

        new_o = None
        if len(found_ranks) == 1:
            new_o = found_ranks[0]
        elif len(found_ranks) > 1:
            print('WARNING: Found multiple ranks for {rank}'.format(rank=rank_label))
        else:
            print('WARNING: Couldn\'t find military rank for {rank}'.format(rank=rank_label))

        if new_o:
            surma.remove((s, p, o))
            surma.add((s, p, new_o))


def _unit_preprocessor(text):
    """
    Preprocess military unit abbreviation strings for all possible combinations

    :param text:
    :return:
    """

    import re
    import itertools

    def _split(part):
        return [a for a, b in re.findall(r'(\w+?)(\b|(?<=[a-zäö])(?=[A-ZÄÖ]))', part)]
        # return [p.strip() for p in part.split('.')]

    def _variations(part):
        inner_parts = _split(part) + ['']
        vars = []
        vars += ['.'.join(inner_parts)]
        vars += ['. '.join(inner_parts)]
        vars += [' '.join(inner_parts)]
        vars += [''.join(inner_parts)]
        return vars

    variation_lists = [_variations(part) + [part] for part in text.split('/')]

    combined_variations = sorted(set(['/'.join(foo).strip().replace(' /', '/') for foo in sorted(set(itertools.product(*variation_lists)))]))
    return ' # '.join(combined_variations) + \
           ' # ' + ' # '.join(sorted(set(variation.strip() for var_list in variation_lists for variation in var_list)))


def link_to_military_units(graph):
    """
    Link casualties to all of their military units in Warsa
    """

    arpa = Arpa('http://demo.seco.tkk.fi/arpa/menehtyneet_units')

    # Query the ARPA service and add the matches
    return arpafy(graph, ns_schema.unit, arpa, ns_schema.joukko_osasto, preprocessor=_unit_preprocessor, progress=True)



#######
# MAIN

if __name__ == "__main__":
    # import doctest

    ##################
    # READ IN CSV DATA

    print('Reading data')

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


    ##################
    # READ IN RDF DATA

    if not reload:
        # Read RDF graph from pickle
        try:
            surma = joblib.load(INPUT_FILE_DIRECTORY + 'surma.pkl')
            surma_onto = joblib.load(INPUT_FILE_DIRECTORY + 'surma_onto.pkl')
            print('Parsed {len} data triples from pickle object.'.format(len=len(surma)))
            print('Parsed {len} ontology triples from pickle object.'.format(len=len(surma_onto)))
        except IOError:
            reload = True

    if reload:
        # Read RDF graph from TTL files
        print('Processing Sotasurma RDF files.')

        surma.parse(INPUT_FILE_DIRECTORY + DATA_FILE, format='turtle')

        input_dir = '{base}/{dir}'.format(base=os.getcwd(), dir=INPUT_FILE_DIRECTORY)
        for f in os.listdir(input_dir):
            if f != DATA_FILE and f.endswith('.ttl'):
                surma_onto.parse(input_dir + f, format='turtle')
        print('Parsed {len} data triples.'.format(len=len(surma)))
        print('Parsed {len} ontology triples.'.format(len=len(surma_onto)))
        joblib.dump(surma, INPUT_FILE_DIRECTORY + 'surma.pkl')
        joblib.dump(surma_onto, INPUT_FILE_DIRECTORY + 'surma_onto.pkl')
        print('Wrote graphs to pickle objects.')


    ##########################################################
    # FIX KNOWN ISSUES AND ADD LINKS TO OTHER SOTASAMPO GRAPHS

    fix_by_direct_uri_mappings()

    if not SKIP_CEMETERIES:
        fix_cemetery_links()

        surma_onto.add((ns_schema.hautausmaakunta, RDF.type, OWL.ObjectProperty))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.label, Literal('Hautausmaan kunta', lang='fi')))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.domain, ns_schema.Hautausmaa))
        surma_onto.add((ns_schema.hautausmaakunta, RDFS.range, ns_schema.Kunta))
        surma_onto.add((ns_schema.hautausmaakunta, ns_skos.prefLabel, Literal('Hautausmaan kunta', lang='fi')))

    print('\nFixed known issues.\n')

    if not SKIP_VALIDATION:
        validate()
        print()

    link_to_warsa_municipalities()

    print()

    link_to_military_ranks()

    print('\nLinked to military ranks.\n')

    surma_onto.remove((ns_schema.sotilasarvo, RDFS.range, None))
    surma_onto.add((ns_schema.sotilasarvo, RDFS.range, URIRef('http://ldf.fi/warsa/actors/ranks/Rank')))

    if not SKIP_UNITS:
        pprint.pprint(link_to_military_units(surma))

    # TODO: Fix possible errors in schema

    # TODO: Kunnat jotka ei löydy Warsasta ja hautauskunnat (nykyisiä kuntia) voisi linkittää esim. paikannimirekisterin paikkoihin

    surma_onto.add((ns_kunnat.kunta_ontologia, ns_dct.contributor, URIRef('http://orcid.org/0000-0002-7373-9338')))
    surma_onto.add((ns_kunnat.kunta_ontologia, ns_dct.contributor, URIRef('http://www.seco.tkk.fi/')))


    ##################
    # SERIALIZE GRAPHS

    if not DRYRUN:
        surma.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")
        surma.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")

        surma_onto.bind("narc", "http://ldf.fi/narc-menehtyneet1939-45/")  # TODO: Move schema stuff to schema namespace (e.g. skos:ConceptSchemes)
        surma_onto.bind("narcs", "http://ldf.fi/schema/narc-menehtyneet1939-45/")
        surma_onto.bind("geo", "http://www.georss.org/georss/")
        surma_onto.bind("dct", "http://purl.org/dc/terms/")

        surma.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma.ttl")
        surma_onto.serialize(format="turtle", destination=OUTPUT_FILE_DIRECTORY + "surma_onto.ttl")
        print('\nSerialized graphs.')


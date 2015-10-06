"""
ARPA service functions for common Sotasampo tasks
"""
import json
import pprint

import re
import itertools

from arpa_linker.arpa import Arpa, arpafy
from rdflib import URIRef


def _create_unit_abbreviations(text, *args):
    """
    Preprocess military unit abbreviation strings for all possible combinations

    :param text: Military unit abbreviation
    :return: String containing all possible abbrevations separated by '#'

    >>> _create_unit_abbreviations('3./JR 1')
    '3./JR 1 # 3./JR. 1. # 3./JR.1. # 3./JR1 # 3/JR 1 # 3/JR. 1. # 3/JR.1. # 3/JR1 # 3 # 3. # JR 1 # JR. 1. # JR.1. # JR1'
    """

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

    combined_variations = sorted(set(['/'.join(combined).strip().replace(' /', '/')
                                      for combined in sorted(set(itertools.product(*variation_lists)))]))
    return ' # '.join(combined_variations) + \
           ' # ' + ' # '.join(sorted(set(variation.strip() for var_list in variation_lists for variation in var_list)))


def link_to_military_units(graph, target_prop, source_prop):
    """
    Link military units to known matching military units in Warsa
    :returns dict containing some statistics and a list of errors

    :type graph: rdflib.Graph
    :param target_prop: target property to use for new links
    :param source_prop: source property as URIRef
    """

    arpa = Arpa('http://demo.seco.tkk.fi/sotasampo_helpers/warsa_actor_units')

    # Query the ARPA service and add the matches
    return arpafy(graph, target_prop, arpa, source_prop,
                  preprocessor=_create_unit_abbreviations, progress=True)


def link_to_warsa_persons(graph_data, graph_schema, target_prop, source_rank_prop, source_firstname_prop,
                          source_lastname_prop, preprocessor=None, validator=None):
    """
    Link a person to known Warsa persons

    :type graph_data: rdflib.Graph
    :type graph_schema: rdflib.Graph
    :param target_prop: target property to use for new links
    :param source_rank_prop: military rank property
    :param source_fullname_prop: full name property
    """

    def _combine_rank_and_names(rank, person_uri, graph):
        return '{rank} {firstname} {lastname}'.format(
            rank=str(next(graph_schema[rank:URIRef('http://www.w3.org/2004/02/skos/core#prefLabel'):], '')),
            firstname=str(next(graph[person_uri:source_firstname_prop:], '')),
            lastname=str(next(graph[person_uri:source_lastname_prop:], '')))

    def _validator(graph, s):
        def _validate_name(text, results):
            if not results:
                return results

            text_parts = text.lower().split(' ')
            rank, firstnames, lastname = (text_parts[0], text_parts[1:-1], text_parts[-1])

            filtered = []
            for person in results:
                try:
                    assert lastname == person['properties'].get('sukunimi')[0].replace('"', '').lower()
                    assert rank == person['properties'].get('sotilasarvolabel', [''])[0].replace('"', '').lower()
                    assert set(firstnames) & \
                       set([p.split('^')[0].replace('"', '').lower() for p in person['properties'].get('etunimet')])
                    filtered.append(person)
                except AssertionError:
                    continue

            return filtered

        return _validate_name

    arpa = Arpa('http://demo.seco.tkk.fi/arpa/warsa_actor_persons')

    if preprocessor is None:
        preprocessor = _combine_rank_and_names

    if validator is None:
        validator = _validator

    # Query the ARPA service and add the matches
    return arpafy(graph_data, target_prop, arpa, source_rank_prop,
                  preprocessor=preprocessor, progress=True, validator=validator)


if __name__ == "__main__":
    print('Running doctests')
    import doctest

    res = doctest.testmod()
    if not res[0]:
        print('OK!')

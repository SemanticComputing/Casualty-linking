"""
ARPA service functions for common Sotasampo tasks
"""

import re
import itertools

from arpa_linker.arpa import Arpa, arpafy


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
    Link casualties to all of their military units in Warsa
    :returns dict containing some statistics and a list of errors

    :param target_prop: target property as URIRef
    :param source_prop: source property as URIRef
    :type graph: rdflib.Graph
    """

    arpa = Arpa('http://demo.seco.tkk.fi/sotasampo_helpers/warsa_actor_units')

    # Query the ARPA service and add the matches
    return arpafy(graph, target_prop, arpa, source_prop,
                  preprocessor=_create_unit_abbreviations, progress=True)


if __name__ == "__main__":
    print('Running doctests')
    import doctest
    res = doctest.testmod()
    if not res[0]:
        print('OK!')

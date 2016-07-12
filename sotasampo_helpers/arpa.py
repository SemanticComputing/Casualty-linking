"""
ARPA service functions for common Sotasampo tasks
"""
import logging

import sys
import re
import itertools

from arpa_linker.arpa import Arpa, ArpaMimic, parse_args, process, process_graph, log_to_file
from rdflib import URIRef, Namespace, Graph
from fuzzywuzzy import fuzz

log = logging.getLogger(__name__)


class Validator:
    def __init__(self, graph, graph_schema, birthdate_prop, deathdate_prop, source_rank_prop,
                    source_firstname_prop, source_lastname_prop):
        self.graph = graph
        self.graph_schema = graph_schema
        self.birthdate_prop = birthdate_prop
        self.deathdate_prop = deathdate_prop
        self.source_rank_prop = source_rank_prop
        self.source_firstname_prop = source_firstname_prop
        self.source_lastname_prop = source_lastname_prop

    def validate(self, results, text, s):
        if not results:
            return results

        rank = self.graph.value(s, self.source_rank_prop)
        rank = str(self.graph_schema.value(rank, URIRef('http://www.w3.org/2004/02/skos/core#prefLabel'))).lower()
        firstnames = str(self.graph.value(s, self.source_firstname_prop)).replace('/', ' ').lower().split()
        lastname = str(self.graph.value(s, self.source_lastname_prop)).replace('/', ' ').lower()

        filtered = []
        _fuzzy_lastname_match_limit = 50
        _fuzzy_firstname_match_limit = 60

        for person in results:
            score = 0
            res_id = None
            try:
                res_id = person['properties'].get('id')[0].replace('"', '')
                res_ranks = [r.replace('"', '').lower() for r in person['properties'].get('rank_label', [''])]

                res_lastname = person['properties'].get('sukunimi')[0].replace('"', '').lower()
                res_firstnames = person['properties'].get('etunimet')[0].split('^')[0].replace('"', '').lower()
                res_firstnames = res_firstnames.split()

                res_birthdates = (min(person['properties'].get('birth_start', [''])).split('^')[0].replace('"', ''),
                                    max(person['properties'].get('birth_end', [''])).split('^')[0].replace('"', ''))
                res_deathdates = (min(person['properties'].get('death_start', [''])).split('^')[0].replace('"', ''),
                                    max(person['properties'].get('death_end', [''])).split('^')[0].replace('"', ''))

            except TypeError:
                log.info('Unable to read data for validation for {uri} , skipping result...'.format(uri=res_id))
                continue

            log.debug('Potential match for person {p1text} <{p1}> : {p2text} {p2}'.
                        format(p1text=' '.join([rank] + firstnames + [lastname]),
                                p1=s,
                                p2text=' '.join(res_ranks + res_firstnames + [res_lastname]),
                                p2=res_id))

            fuzzy_lastname_match = fuzz.token_set_ratio(lastname, res_lastname, force_ascii=False)

            if fuzzy_lastname_match >= _fuzzy_lastname_match_limit:
                log.debug('Fuzzy last name match for {f1} and {f2}: {fuzzy}'
                            .format(f1=lastname, f2=res_lastname, fuzzy=fuzzy_lastname_match))
                score += int((fuzzy_lastname_match - _fuzzy_lastname_match_limit) /
                                (100 - _fuzzy_lastname_match_limit) * 100)

            if rank and res_ranks and rank != 'tuntematon':
                if rank in res_ranks:
                    score += 25
                else:
                    score -= 25

            birthdate = str(self.graph.value(s, self.birthdate_prop))
            deathdate = str(self.graph.value(s, self.deathdate_prop))

            if res_birthdates[0] and birthdate:
                if res_birthdates[0] <= birthdate:
                    if res_birthdates[0] == birthdate:
                        score += 50
                else:
                    score -= 25

            if res_birthdates[1] and birthdate:
                if birthdate <= res_birthdates[1]:
                    if res_birthdates[1] == birthdate:
                        score += 50
                else:
                    score -= 25

            if res_deathdates[0] and deathdate:
                if res_deathdates[0] <= deathdate:
                    if res_deathdates[0] == deathdate:
                        score += 50
                else:
                    score -= 25

            if res_deathdates[1] and deathdate:
                if deathdate <= res_deathdates[1]:
                    if deathdate == res_deathdates[1]:
                        score += 50
                else:
                    score -= 25

            s_first1 = ' '.join(firstnames)
            s_first2 = ' '.join(res_firstnames)
            fuzzy_firstname_match = max(fuzz.partial_ratio(s_first1, s_first2),
                                        fuzz.token_sort_ratio(s_first1, s_first2, force_ascii=False),
                                        fuzz.token_set_ratio(s_first1, s_first2, force_ascii=False))

            if fuzzy_firstname_match >= _fuzzy_firstname_match_limit:
                log.debug('Fuzzy first name match for {f1} and {f2}: {fuzzy}'
                            .format(f1=firstnames, f2=res_firstnames, fuzzy=fuzzy_firstname_match))
                score += int((fuzzy_firstname_match - _fuzzy_firstname_match_limit) /
                                (100 - _fuzzy_firstname_match_limit) * 100)
            else:
                log.debug('No fuzzy first name match for {f1} and {f2}: {fuzzy}'
                            .format(f1=firstnames, f2=res_firstnames, fuzzy=fuzzy_firstname_match))

            person['score'] = score

            if score > 200:
                filtered.append(person)

                log.info('Found matching Warsa person for {rank} {fn} {ln} {uri}: '
                            '{res_rank} {res_fn} {res_ln} {res_uri} [score: {score}]'.
                            format(rank=rank, fn=s_first1, ln=lastname, uri=s,
                                res_rank=res_ranks, res_fn=s_first2, res_ln=res_lastname, res_uri=res_id,
                                score=score))
            else:
                log.info('Skipping potential match because of too low score [{score}]: {p1}  <<-->>  {p2}'.
                            format(p1=s, p2=res_id, score=score))

        if len(filtered) == 1:
            return filtered
        elif len(filtered) > 1:
            log.warning('Found several matches for Warsa person {s} ({text}): {ids}'.
                        format(s=s, text=text,
                                ids=', '.join(p['properties'].get('id')[0].split('^')[0].replace('"', '')
                                                for p in filtered)))

            best_matches = sorted(filtered, key=lambda p: p['score'], reverse=True)
            log.warning('Choosing best match: {id}'.format(id=best_matches[0].get('id')))
            return [best_matches[0]]

        return []


def _create_unit_abbreviations(text, *args):
    """
    Preprocess military unit abbreviation strings for all possible combinations

    :param text: Military unit abbreviation
    :return: List containing all possible abbrevations

    >>> _create_unit_abbreviations('3./JR 1')
    '3./JR 1 # 3./JR. 1. # 3./JR.1. # 3./JR1 # 3/JR 1 # 3/JR. 1. # 3/JR.1. # 3/JR1 # JR 1 # JR. 1. # JR.1. # JR1'
    >>> _create_unit_abbreviations('27.LK')
    '27 LK # 27. LK. # 27.LK # 27.LK. # 27LK # 27 LK # 27. LK. # 27.LK # 27.LK. # 27LK'
    >>> _create_unit_abbreviations('P/L Ilmarinen')
    'P./L Ilmarinen # P./L. Ilmarinen. # P./L.Ilmarinen. # P./LIlmarinen # P/L Ilmarinen # P/L. Ilmarinen. # P/L.Ilmarinen. # P/LIlmarinen # L Ilmarinen # L. Ilmarinen. # L.Ilmarinen. # LIlmarinen # P # P.'
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

    variationset = set(variation.strip() for var_list in variation_lists for variation in var_list
                       if not re.search(r'^[0-9](\.)?$', variation.strip()))

    return ' # '.join(combined_variations) + ' # ' + ' # '.join(sorted(variationset))


def link_to_military_units(graph, graph_schema, target_prop, source_prop, arpa, *args, **kwargs):
    """
    Link military units to known matching military units in Warsa
    :returns dict containing some statistics and a list of errors

    :param graph: RDF graph containing the units strings to be linked
    :type graph: rdflib.Graph

    :param target_prop: target property to use for new links
    :param source_prop: source property as URIRef
    :param arpa: the Arpa instance
    """

    # Query the ARPA service and add the matches
    return process_graph(graph, target_prop, arpa, source_prop=source_prop,
            preprocessor=_create_unit_abbreviations, progress=True, **kwargs)


def link_to_pnr(graph, graph_schema, target_prop, source_prop, arpa, *args, **kwargs):
    """
    Link municipalities to Paikannimirekisteri.
    :returns dict containing some statistics and a list of errors

    :type graph: rdflib.Graph
    :param target_prop: target property to use for new links
    :param source_prop: source property as URIRef
    """

    def _get_municipality_label(uri, *args):
        """
        :param uri: municipality URI
        """
        return str(graph_schema.value(uri, URIRef('http://www.w3.org/2004/02/skos/core#prefLabel'))).replace('/', ' ')

    # Query the ARPA service and add the matches
    return process_graph(graph, target_prop, arpa, source_prop=source_prop,
                  preprocessor=_get_municipality_label, progress=True, **kwargs)


def link_to_warsa_persons(graph, graph_schema, target_prop, source_prop, arpa, source_lastname_prop,
        source_firstname_prop, source_rank_prop, birthdate_prop, deathdate_prop,
        preprocessor=None, validator=None, **kwargs):
    """
    Link a person to known Warsa persons

    :param graph: RDF graph where the names and such are found
    :type graph: rdflib.Graph

    :param graph_schema: RDF graph containing military rank labels
    :type graph_schema: rdflib.Graph

    :param target_prop: target property to use for new links
    :param source_prop: source property to use for linking
    :param arpa: the Arpa instance

    :param source_lastname_prop: last name property
    :param source_firstname_prop: first name property
    :param source_rank_prop: military rank property
    :param birthdate_prop: birth date property
    :param deathdate_prop: death date property

    :preprocessor: text preprocessor
    :validator: link validator
    """
    # TODO: sotilasarvolabel --> rank_label, pisteytys pitää säätää uusiksi (tarkasta)
    # TODO: henkilöllä voi olla useita sotilasarvoja, vertailu kaikkiin (tarkasta toimiiko)

    # TODO: ARPAn sijaan voisi kysellä suoraan SPARQL-kyselyllä kandidaatit

    # if preprocessor is None:
    #     preprocessor = _combine_rank_and_names
    #
    if validator is None:
        validator = Validator(graph, graph_schema, birthdate_prop, deathdate_prop,
                source_rank_prop, source_firstname_prop, source_lastname_prop)

    # Query the ARPA service, add the matches and serialize the graph to disk.
    return process_graph(graph, target_prop, arpa, source_prop=source_prop,
            preprocessor=preprocessor, validator=validator, progress=True, **kwargs)


def process_stage(link_function, stage, arpa_args, query_template_file=None, rank_schema_file=None):
    log_to_file('process.log', arpa_args.log_level)
    del arpa_args.log_level

    if stage == 'join':
        process(arpa_args.input, arpa_args.fi, arpa_args.output, arpa_args.fo, arpa_args.tprop, source_prop=arpa_args.prop,
                rdf_class=arpa_args.rdf_class, new_graph=arpa_args.new_graph, join_candidates=True,
                run_arpafy=False, progress=True)
    else:
        arpa_args = vars(arpa_args)

        input_format = arpa_args.pop('fi')
        output = arpa_args.pop('output')
        output_format = arpa_args.pop('fo')

        data = Graph()
        data.parse(arpa_args.pop('input'), format=input_format)

        ns_schema = Namespace('http://ldf.fi/schema/narc-menehtyneet1939-45/')

        arpa_url = arpa_args.pop('arpa', None)

        if stage == 'candidates':
            arpa_args['candidates_only'] = True
            schema = None

            arpa = Arpa(arpa_url, arpa_args.pop('no_duplicates'), arpa_args.pop('min_ngram'),
                    retries=arpa_args.pop('retries'), wait_between_tries=arpa_args.pop('wait'),
                    ignore=arpa_args.pop('ignore'))

        else:
            with open(query_template_file) as f:
                qry = f.read()

            schema = Graph()
            schema.parse(rank_schema_file, format=input_format)

            arpa = ArpaMimic(qry, arpa_url, arpa_args.pop('no_duplicates'), arpa_args.pop('min_ngram'),
                    retries=arpa_args.pop('retries'), wait_between_tries=arpa_args.pop('wait'),
                    ignore=arpa_args.pop('ignore'))

        res = link_function(data, schema, arpa_args.pop('tprop'), arpa_args.pop('prop'), arpa,
                ns_schema.sukunimi, ns_schema.etunimet, ns_schema.sotilasarvo, ns_schema.syntymaeaika,
                ns_schema.kuolinaika, **arpa_args)

        res['graph'].serialize(output, format=output_format)


def print_usage(exit_=True):
    print('usage: arpa.py test|(persons|units|pnr candidates|join|(disambiguate query_template_file rank_schema_ttl_file) [arpa_linker_args])')
    if exit_:
        exit()


if __name__ == '__main__':

    if sys.argv[1] == 'test':
        print('Running doctests')
        import doctest

        res = doctest.testmod()
        if not res[0]:
            print('OK!')
        exit()

    if len(sys.argv) < 3:
        print_usage()

    target = sys.argv[1]
    if target == 'persons':
        link_fn = link_to_warsa_persons
    elif target == 'units':
        link_fn = link_to_military_units
    elif target == 'pnr':
        link_fn = link_to_pnr
    else:
        print_usage()

    stage = sys.argv[2]
    if stage == 'disambiguate':
        process_stage(link_fn, stage, parse_args(sys.argv[5:]), query_template_file=sys.argv[3],
                rank_schema_file=sys.argv[4])
    else:
        process_stage(link_fn, stage, parse_args(sys.argv[3:]))

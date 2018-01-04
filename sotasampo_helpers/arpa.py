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

log = logging.getLogger('arpa_linker.arpa')


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
        _FUZZY_LASTNAME_MATCH_LIMIT = 50
        _FUZZY_FIRSTNAME_MATCH_LIMIT = 60

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

            if fuzzy_lastname_match >= _FUZZY_LASTNAME_MATCH_LIMIT:
                log.debug('Fuzzy last name match for {f1} and {f2}: {fuzzy}'
                          .format(f1=lastname, f2=res_lastname, fuzzy=fuzzy_lastname_match))
                score += int((fuzzy_lastname_match - _FUZZY_LASTNAME_MATCH_LIMIT) /
                             (100 - _FUZZY_LASTNAME_MATCH_LIMIT) * 100)

            if rank and res_ranks and rank != 'tuntematon':
                if rank in res_ranks:
                    score += 25
                    if rank not in ['sotamies', 'korpraali']:
                        # More than half of the casualties have rank private and about 15% are corporals.
                        # Give points to ranks higher than these.
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

            # If both are single dates, allow one different character before penalizing
            if res_birthdates[0] and res_birthdates[0] == res_birthdates[1] and \
               fuzz.partial_ratio(res_birthdates[0], birthdate) <= 80:
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

            # If both are single dates, allow one different character before penalizing
            if res_deathdates[0] and res_deathdates[0] == res_deathdates[1] and \
               fuzz.partial_ratio(res_deathdates[0], deathdate) <= 80:
                score -= 25

            s_first1 = ' '.join(firstnames)
            s_first2 = ' '.join(res_firstnames)
            fuzzy_firstname_match = max(fuzz.partial_ratio(s_first1, s_first2),
                                        fuzz.token_sort_ratio(s_first1, s_first2, force_ascii=False),
                                        fuzz.token_set_ratio(s_first1, s_first2, force_ascii=False))

            if fuzzy_firstname_match >= _FUZZY_FIRSTNAME_MATCH_LIMIT:
                log.debug('Fuzzy first name match for {f1} and {f2}: {fuzzy}'
                          .format(f1=firstnames, f2=res_firstnames, fuzzy=fuzzy_firstname_match))
                score += int((fuzzy_firstname_match - _FUZZY_FIRSTNAME_MATCH_LIMIT) /
                             (100 - _FUZZY_FIRSTNAME_MATCH_LIMIT) * 100)
            else:
                log.debug('No fuzzy first name match for {f1} and {f2}: {fuzzy}'
                          .format(f1=firstnames, f2=res_firstnames, fuzzy=fuzzy_firstname_match))

            person['score'] = score

            if score > 210:
                filtered.append(person)

                log.info('Found matching Warsa person for {rank} {fn} {ln} {uri} : '
                         '{res_rank} {res_fn} {res_ln} {res_uri} [score: {score}]'
                         .format(rank=rank, fn=s_first1, ln=lastname, uri=s, res_rank=res_ranks, res_fn=s_first2,
                                 res_ln=res_lastname, res_uri=res_id, score=score))
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
        variations = []
        variations += ['.'.join(inner_parts)]
        variations += ['. '.join(inner_parts)]
        variations += [' '.join(inner_parts)]
        variations += [''.join(inner_parts)]
        return variations

    variation_lists = [_variations(part) + [part] for part in text.split('/')]

    combined_variations = sorted(set(['/'.join(combined).strip().replace(' /', '/')
                                      for combined in sorted(set(itertools.product(*variation_lists)))]))

    variationset = set(variation.strip() for var_list in variation_lists for variation in var_list
                       if not re.search(r'^[0-9](\.)?$', variation.strip()))

    return ' # '.join(combined_variations) + ' # ' + ' # '.join(sorted(variationset))


def link_to_military_units(graph, graph_schema, target_prop, source_prop, arpa, *args, preprocess=True, **kwargs):
    """
    Link military units to known matching military units in Warsa
    :returns dict containing some statistics and a list of errors

    :param graph: RDF graph containing the units strings to be linked
    :type graph: rdflib.Graph

    :param target_prop: target property to use for new links
    :param source_prop: source property as URIRef
    :param arpa: the Arpa instance
    """

    if preprocess:
        preprocessor = _create_unit_abbreviations
    else:
        preprocessor = None

    # Query the ARPA service and add the matches
    return process_graph(graph, target_prop, arpa, source_prop=source_prop,
                         preprocessor=preprocessor, progress=True, **kwargs)


def link_to_pnr(graph, target_prop, source_prop, arpa, *args, preprocess=True, **kwargs):
    """
    Link municipalities to Paikannimirekisteri.
    :returns dict containing some statistics and a list of errors

    :type graph: rdflib.Graph
    :param target_prop: target property to use for new links
    :param source_prop: source property as URIRef
    """

    current_municipalities = {
        'Ahlainen': 'Pori',
        'Aitolahti': 'Tampere',
        'Alahärmä': 'Kauhava',
        'Alastaro': 'Loimaa',
        'Alatornio': 'Tornio',
        'Angelniemi': 'Salo',
        'Anjala': 'Kouvola',
        'Anjalankoski': 'Kouvola',
        'Anttola': 'Mikkeli',
        'Artjärvi': 'Orimattila',
        'Askainen': 'Masku',
        'Bergö': 'Malax',
        'Björköby': 'Korsholm',
        'Bromarv': 'Raseborg',
        'Degerby': 'Inkoo',
        'Dragsfjärd': 'Kemiönsaari',
        'Elimäki': 'Kouvola',
        'Eno': 'Joensuu',
        'Eräjärvi': 'Orivesi',
        'Haapasaari': 'Helsinki',
        'Halikko': 'Salo',
        'Hauho': 'Hämeenlinna',
        'Haukipudas': 'Oulu',
        'Haukivuori': 'Mikkeli',
        'Hiittinen': 'Kimitoön',
        'Himanka': 'Kalajoki',
        'Hinnerjoki': 'Eura',
        'Honkilahti': 'Eura',
        'Huopalahti': 'Helsinki',
        'Iniö': 'Pargas',
        'Jaala': 'Kouvola',
        'Joutseno': 'Lappeenranta',
        'Jurva': 'Kurikka',
        'Jämsänkoski': 'Jämsä',
        'Jäppilä': 'Pieksämäki',
        'Kaarlela': 'Kokkola',
        'Kakskerta': 'Turku',
        'Kalanti': 'Uusikaupunki',
        'Kalvola': 'Hämeenlinna',
        'Kangaslampi': 'Varkaus',
        'Karhula': 'Kotka',
        'Karinainen': 'Pöytyä',
        'Karjala': 'Mynämäki',
        'Karjalohja': 'Lohja',
        'Karkku': 'Sastamala',
        'Karttula': 'Kuopio',
        # 'Karuna': 'Kemiönsaari', # Two new municipalities
        # 'Karuna': 'Sauvo',
        'Karunki': 'Tornio',
        'Kauvatsa': 'Kokemäki',
        'Keikyä': 'Sastamala',
        'Kerimäki': 'Savonlinna',
        'Kestilä': 'Siikalatva',
        'Kesälahti': 'Kitee',
        'Kiihtelysvaara': 'Joensuu',
        'Kiikala': 'Salo',
        'Kiikka': 'Sastamala',
        'Kiikoinen': 'Sastamala',
        'Kiiminki': 'Oulu',
        'Kisko': 'Salo',
        'Kiukainen': 'Eura',
        'Kodisjoki': 'Rauma',
        'Konginkangas': 'Äänekoski',
        'Korpilahti': 'Jyväskylä',
        'Korppoo': 'Pargas',
        'Kortesjärvi': 'Kauhava',
        'Kuhmalahti': 'Kangasala',
        'Kuivaniemi': 'Ii',
        'Kullaa': 'Ulvila',
        'Kulosaari': 'Helsinki',
        'Kuorevesi': 'Jämsä',
        'Kuru': 'Ylöjärvi',
        'Kuusankoski': 'Kouvola',
        'Kuusjoki': 'Salo',
        'Kuusjärvi': 'Outokumpu',
        'Kylmäkoski': 'Akaa',
        'Kymi': 'Kotka',
        'Kälviä': 'Kokkola',
        'Lammi': 'Hämeenlinna',
        'Lappee': 'Lappeenranta',
        'Lappi': 'Rauma',
        'Lauritsala': 'Lappeenranta',
        'Lehtimäki': 'Alajärvi',
        'Leivonmäki': 'Joutsa',
        'Lemu': 'Masku',
        'Liljendal': 'Loviisa',
        'Lohtaja': 'Kokkola',
        'Lokalahti': 'Uusikaupunki',
        'Luopioinen': 'Pälkäne',
        # 'Längelmäki': 'Jämsä', # Two new municipalities
        # 'Längelmäki': 'Orivesi',
        'Maaria': 'Turku',
        'Mellilä': 'Loimaa',
        'Merimasku': 'Naantali',
        'Messukylä': 'Tampere',
        'Metsämaa': 'Loimaa',
        'Mietoinen': 'Mynämäki',
        'Mouhijärvi': 'Sastamala',
        'Munsala': 'Nykarleby',
        'Muurla': 'Salo',
        'Muuruvesi': 'Kuopio',
        'Mänttä': 'Mänttä-Vilppula',
        'Nilsiä': 'Kuopio',
        'Noormarkku': 'Pori',
        'Nuijamaa': 'Lappeenranta',
        'Nummi': 'Lohja',
        'Nummi-Pusula': 'Lohja',
        'Nurmo': 'Seinäjoki',
        'Oulujoki': 'Oulu',
        'Oulunkylä': 'Helsinki',
        'Oulunsalo': 'Oulu',
        'Paattinen': 'Turku',
        'Paavola': 'Siikajoki',
        'Pattijoki': 'Raahe',
        'Perniö': 'Salo',
        'Pertteli': 'Salo',
        'Peräseinäjoki': 'Seinäjoki',
        'Petsamo': 'Tampere',
        'Pielisjärvi': 'Lieksa',
        'Pihlajavesi': 'Keuruu',
        'Piikkiö': 'Kaarina',
        'Piippola': 'Siikalatva',
        'Pohja': 'Raseborg',
        'Pulkkila': 'Siikalatva',
        'Punkaharju': 'Savonlinna',
        'Purmo': 'Pedersöre',
        'Pusula': 'Lohja',
        'Pyhäselkä': 'Joensuu',
        'Pylkönmäki': 'Saarijärvi',
        'Rantsila': 'Siikalatva',
        'Rautio': 'Kalajoki',
        'Renko': 'Hämeenlinna',
        'Revonlahti': 'Siikajoki',
        'Ristiina': 'Mikkeli',
        'Ruotsinpyhtää': 'Loviisa',
        'Ruukki': 'Siikajoki',
        'Rymättylä': 'Naantali',
        'Saari': 'Parikkala',
        'Sahalahti': 'Kangasala',
        'Salmi': 'Kuortane',
        'Saloinen': 'Raahe',
        'Sammatti': 'Lohja',
        'Savonranta': 'Savonlinna',
        'Simpele': 'Rautjärvi',
        'Sippola': 'Kouvola',
        'Snappertuna': 'Raseborg',
        'Somerniemi': 'Somero',
        'Sumiainen': 'Äänekoski',
        'Suodenniemi': 'Sastamala',
        'Suojärvi': 'Janakkala',
        'Suolahti': 'Äänekoski',
        'Suomenniemi': 'Mikkeli',
        'Suomusjärvi': 'Salo',
        'Suoniemi': 'Nokia',
        'Särkisalo': 'Salo',
        'Säräisniemi': 'Vaala',
        'Säynätsalo': 'Jyväskylä',
        'Sääksmäki': 'Valkeakoski',
        'Sääminki': 'Savonlinna',
        'Teisko': 'Tampere',
        'Temmes': 'Tyrnävä',
        'Toijala': 'Akaa',
        'Tottijärvi': 'Nokia',
        'Turtola': 'Pello',
        'Tuulos': 'Hämeenlinna',
        'Tuupovaara': 'Joensuu',
        'Tyrvää': 'Sastamala',
        'Töysä': 'Alavus',
        'Ullava': 'Kokkola',
        'Uskela': 'Salo',
        'Uukuniemi': 'Parikkala',
        'Vahto': 'Rusko',
        'Valkeala': 'Kouvola',
        'Vammala': 'Sastamala',
        'Vampula': 'Huittinen',
        'Vanaja': 'Hämeenlinna',
        'Varpaisjärvi': 'Lapinlahti',
        'Vehkalahti': 'Hamina',
        'Vehmersalmi': 'Kuopio',
        'Velkua': 'Naantali',
        'Vihanti': 'Raahe',
        'Viiala': 'Akaa',
        'Vilppula': 'Mänttä-Vilppula',
        'Virtasalmi': 'Pieksämäki',
        'Vuolijoki': 'Kajaani',
        'Vähäkyrö': 'Vaasa',
        'Värtsilä': 'Tohmajärvi',
        'Västanfjärd': 'Kimitoön',
        'Yli-Ii': 'Oulu',
        'Ylihärmä': 'Kauhava',
        'Ylikiiminki': 'Oulu',
        'Ylistaro': 'Seinäjoki',
        'Ylämaa': 'Lappeenranta',
        'Yläne': 'Pöytyä',
        'Äetsä': 'Sastamala',
        'Ähtävä': 'Pedersören kunta',
        'Esse Ähtävä': 'Pedersören kunta',
        'Koivulahti': 'Mustasaari',
        'Kvevlax Koivulahti': 'Mustasaari',
        # 'Sulva': 'Mustasaari', # Two new municipalities
        # 'Sulva': 'Vaasa',
        'Alaveteli': 'Kruunupyy',
        'Nedervetil Alaveteli': 'Kruunupyy',
        'Houtskari': 'Parainen',
        'Houtskär Houtskari': 'Parainen',
        'Jepua': 'Uusikaarlepyy',
        'Jeppo Jepua': 'Uusikaarlepyy',
        'Kemiö': 'Kemiönsaari',
        'Kimito Kemiö': 'Kemiönsaari',
        'Maksamaa': 'Vöyri',
        'Maxmo Maksamaa': 'Vöyri',
        'Nauvo': 'Parainen',
        'Nagu Nauvo': 'Parainen',
        'Oravainen': 'Vöyri',
        'Oravais Oravainen': 'Vöyri',
        'Pernaja': 'Loviisa',
        'Pernå Pernaja': 'Loviisa',
        'Pirttikylä': 'Närpiö',
        'Pörtom Pirttikylä': 'Närpiö',
        'Raippaluoto': 'Mustasaari',
        'Replot Raippaluoto': 'Mustasaari',
        'Siipyy': 'Kristiinankaupunki',
        'Sideby Siipyy': 'Kristiinankaupunki',
        'Tammisaari': 'Raasepori',
        'Tammisaari Ekenäs': 'Raasepori',
        'Teerijärvi': 'Kruunupyy',
        'Terjärv Teerijärvi': 'Kruunupyy',
        'Ylimarkku': 'Närpiö',
        'Övermark Ylimarkku': 'Närpiö',
        'Tiukka': 'Kristiinankaupunki',
        'Tjöck Tiukka': 'Kristiinankaupunki',
        'Petolahti': 'Maalahti',
        'Petalax Petolahti': 'Maalahti',
        'Karjaa': 'Raasepori',
        'Karjaa Karis': 'Raasepori',
        'Karjaan mlk': 'Raasepori',
        'Karjaan mlk Karis lk': 'Raasepori',
        'Hyvinkään mlk': 'Hyvinkää',
        'Haagan kauppala': 'Helsinki',
        # 'Kuopion mlk': 'Kuopio', # Two new municipalities
        # 'Kuopion mlk': 'Siilinjärvi',
        'Tammisaaren mlk': 'Raasepori',
        'Karjaan mlk': 'Raasepori',
        'Koski Hl.': 'Hollola',
        'Uusikirkko Tl': 'Uusikaupunki',
        'Tenhola': 'Raasepori',
        'Tenhola Tenala': 'Raasepori',
    }

    def _get_municipality_label(val, uri, *args2):
        """
        :param uri: municipality URI
        """
        lbl = str(graph.value(uri, URIRef('http://www.w3.org/2004/02/skos/core#prefLabel'))).replace('/', ' ')
        lbl = current_municipalities.get(lbl, lbl)
        return lbl

    if preprocess:
        preprocessor = _get_municipality_label
    else:
        preprocessor = None

    # Query the ARPA service and add the matches
    return process_graph(graph, target_prop, arpa, new_graph=True, source_prop=source_prop,
                         preprocessor=preprocessor, progress=True, **kwargs)


def link_to_warsa_persons(graph, graph_schema, target_prop, source_prop, arpa, source_lastname_prop,
                          source_firstname_prop, source_rank_prop, birthdate_prop, deathdate_prop, preprocess=False,
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
    # if preprocessor is None:
    #     preprocessor = _combine_rank_and_names
    #
    if validator is None:
        validator = Validator(graph, graph_schema, birthdate_prop, deathdate_prop,
                              source_rank_prop, source_firstname_prop, source_lastname_prop)

    # Query the ARPA service, add the matches and serialize the graph to disk.
    return process_graph(graph, target_prop, arpa, source_prop=source_prop,
                         preprocessor=preprocessor, validator=validator, progress=True, **kwargs)


def process_stage(link_function, stage, arpa_args, query_template_file=None, rank_schema_file=None, pruner=None):
    log_to_file('process.log', arpa_args.log_level)
    del arpa_args.log_level

    log.debug('Now at process_stage')

    if stage == 'join':
        process(arpa_args.input, arpa_args.fi, arpa_args.output, arpa_args.fo, arpa_args.tprop,
                source_prop=arpa_args.prop, rdf_class=arpa_args.rdf_class, new_graph=arpa_args.new_graph,
                join_candidates=True, run_arpafy=False, progress=True, pruner=pruner, prune=bool(pruner))
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
            preprocess = True

            arpa = Arpa(arpa_url, arpa_args.pop('no_duplicates'), arpa_args.pop('min_ngram'),
                        retries=arpa_args.pop('retries'), wait_between_tries=arpa_args.pop('wait'),
                        ignore=arpa_args.pop('ignore'))

        else:
            with open(query_template_file) as f:
                qry = f.read()

            schema = Graph()
            schema.parse(rank_schema_file, format=input_format)
            preprocess = False

            arpa = ArpaMimic(qry, arpa_url, arpa_args.pop('no_duplicates'), arpa_args.pop('min_ngram'),
                             retries=arpa_args.pop('retries'), wait_between_tries=arpa_args.pop('wait'),
                             ignore=arpa_args.pop('ignore'))

        result = link_function(data, schema, arpa_args.pop('tprop'), arpa_args.pop('prop'), arpa,
                               ns_schema.sukunimi, ns_schema.etunimet, ns_schema.sotilasarvo, ns_schema.syntymaeaika,
                               ns_schema.kuolinaika, preprocess=preprocess, **arpa_args)

        result['graph'].serialize(output, format=output_format)


def print_usage(exit_=True):
    print('usage: arpa.py test|(persons|units|pnr|(disambiguate query_template_file schema_ttl_file) [arpa_linker_args])')
    if exit_:
        exit()


def prune_extra_unit_candidates(obj):
    """
    Remove erroneous candidates with # in them
    >>> prune_extra_unit_candidates('3./JR 1 #3./JR. 1.')
    False
    >>> prune_extra_unit_candidates('3./JR 1')
    '3./JR 1'
    """

    return False if '#' in obj else obj

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

    link_fn = None
    prune_function = None
    target = sys.argv[1]
    stage = sys.argv[2]

    if target == 'persons':
        link_fn = link_to_warsa_persons
    elif target == 'units':
        link_fn = link_to_military_units
        if stage == 'join':
            prune_function = prune_extra_unit_candidates
    elif target == 'pnr':
        # NOT NEEDED HERE
        link_fn = link_to_pnr
    else:
        print_usage()

    if stage == 'disambiguate':
        process_stage(link_fn, stage, parse_args(sys.argv[5:]), query_template_file=sys.argv[3],
                      rank_schema_file=sys.argv[4])
    else:
        process_stage(link_fn, stage, parse_args(sys.argv[3:]), pruner=prune_function)


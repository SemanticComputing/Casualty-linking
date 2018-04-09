#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Mapping of CSV columns to RDF properties
"""
from datetime import date, datetime
from functools import partial

from rdflib import Namespace

from converters import convert_dates, strip_dash, convert_from_dict, urify
from namespaces import SCHEMA_NS, BIOC, MOTHER_TONGUE_NS, KANSALLISUUS, KANSALAISUUS, MARITAL_NS, GENDER_NS, \
    PERISHING_CLASSES_NS, KUNNAT, WARSA_NS

from validators import validate_dates, validate_mother_tongue

# CSV column mapping. Person name and person index number are taken separately.

MUNICIPALITY_PREFIX = Namespace(str(KUNNAT) + "k")


CITIZENSHIPS = {
    'ITA': KANSALAISUUS.Italia,
    'NO': KANSALAISUUS.Norja,
    'NL': KANSALAISUUS.Neuvostoliitto,
    'RU': KANSALAISUUS.Ruotsi,
    'SA': KANSALAISUUS.Saksa,
    'SU': KANSALAISUUS.Suomi,
    'FI': KANSALAISUUS.Suomi,
    'TA': KANSALAISUUS.Tanska,
    'HUN': KANSALAISUUS.Unkari,
    'IN': KANSALAISUUS.Inkeri,
    'VI': KANSALAISUUS.Viro,
    None: KANSALAISUUS.Tuntematon,
}

LANGUAGES = {
    'it': MOTHER_TONGUE_NS.Italia,
    'no': MOTHER_TONGUE_NS.Norja,
    'ru': MOTHER_TONGUE_NS.Ruotsi,
    'sa': MOTHER_TONGUE_NS.Saksa,
    'sm': MOTHER_TONGUE_NS.Saame,
    'su': MOTHER_TONGUE_NS.Suomi,
    'ta': MOTHER_TONGUE_NS.Tanska,
    'tu': MOTHER_TONGUE_NS.Turkki,
    've': MOTHER_TONGUE_NS.Venaejae,
    'vi': MOTHER_TONGUE_NS.Viro,
    None: MOTHER_TONGUE_NS.Tuntematon,
}

MARITAL_STATUSES = {
    'N': MARITAL_NS.Naimisissa,
    'Y': MARITAL_NS.Naimaton,
    'E': MARITAL_NS.Eronnut,
    'L': MARITAL_NS.Leski,
    None: MARITAL_NS.Tuntematon,
}

GENDERS = {
    'M': GENDER_NS.Mies,
    'N': GENDER_NS.Nainen,
    None: GENDER_NS.Tuntematon,
}

NATIONALITIES = {
    'ITA': KANSALLISUUS.Italia,
    'NO': KANSALLISUUS.Norja,
    'NL': KANSALLISUUS.Neuvostoliitto,
    'RU': KANSALLISUUS.Ruotsi,
    'SA': KANSALLISUUS.Saksa,
    'SU': KANSALLISUUS.Suomi,
    'FI': KANSALLISUUS.Suomi,
    'TA': KANSALLISUUS.Tanska,
    'HUN': KANSALLISUUS.Unkari,
    'IN': KANSALLISUUS.Inkeri,
    'VI': KANSALLISUUS.Viro,
    None: KANSALLISUUS.Tuntematon,
}

PERISHING_CLASSES = {
    'A': PERISHING_CLASSES_NS.A,
    'B': PERISHING_CLASSES_NS.B,
    'C': PERISHING_CLASSES_NS.C,
    'D': PERISHING_CLASSES_NS.D,
    'F': PERISHING_CLASSES_NS.F,
    'S': PERISHING_CLASSES_NS.S,
    None: PERISHING_CLASSES_NS.Tuntematon,
}


CASUALTY_MAPPING = {
    # 'ID':
    #     {
    #         'uri': NARCS.local_id,
    #         'name_fi': 'Kansallisarkiston tunniste',
    #         'name_en': 'National Archives ID',
    #         'description_fi': 'Kansallisarkiston tunniste',
    #         'description_en': 'National Archives ID',
    #     },
    'SNIMI':
        {
            'uri': WARSA_NS.family_name,
            'name_fi': 'Sukunimi',
            'name_en': 'Family name',
            'description_fi': 'Henkilön sukunimi',
            'description_en': 'Person family name',
        },
    'ENIMET':
        {
            'uri': WARSA_NS.given_names,
            'name_fi': 'Etunimet',
            'name_en': 'Given names',
            'description_fi': 'Henkilön etunimet',
            'description_en': 'Person given names',
        },
    'SSAATY':
        {
            'uri': WARSA_NS.marital_status,
            'name_fi': 'Siviilisääty',
            'name_en': 'Marital status',
            'description_fi': 'Siviilisääty',
            'description_en': 'Marital status',
            'converter': partial(convert_from_dict, MARITAL_STATUSES)
        },
    'SPUOLI':
        {
            'uri': WARSA_NS.gender,
            'name_fi': 'Sukupuoli',
            'name_en': 'Gender',
            'converter': partial(convert_from_dict, GENDERS)
        },
    'KANSALAISUUS':
        {
            'uri': WARSA_NS.citizenship,
            'name_fi': 'Kansalaisuus',
            'name_en': 'Citizenship',
            'converter': partial(convert_from_dict, CITIZENSHIPS)
        },
    'KANSALLISUUS':
        {
            'uri': WARSA_NS.nationality,
            'name_fi': 'Kansallisuus',
            'name_en': 'Nationality',
            'converter': partial(convert_from_dict, NATIONALITIES)
        },
    'AIDINKIELI':
        {
            'uri': WARSA_NS.mother_tongue,
            'name_fi': 'Äidinkieli',
            'name_en': 'Mother tongue',
            'converter': partial(convert_from_dict, LANGUAGES)
        },
    'LASTENLKM':
        {
            'uri': WARSA_NS.number_of_children,
            'name_fi': 'Lasten lukumäärä',
            'name_en': 'Number of children',
            'converter': lambda x: int(x) if x else None
        },
    'AMMATTI':
        {
            # 'uri': BIOC.has_occupation,
            'uri': SCHEMA_NS.occupation,
            'name_fi': 'Ammatti',
            'name_en': 'Occupation',
        },
    'SOTARVO':
        {
            'uri': SCHEMA_NS.rank,
            'name_fi': 'Sotilasarvo',
            'name_en': 'Military rank',
        },
    'JOSKOODI':
        {
            'uri': WARSA_NS.unit_code,
            'name_fi': 'Joukko-osastokoodi',
            'name_en': 'Military unit identification code',
        },
    'JOSNIMI':
        {
            'uri': SCHEMA_NS.unit,
            'name_fi': 'Joukko-osasto',
            'name_en': 'Military unit',
        },
    'SAIKA':
        {
            'uri': WARSA_NS.date_of_birth,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1860, 1, 1), before=date(1935, 1, 1)),
            'name_fi': 'Syntymäpäivä',
            'name_en': 'Date of birth',
        },
    'SKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_birth,
            'name_fi': 'Synnyinkunta',
            'name_en': 'Municipality of birth',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'KIRJKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_domicile,
            'name_fi': 'Kotikunta',
            'name_en': 'Municipality of domicile',
            'description_fi': 'Henkilön kirjoillaolokunta',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'ASKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_residence,
            'name_fi': 'Asuinkunta',
            'name_en': 'Municipality of residence',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'HAAVAIKA':
        {
            'uri': WARSA_NS.date_of_wounding,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_fi': 'Haavoittumispäivä',
            'name_en': 'Date of wounding',
        },
    'HAAVKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_wounding,
            'name_fi': 'Haavoittumiskunta',
            'name_en': 'Municipality of wounding',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'HAAVPAIKKA':
        {
            'uri': WARSA_NS.place_of_wounding,
            'name_fi': 'Haavoittumispaikka',
            'name_en': 'Place of wounding',
        },
    'KATOAIKA':
        {
            'uri': WARSA_NS.date_of_going_mia,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_en': 'Date of going missing in action',
            'name_fi': 'Katoamispäivä',
        },
    'KATOKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_going_mia,
            'name_fi': 'Katoamiskunta',
            'name_en': 'Municipality of going missing in action',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'KATOPAIKKA':
        {
            'uri': WARSA_NS.place_of_going_mia,
            'name_fi': 'Katoamispaikka',
            'name_en': 'Place of going missing in action',
        },
    'KUOLINAIKA':
        {
            'uri': WARSA_NS.date_of_death,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1939, 11, 30), before=date.today()),
            'name_fi': 'Kuolinpäivä',
            'name_en': 'Date of death',
        },
    'KUOLINKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_death,
            'name_en': 'Municipality of death',
            'name_fi': 'Kuolinkunta'
        },
    'KUOLINPAIKKA':
        {
            'uri': WARSA_NS.place_of_death,
            'name_fi': 'Kuolinpaikka',
            'name_en': 'Place of death',
        },
    'MENEHTLUOKKA':
        {
            'uri': SCHEMA_NS.perishing_category,
            'name_fi': 'Menehtymisluokka',
            'name_en': 'Perishing category',
            'converter': partial(convert_from_dict, PERISHING_CLASSES)
        },
    'HKUNTA':
        {
            'uri': SCHEMA_NS.municipality_of_burial,
            'name_fi': 'Hautauskunta',
            'name_en': 'Municipality of burial',
        },
    'HMAA':
        {
            'uri': SCHEMA_NS.graveyard_number,
            'name_fi': 'Hautausmaan numero',
            'name_en': 'Burial graveyard number',
        },
    'HPAIKKA':
        {
            'uri': SCHEMA_NS.place_of_burial,
            'name_fi': 'Hautapaikan numero',
            'name_en': 'Place of burial (number)',
        },
    'VAPAA_PAIKKATIETO':
        {
            # TODO: Validator to filter out some words
            'uri': SCHEMA_NS.additional_information,
            'name_fi': 'Lisätietoja',
            'name_en': 'Additional information',
        },
}

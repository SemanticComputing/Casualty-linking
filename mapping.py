#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Mapping of CSV columns to RDF properties
"""
from datetime import date, datetime
from functools import partial

from rdflib import Namespace

from converters import convert_dates, strip_dash, convert_from_dict, urify
from namespaces import NARCS, BIOC, MOTHER_TONGUE_NS, KANSALLISUUS, KANSALAISUUS, MARITAL_NS, GENDER_NS, \
    PERISHING_CLASSES_NS, KUNNAT

from validators import validate_dates, validate_mother_tongue

# CSV column mapping. Person name and person index number are taken separately.

MUN_PREFIX = Namespace(str(KUNNAT) + "k")


CITIZENSHIPS = {
    'ITA': KANSALAISUUS.Italia,
    'NO': KANSALAISUUS.Norja,
    'NL': KANSALAISUUS.Neuvostoliitto,
    'RU': KANSALAISUUS.Ruotsi,
    'SA': KANSALAISUUS.Saksa,
    'SU': KANSALLISUUS.Suomi,
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
            'uri': NARCS.sukunimi,
            'name_fi': 'Sukunimi',
            'name_en': 'Person last name',
            'description_fi': 'Henkilön sukunimi',
            'description_en': 'Person last name',
        },
    'ENIMET':
        {
            'uri': NARCS.etunimet,
            'name_fi': 'Etunimet',
            'name_en': 'First names',
            'description_fi': 'Henkilön etunimet',
            'description_en': 'Person first names',
        },
    'SSAATY':
        {
            'uri': NARCS.siviilisaesaety,
            'name_fi': 'Siviilisääty',
            'name_en': 'Marital status',
            'description_fi': 'Siviilisääty',
            'description_en': 'Marital status',
            'converter': partial(convert_from_dict, MARITAL_STATUSES)
        },
    'SPUOLI':
        {
            'uri': NARCS.sukupuoli,
            'name_fi': 'Sukupuoli',
            'name_en': 'Gender',
            'converter': partial(convert_from_dict, GENDERS)
        },
    'KANSALAISUUS':
        {
            'uri': NARCS.kansalaisuus,
            'name_fi': 'Kansalaisuus',
            'name_en': 'Citizenship',
            'converter': partial(convert_from_dict, CITIZENSHIPS)
        },
    'KANSALLISUUS':
        {
            'uri': NARCS.kansallisuus,
            'name_fi': 'Kansallisuus',
            'name_en': 'Nationality',
            'converter': partial(convert_from_dict, NATIONALITIES)
        },
    'AIDINKIELI':
        {
            'uri': NARCS.aeidinkieli,
            'name_fi': 'Äidinkieli',
            'name_en': 'Mother tongue',
            'converter': partial(convert_from_dict, LANGUAGES)
        },
    'LASTENLKM':
        {
            'uri': NARCS.lasten_lukumaeaerae,
            'name_fi': 'Lasten lukumäärä',
            'name_en': 'Amount of children',
            'converter': lambda x: int(x) if x else None
        },
    'AMMATTI':
        {
            # 'uri': BIOC.has_occupation,
            'uri': NARCS.ammatti,
            'name_fi': 'Ammatti',
            'name_en': 'Occupation',
        },
    'SOTARVO':
        {
            'uri': NARCS.sotilasarvo,
            'name_fi': 'Sotilasarvo',
            'name_en': 'Military rank',
        },
    'JOSKOODI':
        {
            'uri': NARCS.joukko_osastokoodi,
            'name_fi': 'Joukko-osastokoodi',
            'name_en': 'Military unit key',
        },
    'JOSNIMI':
        {
            'uri': NARCS.joukko_osasto,
            'name_fi': 'Joukko-osasto',
            'name_en': 'Military unit',
        },
    'SAIKA':
        {
            'uri': NARCS.syntymaeaika,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1860, 1, 1), before=date(1935, 1, 1)),
            'name_fi': 'Syntymäaika',
            'name_en': 'Date of birth',
        },
    'SKUNTA':
        {
            'uri': NARCS.synnyinkunta,
            'name_fi': 'Synnyinkunta',
            'name_en': 'Municipality of birth',
            'converter': partial(urify, MUN_PREFIX),
        },
    'KIRJKUNTA':
        {
            'uri': NARCS.kotikunta,
            'name_fi': 'Kotikunta',
            'name_en': 'Place of domicile',
            'converter': partial(urify, MUN_PREFIX),
        },
    'ASKUNTA':
        {
            'uri': NARCS.asuinkunta,
            'name_fi': 'Asuinkunta',
            'name_en': 'Principal abode',
            'converter': partial(urify, MUN_PREFIX),
        },
    'HAAVAIKA':
        {
            'uri': NARCS.haavoittumisaika,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_fi': 'Haavoittumisaika',
            'name_en': 'Wounding date',
        },
    'HAAVKUNTA':
        {
            'uri': NARCS.haavoittumiskunta,
            'name_fi': 'Haavoittumiskunta',
            'name_en': 'Wounding municipality',
            'converter': partial(urify, MUN_PREFIX),
        },
    'HAAVPAIKKA':
        {
            'uri': NARCS.haavoittumispaikka,
            'name_fi': 'Haavoittumispaikka',
            'name_en': 'Wounding place',
        },
    'KATOAIKA':
        {
            'uri': NARCS.time_gone_missing,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_en': 'Date of going missing',
            'name_fi': 'Katoamispäivä',
        },
    'KATOKUNTA':
        {
            'uri': NARCS.katoamiskunta,
            'name_fi': 'Katoamiskunta',
            'name_en': 'Municipality of going missing',
            'converter': partial(urify, MUN_PREFIX),
        },
    'KATOPAIKKA':
        {
            'uri': NARCS.katoamispaikka,
            'name_fi': 'Katoamispaikka',
            'name_en': 'Place of going missing',
        },
    'KUOLINAIKA':
        {
            'uri': NARCS.death_date,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1939, 11, 30), before=date.today()),
            'name_fi': 'Kuolinpäivä',
            'name_en': 'Date of death',
        },
    'KUOLINPAIKKA':
        {
            'uri': NARCS.kuolinpaikka,
            'name_fi': 'Kuolinpaikka',
            'name_en': 'Place of death',
        },
    'MENEHTLUOKKA':
        {
            'uri': NARCS.menehtymisluokka,
            'name_fi': 'Menehtymisluokka',
            'name_en': 'Perishing class',
            'converter': partial(convert_from_dict, PERISHING_CLASSES)
        },
    'HKUNTA':
        {
            'uri': NARCS.hautauskunta_id,
            'name_fi': 'Hautauskunta',
            'name_en': 'Burial municipality',
        },
    'HMAA':
        {
            'uri': NARCS.hautausmaa_nro,
            'name_fi': 'Hautausmaa',
            'name_en': 'Burial graveyard',
        },
    'HPAIKKA':
        {
            'uri': NARCS.hautapaikka,
            'name_fi': 'Hautapaikka',
            'name_en': 'Burial place',
        },
    'VAPAA_PAIKKATIETO':
        {
            'uri': NARCS.additional_information,
            'name_fi': 'Lisätieto',
            'name_en': 'Additional information',
        },
}

#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Mapping of CSV columns to RDF properties
"""
from datetime import date, datetime
from functools import partial

from rdflib import Namespace

from converters import convert_dates, strip_dash, convert_from_dict, urify, filter_additional_information
from namespaces import SCHEMA_CAS, BIOC, MOTHER_TONGUES, NATIONALITIES, CITIZENSHIPS, MARITAL_STATUSES, GENDERS, \
    PERISHING_CLASSES, MUNICIPALITIES, SCHEMA_WARSA

from validators import validate_dates, validate_mother_tongue

# CSV column mapping. Person name and person index number are taken separately.

GRAVEYARD_MAPPING = {
    'http://ldf.fi/warsa/places/cemeteries/h0520_1':
    'http://ldf.fi/warsa/places/cemeteries/h0929_1',  # Pieksämäki
    'http://ldf.fi/warsa/places/cemeteries/h0135_1':
    'http://ldf.fi/warsa/places/cemeteries/h0927_1',  # Laitila
}

MUNICIPALITY_PREFIX = Namespace(str(MUNICIPALITIES) + "k")

CITIZENSHIPS = {
    'ITA': CITIZENSHIPS.Italia,
    'NO': CITIZENSHIPS.Norja,
    'NL': CITIZENSHIPS.Neuvostoliitto,
    'RU': CITIZENSHIPS.Ruotsi,
    'SA': CITIZENSHIPS.Saksa,
    'SU': CITIZENSHIPS.Suomi,
    'FI': CITIZENSHIPS.Suomi,
    'TA': CITIZENSHIPS.Tanska,
    'HUN': CITIZENSHIPS.Unkari,
    'IN': CITIZENSHIPS.Inkeri,
    'VI': CITIZENSHIPS.Viro,
    None: CITIZENSHIPS.Tuntematon,
}

LANGUAGES = {
    'it': MOTHER_TONGUES.Italia,
    'no': MOTHER_TONGUES.Norja,
    'ru': MOTHER_TONGUES.Ruotsi,
    'sa': MOTHER_TONGUES.Saksa,
    'sm': MOTHER_TONGUES.Saame,
    'su': MOTHER_TONGUES.Suomi,
    'ta': MOTHER_TONGUES.Tanska,
    'tu': MOTHER_TONGUES.Turkki,
    've': MOTHER_TONGUES.Venaejae,
    'vi': MOTHER_TONGUES.Viro,
    None: MOTHER_TONGUES.Tuntematon,
}

MARITAL_STATUSES = {
    'N': MARITAL_STATUSES.Naimisissa,
    'Y': MARITAL_STATUSES.Naimaton,
    'E': MARITAL_STATUSES.Eronnut,
    'L': MARITAL_STATUSES.Leski,
    None: MARITAL_STATUSES.Tuntematon,
}

GENDERS = {
    'M': GENDERS.Mies,
    'F': GENDERS.Nainen,
    None: GENDERS.Tuntematon,
}

NATIONALITIES = {
    'ITA': NATIONALITIES.Italia,
    'NO': NATIONALITIES.Norja,
    'NL': NATIONALITIES.Neuvostoliitto,
    'RU': NATIONALITIES.Ruotsi,
    'SA': NATIONALITIES.Saksa,
    'SU': NATIONALITIES.Suomi,
    'FI': NATIONALITIES.Suomi,
    'TA': NATIONALITIES.Tanska,
    'HUN': NATIONALITIES.Unkari,
    'IN': NATIONALITIES.Inkeri,
    'VI': NATIONALITIES.Viro,
    None: NATIONALITIES.Tuntematon,
}

PERISHING_CLASSES = {
    'A': PERISHING_CLASSES.A,
    'B': PERISHING_CLASSES.B,
    'C': PERISHING_CLASSES.C,
    'D': PERISHING_CLASSES.D,
    'F': PERISHING_CLASSES.F,
    'S': PERISHING_CLASSES.S,
    None: PERISHING_CLASSES.Tuntematon,
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
            'uri': SCHEMA_WARSA.family_name,
            'name_fi': 'Sukunimi',
            'name_en': 'Family name',
            'description_fi': 'Henkilön sukunimi',
            'description_en': 'Person family name',
        },
    'ENIMET':
        {
            'uri': SCHEMA_WARSA.given_names,
            'name_fi': 'Etunimet',
            'name_en': 'Given names',
            'description_fi': 'Henkilön etunimet',
            'description_en': 'Person given names',
        },
    'SSAATY':
        {
            'uri': SCHEMA_WARSA.marital_status,
            'name_fi': 'Siviilisääty',
            'name_en': 'Marital status',
            'description_fi': 'Siviilisääty',
            'description_en': 'Marital status',
            'converter': partial(convert_from_dict, MARITAL_STATUSES)
        },
    'SPUOLI':
        {
            'uri': SCHEMA_WARSA.gender,
            'name_fi': 'Sukupuoli',
            'name_en': 'Gender',
            'converter': partial(convert_from_dict, GENDERS)
        },
    'KANSALAISUUS':
        {
            'uri': SCHEMA_WARSA.citizenship,
            'name_fi': 'Kansalaisuus',
            'name_en': 'Citizenship',
            'converter': partial(convert_from_dict, CITIZENSHIPS)
        },
    'KANSALLISUUS':
        {
            'uri': SCHEMA_WARSA.nationality,
            'name_fi': 'Kansallisuus',
            'name_en': 'Nationality',
            'converter': partial(convert_from_dict, NATIONALITIES)
        },
    'AIDINKIELI':
        {
            'uri': SCHEMA_WARSA.mother_tongue,
            'name_fi': 'Äidinkieli',
            'name_en': 'Mother tongue',
            'converter': partial(convert_from_dict, LANGUAGES)
        },
    'LASTENLKM':
        {
            'uri': SCHEMA_WARSA.number_of_children,
            'name_fi': 'Lasten lukumäärä',
            'name_en': 'Number of children',
            'converter': lambda x: int(x) if x.isnumeric() else None
        },
    'AMMATTI':
        {
            'uri': SCHEMA_WARSA.occupation_literal,
            'name_fi': 'Ammatti',
            'name_en': 'Occupation',
        },
    'SOTARVO':
        {
            'uri': SCHEMA_CAS.rank_literal,
            'name_fi': 'Sotilasarvo',
            'name_en': 'Military rank',
        },
    'JOSKOODI':
        {
            'uri': SCHEMA_CAS.unit_code,
            'name_fi': 'Joukko-osaston peiteluku',
            'name_en': 'Military unit identification code',
            'description_fi': 'Henkilön kuolinhetken joukko-osaston peiteluku',
        },
    'JOSNIMI':
        {
            'uri': SCHEMA_CAS.unit_literal,
            'name_fi': 'Joukko-osasto',
            'name_en': 'Military unit',
            'description_fi': 'Henkilön joukko-osasto kuolinhetkellä',
        },
    'SAIKA':
        {
            'uri': SCHEMA_WARSA.date_of_birth,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1860, 1, 1), before=date(1935, 1, 1)),
            'name_fi': 'Syntymäpäivä',
            'name_en': 'Date of birth',
        },
    'SKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_birth,
            'name_fi': 'Synnyinkunta',
            'name_en': 'Municipality of birth',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'KIRJKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_domicile,
            'name_fi': 'Kotikunta',
            'name_en': 'Municipality of domicile',
            'description_fi': 'Henkilön kirjoillaolokunta',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'ASKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_residence,
            'name_fi': 'Asuinkunta',
            'name_en': 'Municipality of residence',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'HAAVAIKA':
        {
            'uri': SCHEMA_WARSA.date_of_wounding,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_fi': 'Haavoittumispäivä',
            'name_en': 'Date of wounding',
        },
    'HAAVKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_wounding,
            'name_fi': 'Haavoittumiskunta',
            'name_en': 'Municipality of wounding',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'HAAVPAIKKA':
        {
            'uri': SCHEMA_WARSA.place_of_wounding,
            'name_fi': 'Haavoittumispaikka',
            'name_en': 'Place of wounding',
        },
    'KATOAIKA':
        {
            'uri': SCHEMA_WARSA.date_of_going_mia,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_en': 'Date of going missing in action',
            'name_fi': 'Katoamispäivä',
        },
    'KATOKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_going_mia,
            'name_fi': 'Katoamiskunta',
            'name_en': 'Municipality of going missing in action',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'KATOPAIKKA':
        {
            'uri': SCHEMA_WARSA.place_of_going_mia_literal,
            'name_fi': 'Katoamispaikka',
            'name_en': 'Place of going missing in action',
        },
    'KUOLINAIKA':
        {
            'uri': SCHEMA_WARSA.date_of_death,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1939, 11, 30), before=date.today()),
            'name_fi': 'Kuolinpäivä',
            'name_en': 'Date of death',
        },
    'KUOLINKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_death,
            'name_en': 'Municipality of death',
            'name_fi': 'Kuolinkunta',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'KUOLINPAIKKA':
        {
            'uri': SCHEMA_WARSA.place_of_death_literal,
            'name_fi': 'Kuolinpaikka',
            'name_en': 'Place of death',
        },
    'MENEHTLUOKKA':
        {
            'uri': SCHEMA_CAS.perishing_category,
            'name_fi': 'Menehtymisluokka',
            'name_en': 'Perishing category',
            'converter': partial(convert_from_dict, PERISHING_CLASSES)
        },
    'HKUNTA':
        {
            'uri': SCHEMA_CAS.municipality_of_burial,
            'name_fi': 'Hautauskunta',
            'name_en': 'Municipality of burial',
            'converter': partial(urify, MUNICIPALITY_PREFIX),
        },
    'HMAA':
        {
            'uri': SCHEMA_CAS.graveyard_number,
            'name_fi': 'Hautausmaan numero',
            'name_en': 'Burial graveyard number',
        },
    'HPAIKKA':
        {
            'uri': SCHEMA_CAS.place_of_burial_number,
            'name_fi': 'Hautapaikan numero',
            'name_en': 'Place of burial (number)',
        },
    'VAPAA_PAIKKATIETO':
        {
            'uri': SCHEMA_CAS.additional_information,
            'name_fi': 'Lisätietoja',
            'name_en': 'Additional information',
            'converter': filter_additional_information,
        },
}

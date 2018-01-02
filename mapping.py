#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Mapping of CSV columns to RDF properties
"""
from datetime import date, datetime
from functools import partial

from converters import convert_dates, strip_dash, convert_swedish
from namespaces import SCHEMA_NS, BIOC

from validators import validate_dates, validate_mother_tongue

# CSV column mapping. Person name and person index number are taken separately.

PRISONER_MAPPING = {
    'ID':
        {
            'uri': SCHEMA_NS.local_id,
            'name_fi': 'Kansallisarkiston tunniste',
            'name_en': 'National Archives ID',
            'description_fi': 'Kansallisarkiston tunniste',
            'description_en': 'National Archives ID',
        },
    'SNIMI':
        {
            'uri': SCHEMA_NS.sukunimi,
            'name_fi': 'Sukunimi',
            'name_en': 'Person last name',
            'description_fi': 'Henkilön sukunimi',
            'description_en': 'Person last name',
        },
    'ENIMET':
        {
            'uri': SCHEMA_NS.etunimet,
            'name_fi': 'Etunimet',
            'name_en': 'First names',
            'description_fi': 'Henkilön etunimet',
            'description_en': 'Person first names',
        },
    'SSAATY':
        {
            'uri': SCHEMA_NS.siviilisaesaety,
            'name_fi': 'Siviilisääty',
            'name_en': 'Marital status',
            'description_fi': 'Siviilisääty',
            'description_en': 'Marital status',
        },
    'SPUOLI':
        {
            'uri': SCHEMA_NS.sukupuoli,
            'name_fi': 'Sukupuoli',
            'name_en': 'Gender',
        },
    'KANSALAISUUS':
        {
            'uri': SCHEMA_NS.kansalaisuus,
            'name_fi': 'Kansalaisuus',
            'name_en': 'Citizenship',
        },
    'KANSALLISUUS':
        {
            'uri': SCHEMA_NS.kansallisuus,
            'name_fi': 'Kansallisuus',
            'name_en': 'Nationality',
        },
    'AIDINKIELI':
        {
            'uri': SCHEMA_NS.aeidinkieli,
            'name_fi': 'Äidinkieli',
            'name_en': 'Mother tongue',
        },
    'LASTENLKM':
        {
            'uri': SCHEMA_NS.lasten_lukumaeaerae,
            'name_fi': 'Lasten lukumäärä',
            'name_en': 'Amount of children',
        },
    'AMMATTI':
        {
            'uri': BIOC.has_occupation,
            'name_fi': 'Ammatti',
            'name_en': 'Occupation',
        },
    'SOTARVO':
        {
            'uri': SCHEMA_NS.sotilasarvo,
            'name_fi': 'Sotilasarvo',
            'name_en': 'Military rank',
        },
    'JOSKOODI':
        {
            'uri': SCHEMA_NS.joukko_osastokoodi,
            'name_fi': 'Joukko-osastokoodi',
            'name_en': 'Military unit key',
        },
    'JOSNIMI':
        {
            'uri': SCHEMA_NS.joukko_osasto,
            'name_fi': 'Joukko-osasto',
            'name_en': 'Military unit',
        },
    'SAIKA':
        {
            'uri': SCHEMA_NS.syntymaeaika,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1860, 1, 1), before=date(1935, 1, 1)),
            'name_fi': 'Syntymäaika',
            'name_en': 'Date of birth',
        },
    'SKUNTA':
        {
            'uri': SCHEMA_NS.synnyinkunta,
            'name_fi': 'Synnyinkunta',
            'name_en': 'Municipality of birth',
        },
    'KIRJKUNTA':
        {
            'uri': SCHEMA_NS.kotikunta,
            'name_fi': 'Kotikunta',
            'name_en': 'Place of domicile',
        },
    'ASKUNTA':
        {
            'uri': SCHEMA_NS.asuinkunta,
            'name_fi': 'Asuinkunta',
            'name_en': 'Principal abode',
        },
    'HAAVAIKA':
        {
            'uri': SCHEMA_NS.haavoittumisaika,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_fi': 'Haavoittumisaika',
            'name_en': 'Wounding date',
        },
    'HAAVKUNTA':
        {
            'uri': SCHEMA_NS.haavoittumiskunta,
            'name_fi': 'Haavoittumiskunta',
            'name_en': 'Wounding municipality',
        },
    'HAAVPAIKKA':
        {
            'uri': SCHEMA_NS.haavoittumispaikka,
            'name_fi': 'Haavoittumispaikka',
            'name_en': 'Wounding place',
        },
    'KATOAIKA':
        {
            'uri': SCHEMA_NS.time_gone_missing,
            'converter': convert_dates,
            'validator': validate_dates,
            'name_en': 'Date of going missing',
            'name_fi': 'Katoamispäivä',
        },
    'KATOKUNTA':
        {
            'uri': SCHEMA_NS.katoamiskunta,
            'name_fi': 'Katoamiskunta',
            'name_en': 'Municipality of going missing',
        },
    'KATOPAIKKA':
        {
            'uri': SCHEMA_NS.katoamispaikka,
            'name_fi': 'Katoamispaikka',
            'name_en': 'Place of going missing',
        },
    'KUOLINAIKA':
        {
            'uri': SCHEMA_NS.death_date,
            'converter': convert_dates,
            'validator': partial(validate_dates, after=date(1939, 11, 30), before=date.today()),
            'name_fi': 'Kuolinpäivä',
            'name_en': 'Date of death',
        },
    'KUOLINPAIKKA':
        {
            'uri': SCHEMA_NS.kuolinpaikka,
            'name_fi': 'Kuolinpaikka',
            'name_en': 'Place of death',
        },
    'MENEHTLUOKKA':
        {
            'uri': SCHEMA_NS.menehtymisluokka,
            'name_fi': 'Menehtymisluokka',
            'name_en': 'Perishing class',
        },
    'HKUNTA':
        {
            'uri': SCHEMA_NS.hautauskunta,
            'name_fi': 'Hautauskunta',
            'name_en': 'Burial municipality',
        },
    'HMAA':
        {
            'uri': SCHEMA_NS.hautausmaa,
            'name_fi': 'Hautausmaa',
            'name_en': 'Burial graveyard',
        },
    'HPAIKKA':
        {
            'uri': SCHEMA_NS.hautapaikka,
            'name_fi': 'Hautapaikka',
            'name_en': 'Burial place',
        },
    'VAPAA_PAIKKATIETO':
        {
            'uri': SCHEMA_NS.additional_information,
            'name_fi': 'Lisätieto',
            'name_en': 'Additional information',
        },
}

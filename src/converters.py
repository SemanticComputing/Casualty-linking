#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Converters for CSV cell data
"""

import datetime
import logging
import re

from rdflib import Graph, Literal
from slugify import slugify

from namespaces import *


log = logging.getLogger(__name__)

DISALLOWED_ADDITIONAL_INFORMATION = ['kuolemanrangaistus', 'teloitettu', 'ammuttu']


def convert_dates(raw_date: str):
    """
    Convert date string to iso8601 date

    :param raw_date: raw date string from the CSV
    :return: ISO 8601 compliant date if can be parse, otherwise original date string
    """
    if not raw_date:
        return raw_date

    if not set(raw_date.replace('.', '').lower()) - {'x'}:
        log.info('Removing reference to unknown date: %s' % raw_date)
        return

    # Corrections based on manual inspection of erroneous dates
    datestr = str(raw_date).strip().replace('O', '0').replace(',', '.')
    datestr = datestr.replace('26.02.0194', '26.02.1944')
    datestr = datestr.replace('03.07.0194', '03.07.1944')
    datestr = datestr.replace('13.09.0194', '13.09.1943')
    datestr = datestr.replace('18.09.0041', '18.09.1941')
    datestr = datestr.replace('16.12.0199', '16.12.1939')

    try:
        date = datetime.datetime.strptime(datestr, '%d.%m.%Y').date()

        if str(date.year).rjust(4, '0')[:2] in ['09', '10']:
            date = datetime.date(int('19' + str(date.year)[2:]), date.month, date.day)

    except ValueError:
        if datestr[:2].lower() != 'xx':
            log.warning('Invalid value for date conversion: %s' % datestr)
        else:
            log.debug('Invalid value for date conversion: %s' % datestr)

        date = datestr

    return date


def convert_person_name(raw_name: str):
    """
    Unify name syntax and split into first names and last name

    :param raw_name: Original name string
    :return: tuple containing first names, last name and full name
    """
    re_name_split = \
        r'([A-ZÅÄÖÜÉÓÁ/\-]+(?:\s+\(?E(?:NT)?[\.\s]+[A-ZÅÄÖÜÉÓÁ/\-]+)?\)?)\s*(?:(VON))?,?\s*([A-ZÅÄÖÜÉÓÁ/\- \(\)0-9,.]*)'

    fullname = raw_name.upper()

    namematch = re.search(re_name_split, fullname)
    (lastname, extra, firstnames) = namematch.groups() if namematch else (fullname, None, '')

    # Unify syntax for previous names
    prev_name_regex = r'([A-ZÅÄÖÜÉÓÁ/\-]{2}) +\(?(E(?:NT)?[\.\s]+)([A-ZÅÄÖÜÉÓÁ/\-]+)\)?'
    lastname = re.sub(prev_name_regex, r'\1 (ent. \3)', str(lastname))

    lastname = lastname.title().replace('(Ent. ', '(ent. ')
    firstnames = firstnames.title()

    if extra:
        extra = extra.lower()
        lastname = ' '.join([extra, lastname])

    fullname = lastname

    if firstnames:
        fullname += ', ' + firstnames

    log.debug('Name %s was unified to form %s' % (raw_name, fullname))

    return firstnames, lastname, fullname


def strip_dash(raw_value: str):
    return '' if raw_value.strip() == '-' else raw_value


def convert_from_dict(dict, language: str):
    return dict.get(language, dict[None])


def urify(namespace: Namespace, value: str):
    if value:
        return namespace[value]


def filter_additional_information(raw_value: str):
    for disallowed_value in DISALLOWED_ADDITIONAL_INFORMATION:
        if raw_value.lower() in disallowed_value:
            return ''

    return raw_value


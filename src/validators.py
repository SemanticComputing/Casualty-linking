#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Converters for CSV cell data
"""

from datetime import date
import logging


log = logging.getLogger(__name__)


def validate_dates(resolved, original, after=date(1939, 11, 28), before=date(1945, 4, 25)):
    """
    Validate that a date occurs within given date range. Defaults to war time dates.

    :param resolved: Resolved date (datetime)
    :param original: Original date string
    :param after:
    :param before:
    :return: error string in finnish
    """
    if not resolved:
        return

    if type(resolved) == str:
        if resolved[:2] != 'xx':
            return 'Päivämäärä ei ole kelvollinen'
        else:
            return

    if resolved < after:
        return 'Päivämäärä liian varhainen'

    if resolved > before:
        return 'Päivämäärä liian myöhäinen'

    return


def validate_person_name(resolved, original):
    if resolved.lower() != original.lower():
        # log.warning('New name %s differs from %s' % (resolved, original))
        return 'Tulkittu nimi [%s] poikkeaa alkuperäisestä' % resolved

    return


def validate_mother_tongue(resolved, original):
    if original.strip() and original.upper() != 'X':
        return 'Epäselvä arvo'

    return

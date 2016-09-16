#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
'''
Create an ontology based on literal RDF data
'''
import argparse

import logging

from rdflib import *
from slugify import slugify

logging.basicConfig(filename='ontologizer.log', filemode='a', level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)

log.debug('Starting ontology creation')

argparser = argparse.ArgumentParser(description="Create flat ontology based on input file and property",
                                    fromfile_prefix_chars='@')

argparser.add_argument("input", help="Input RDF data file")
argparser.add_argument("output", help="Output RDF data file")
argparser.add_argument("output_schema", help="Output RDF schema file")
argparser.add_argument("property", metavar="SOURCE_PROPERTY", help="Property used in input file")
argparser.add_argument("tproperty", metavar="TARGET_PROPERTY", help="Target property for output file")
argparser.add_argument("tclass", metavar="TARGET_CLASS", help="Target class for target property values")
argparser.add_argument("tnamespace", metavar="TARGET_NAMESPACE", help="Namespace for target values")

argparser.add_argument("--remove", dest='remove', action='store_true', default=False,
                       help="Remove original property triples")

argparser.add_argument("--format", default='turtle', type=str,
                       help="Format of RDF files [default: turtle]")

argparser.add_argument("--mapping", metavar='FILE', type=str,
                       help="File containing value mappings")

args = argparser.parse_args()

ns_target = Namespace(args.tnamespace)
input = Graph().parse(args.input, format=args.format)

log.debug('Parsed input file')

output_schema = Graph()
output = Graph()
onto = Graph()

for (sub, obj) in input.subject_objects(URIRef(args.property)):
    new_obj = ns_target[slugify(obj)]
    output.add((sub, URIRef(args.tproperty), new_obj))
    output_schema.add((new_obj, RDF.type, URIRef(args.tclass)))

output.serialize(format=args.format, destination=args.output)
output_schema.serialize(format=args.format, destination=args.output_schema)

log.debug('Serialized output files')

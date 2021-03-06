#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-

import argparse
import datetime
import logging
import pandas as pd

from rdflib import URIRef, Graph, Literal, RDF, XSD
from mapping import CASUALTY_MAPPING, GRAVEYARD_MAPPING
from namespaces import DCT, SKOS, SCHEMA_CAS, SCHEMA_WARSA, bind_namespaces, CEMETERIES, DATA_CAS


class RDFMapper:
    """
    Map tabular data (currently pandas DataFrame) to RDF. Create a class instance of each row.
    """

    def __init__(self, mapping, instance_class, cemeteries=(), loglevel='WARNING'):
        self.mapping = mapping
        self.instance_class = instance_class
        self.table = None
        self.data = Graph()
        self.schema = Graph()
        # self.errors = pd.DataFrame(columns=['nro', 'sarake', 'virhe', 'arvo'])
        self.errors = []
        self.cemeteries = cemeteries

        logging.basicConfig(filename='casualties.log',
                            filemode='a',
                            level=getattr(logging, loglevel),
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.log = logging.getLogger(__name__)

    def map_row_to_rdf(self, entity_uri, row, person_id=None):
        """
        Map a single row to RDF.

        :param entity_uri: URI of the instance being created
        :param row: tabular data
        :param person_id:
        :return:
        """
        row_rdf = Graph()
        row_errors = []

        # Loop through the mapping dict and convert data to RDF
        for column_name in self.mapping:

            mapping = self.mapping[column_name]
            value = row[column_name]

            value = str(value).strip()
            conv_error = None
            original_value = value

            converter = mapping.get('converter')
            validator = mapping.get('validator')
            value = converter(value) if converter else value
            conv_error = validator(value, original_value) if validator else None

            name = ' '.join(row[1:3])

            if conv_error:
                row_errors.append([person_id, name, column_name, conv_error, original_value])

            if value not in [None, '']:
                if type(value) == datetime.date:
                    rdf_value = Literal(value, datatype=XSD.date)
                elif type(value) == URIRef:
                    rdf_value = value
                else:
                    rdf_value = Literal(value)

                if mapping.get('value_uri_base'):
                    rdf_value = URIRef(mapping['value_uri_base'] + value)

                row_rdf.add((entity_uri, mapping['uri'], rdf_value))

        if row_rdf:
            row_rdf.add((entity_uri, RDF.type, self.instance_class))
            row_rdf = self.convert_graveyards(entity_uri, row_rdf)
        else:
            # Don't create class instance if there is no data about it
            logging.debug('No data found for {uri}'.format(uri=entity_uri))
            row_errors.append([person_id, name, '', 'Ei tietoa henkilöstä', ''])

        for error in row_errors:
            self.errors.append(error)

        return row_rdf

    def convert_graveyards(self, uri, graph: Graph):
        """
        Convert graveyard information into URIs. Check if the created URI exists in cemeteries ontology.
        """
        mun = graph.value(uri, SCHEMA_CAS.municipality_of_burial)
        if not mun or str(mun) == 'X':
            return graph

        gy = graph.value(uri, SCHEMA_CAS.graveyard_number)
        gy_uri = '{base}h{mun}'.format(base=CEMETERIES, mun=str(mun).split('/k')[-1])
        # mun_uri = '{base}k{mun}'.format(base=KUNNAT, mun=mun)
        if gy:
            gy_uri += '_{gy}'.format(gy=gy)
        else:
            return graph

        gy_uri = URIRef(GRAVEYARD_MAPPING.get(gy_uri, gy_uri))

        if gy_uri not in self.cemeteries:
            logging.info('Cemetery {gy} not found for person {p}'.format(gy=gy_uri, p=uri))
            return graph

        if str(gy).isnumeric():
            graph.add((uri, SCHEMA_WARSA.buried_in, gy_uri))

        graph.remove((uri, SCHEMA_CAS.graveyard_number, gy))

        return graph

    def read_csv(self, csv_input):
        """
        Read in a CSV files using pandas.read_csv

        :param csv_input: CSV input (filename or buffer)
        """
        def strip_upper(value):
            return value.strip().upper() if value else None

        def stripper(value):
            return value.strip() if value != '' else None

        def x_stripper(value):
            return value.strip() if value.strip() not in ['x', ''] else None

        csv_data = pd.read_csv(csv_input, encoding='UTF-8', index_col=False, sep=',', quotechar='"',
                               # parse_dates=[1], infer_datetime_format=True, dayfirst=True,
                               na_values=[' '],
                               converters={
                                   'AMMATTI': lambda x: x.lower().strip(),
                                   'ASKUNTA': x_stripper,
                                   'KIRJKUNTA': x_stripper,
                                   'HAAVKUNTA': x_stripper,
                                   'KATOKUNTA': x_stripper,
                                   'KUOLINKUNTA': x_stripper,
                                   'SKUNTA': x_stripper,
                                   'HKUNTA': x_stripper,
                                   'HMAA': stripper,
                                   'HPAIKKA': stripper,
                                   'KANSALLISUUS': strip_upper,
                                   'KANSALAISUUS': strip_upper,
                                   'LASTENLKM': stripper,
                                   'JOSKOODI': stripper,
                                   'JOSNIMI': stripper,
                                   # 0: lambda x: int(x) if x and x.isnumeric() else -1
                               })

        self.table = csv_data.fillna('').applymap(lambda x: x.strip() if type(x) == str else x)
        logging.info('Read {num} rows from CSV'.format(num=len(self.table)))
        self.log.info('Data read from CSV %s' % csv_input)

    def serialize(self, destination_data, destination_schema):
        """
        Serialize RDF graphs

        :param destination_data: serialization destination for data
        :param destination_schema: serialization destination for schema
        :return: output from rdflib.Graph.serialize
        """
        bind_namespaces(self.data)
        bind_namespaces(self.schema)

        data = self.data.serialize(format="turtle", destination=destination_data)
        schema = self.schema.serialize(format="turtle", destination=destination_schema)
        self.log.info('Data serialized to %s' % destination_data)
        self.log.info('Schema serialized to %s' % destination_schema)

        return data, schema  # Return for testing purposes

    def process_rows(self):
        """
        Loop through CSV rows and convert them to RDF
        """
        for index in self.table.index:
            person_id = self.table.ix[index][0]
            person_uri = DATA_CAS['p' + str(person_id)]
            row_rdf = self.map_row_to_rdf(person_uri, self.table.ix[index][1:], person_id=person_id)
            if row_rdf:
                self.data += row_rdf

        for prop in self.mapping.values():
            self.schema.add((prop['uri'], RDF.type, RDF.Property))
            if 'name_fi' in prop:
                self.schema.add((prop['uri'], SKOS.prefLabel, Literal(prop['name_fi'], lang='fi')))
            if 'name_en' in prop:
                self.schema.add((prop['uri'], SKOS.prefLabel, Literal(prop['name_en'], lang='en')))
            if 'description_fi' in prop:
                self.schema.add((prop['uri'], DCT.description, Literal(prop['description_fi'], lang='fi')))

        error_df = pd.DataFrame(columns=['nro', 'nimi', 'sarake', 'virhe', 'arvo'], data=self.errors)
        error_df.to_csv('output/errors.csv', ',', index=False)


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(description="Process casualties CSV", fromfile_prefix_chars='@')

    argparser.add_argument("input", help="Input CSV file")
    argparser.add_argument("cemeteries", help="Input cemeteries turtle file")
    argparser.add_argument("--outdata", help="Output file to serialize RDF dataset to (.ttl)", default=None)
    argparser.add_argument("--outschema", help="Output file to serialize RDF schema to (.ttl)", default=None)
    argparser.add_argument("--loglevel", default='INFO', help="Logging level, default is INFO.",
                           choices=["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    args = argparser.parse_args()

    cemetery_uris = list(Graph().parse(args.cemeteries, format='turtle').subjects())
    mapper = RDFMapper(CASUALTY_MAPPING, SCHEMA_WARSA.DeathRecord, cemeteries=cemetery_uris,
                       loglevel=args.loglevel.upper())
    mapper.read_csv(args.input)

    mapper.process_rows()

    mapper.serialize(args.outdata, args.outschema)

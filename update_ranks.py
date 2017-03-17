#!/usr/bin/env python3
#  -*- coding: UTF-8 -*-
"""
Update data/surma_ranks.ttl with ranks from WarSampo endpoint
"""

import rdf_dm as r

ranks = r.read_graph_from_sparql("http://ldf.fi/warsa/sparql", 'http://ldf.fi/warsa/actors/ranks')
ranks.serialize(format="turtle", destination="data/surma_ranks.ttl")

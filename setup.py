import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="sotasampo_helpers",
    version="0.0.1",
    author="Mikko Koho",
    author_email="mikko.koho@aalto.fi",
    description="Sotasampo helper functions",
    license="MIT",
    keywords="rdf",
    url="",
    long_description=read('README'),
    packages=['sotasampo_helpers'],
    install_requires=[
        'rdflib >= 4.2.1',
    ],
)

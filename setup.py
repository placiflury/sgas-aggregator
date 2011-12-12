from distutils.core import setup


setup(
    name = "sgas_aggregator",
    version = "0.4.1",
    description = "An aggregator of the SGAS accouting records.",
    long_description = """
    This module provides a daemon that aggregates SGAS accounting records
    as collected by the SGAS LUTS into a set of aggregates. The aggregates
    are stored in database tables that (should) allow retrieval for several
    representations of the data wihtout need for being further processed 
    (like usage records summed up etc.).
    """,
    platforms = "Linux",
    license = "BSD. Copyright (c) 2008, SMSCG - Swiss Multi Science Computing Grid. All rights reserved." ,
    author = "Placi Flury",
    author_email = "grid@switch.ch",
    url = "http://www.smscg.ch",
    download_url = "https://subversion.switch.ch/svn/smscg/smscg/",
    packages = ['sgasaggregator','sgasaggregator/sgas', 'sgasaggregator/sgascache', 'sgasaggregator/utils'],
    scripts = ['sgas_aggregator'],
    data_files=[('.',['config/config.ini','config/logging.conf'])]
)


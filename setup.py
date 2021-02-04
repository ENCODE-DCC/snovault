import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()
VERSION = "1.0.52"


INSTALL_REQUIRES = [
    "PasteDeploy==2.1.0",
    "Pillow==7.0.0",
    "SQLAlchemy==1.3.13",
    "WSGIProxy2==0.4.6",
    "WebOb==1.8.6",
    "WebTest==2.0.34",
    "bcrypt==3.1.7",
    "boto3==1.11.9",
    "botocore==1.14.9",
    "elasticsearch-dsl==5.4.0",
    "elasticsearch==5.4.0",
    "future==0.18.2",
    "humanfriendly==6.1",
    "jsonschema-serialize-fork @ git+https://github.com/lrowe/jsonschema_serialize_fork.git@2.1.1",  # noqa
    "lucenequery==0.1",
    "passlib==1.7.2",
    "psutil==5.6.7",
    "psycopg2==2.8.4",
    "pyramid-localroles==0.1",
    "pyramid-multiauth==0.9.0",
    "pyramid-tm==2.4",
    "pyramid-translogger==0.1",
    "pyramid==1.10.4",
    "pyramid_retry==2.1.1",
    "python-magic==0.4.15",
    "pytz==2019.3",
    "rdflib-jsonld==0.4.0",
    "rdflib==4.2.2",
    "redis==3.5.3",
    "repoze.debug==1.1",
    "requests==2.22.0",
    "rfc3987==1.3.8",
    "rutter==0.2",
    "simplejson==3.17.0",
    "subprocess-middleware @ git+https://github.com/lrowe/subprocess_middleware.git@0.3",  # noqa
    "transaction==3.0.0",
    "venusian==3.0.0",
    "waitress==1.4.3",
    "xlrd==1.2.0",
    "zc.buildout==2.13.2",
    "zope.interface==4.7.1",
    "zope.sqlalchemy==1.2",
]

EXTRAS_REQUIRE = {
    "tests": [
        "pytest==5.3.2",
        "pytest-bdd==3.2.1",
        "pytest-mock==2.0.0",
        "pytest-splinter==2.0.1",
        "pytest-exact-fixtures==0.3",
        "pytest-instafail==0.4.1.post0",
        "pytest-timeout==1.3.4",
    ]
}

EXTRAS_REQUIRE["dev"] = EXTRAS_REQUIRE["tests"]

setup(
    name='snovault',
    version=VERSION,
    description='Snovault Hybrid Object Relational Database Framework',
    long_description=README + CHANGES,
    long_description_content_type="text/x-rst",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    package_data={'':['nginx-dev.conf']},
    zip_safe=False,
    author='Benjamin Hitz',
    author_email='hitz@stanford.edu',
    url='http://github.com/ENCODE-DCC/snovault/',
    license='MIT',
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points='''
        [console_scripts]
        batchupgrade = snovault.batchupgrade:main
        create-mapping = snovault.elasticsearch.create_mapping:main
        dev-servers = snovault.dev_servers:main
        es-index-listener = snovault.elasticsearch.es_index_listener:main

        add-date-created = snowflakes.commands.add_date_created:main
        check-rendering = snowflakes.commands.check_rendering:main
        deploy = snowflakes.commands.deploy:main
        extract_test_data = snowflakes.commands.extract_test_data:main
        es-index-data = snowflakes.commands.es_index_data:main
        import-data = snowflakes.commands.import_data:main
        jsonld-rdf = snowflakes.commands.jsonld_rdf:main
        profile = snowflakes.commands.profile:main
        spreadsheet-to-json = snowflakes.commands.spreadsheet_to_json:main
        migrate-attachments-aws = snowflakes.commands.migrate_attachments_aws:main
        create-admin-user = snowflakes.commands.create_admin_user:main 

        [paste.app_factory]
        main = snowflakes:main
        snowflakes = snowflakes:main

        [paste.composite_factory]
        indexer = snovault.elasticsearch.es_index_listener:composite

        [paste.filter_app_factory]
        memlimit = snowflakes.memlimit:filter_app
        ''',
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Framework :: Pyramid',


        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',
        'Topic :: Database :: Database Engines/Servers',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
)

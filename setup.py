"""
Setup file for installation of the dataduct code
"""
from setuptools import find_packages
from setuptools import setup

from dataduct import __version__ as version

setup(
    name='dataduct',
    version=version,
    author='Coursera Inc.',
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    namespace_packages=['dataduct'],
    include_package_data=True,
    url='https://github.com/coursera/dataduct',
    long_description=open('README.rst').read(),
    author_email='data-infra@coursera.org',
    license='Apache License 2.0',
    description='DataPipeline for Humans',
    install_requires=[
        'boto>=2.38',
        'MySQL-python>=1.2.3',
        'pandas>=0.14',
        'psycopg2>=2.6',
        'pyparsing>=1.5.6',
        'pytimeparse>=1.1.4',
        'PyYAML>=3.11',
        'testfixtures>=4.1.2',
        'pyprind'
    ],
    scripts=['bin/dataduct'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: MacOS :: MacOS 9',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Unix Shell',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Scientific/Engineering :: Visualization',
        'Topic :: Utilities',
    ],
)

"""
Setup file for installation of the etllib code
"""
from setuptools import setup

setup(
    name='dataduct',
    version='0.1.0',
    author='Coursera Inc.',
    packages=[
        'dataduct',
        'dataduct.config',
        'dataduct.pipeline',
        'dataduct.s3',
        'dataduct.steps',
        'dataduct.utils',
    ],
    namespace_packages=['dataduct'],
    include_package_data=True,
    url='https://github.com/coursera/dataduct',
    long_description=open('README.rst').read(),
    author_email='data-infra@coursera.org',
    license='Apache License 2.0',
    description='DataPipeline for Humans.',
    install_requires=[
        'boto>=2.32',
        'pyyaml'
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

"""
Setup file for installation of the etllib code
"""
from distutils.core import setup

setup(
    name='dataduct',
    version='0.1.0',
    author='Coursera Inc.',
    packages=[
        'dataduct',
        'dataduct.pipeline',
        'dataduct.s3',
        'dataduct.steps',
        'dataduct.utils',
    ],
    namespace_packages=['dataduct'],
    include_package_data=True,
    url='http://www.coursera.org',
    long_description=open('README.rst').read(),
    author_email='data-infra@coursera.org',
    license='Apache License 2.0',
    description='Fix everything about datapipeline',
    install_requires=[
        'setuptools',
    ],
    classifiers=[
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
    ],
)

# TODO: change description
# TODO: change dependencies
# TODO: change url from documentation

"""
Setup file for installation of the etllib code
"""
from distutils.core import setup

setup(
    name='etllib',
    version='0.0.1',
    author='Coursera Inc.',
    packages=['etllib'],
    namespace_packages=['etllib'],
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

# TODO: change name
# TODO: change description
# TODO: change dependencies
# TODO: Update packages
# TODO: change version fix
# TODO: change url from documentation

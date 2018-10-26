#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='pyfilemaker2',
    version="0.1.9",
    description='Python Object Wrapper for FileMaker Server XML Interface',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords=['FileMaker'],
    author='Klokan Petr Pridal, Pieter Claerhout, Marcin Kawa, Jeremie Borel',
    author_email='klokan@klokan.cz, pieter@yellowduck.be, kawa.macin@gmail.com',
    url='https://github.com/jeremie-borel/pyfilemaker2/',
    download_url='https://github.com/jeremie-borel/pyfilemaker2/',
    license='http://www.opensource.org/licenses/bsd-license.php',
    platforms = ['any'],
    packages=['pyfilemaker2', 'pyfilemaker2.errors', 'pyfilemaker2.tests', 'pyfilemaker2.tests.ressources'],
    package_data={
        '': ['*.xml'],
    },
    install_requires=['requests','lxml','future'],
)

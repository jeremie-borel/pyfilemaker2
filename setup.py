#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='pyfilemaker2',
    version="0.2.2",
    description='Python Object Wrapper for FileMaker Server XML Interface',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.9',
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
    packages=['pyfilemaker2', 'pyfilemaker2.errors', 'pyfilemaker2.tests'],
    include_package_data=True,
    package_data={
        '': ['*.md'],
        'pyfilemaker2.tests': ['ressources/*.xml', 'ressources/*.fmp12']
    },
    install_requires=['requests','lxml'],
)

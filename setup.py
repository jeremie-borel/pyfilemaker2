#!/usr/bin/env python

from setuptools import setup

setup(
    name='pyfilemaker2',
    version="0.1",
    description='Python Object Wrapper for FileMaker Server XML Interface',
    classifiers=[
        'Development Status :: 1 - Alpha',
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
    url='',
    download_url='',
    license='http://www.opensource.org/licenses/bsd-license.php',
    platforms = ['any'],
    packages=['pyfilemaker2'],
    install_requires=['requests','lxml','future'],
)

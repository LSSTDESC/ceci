#!/usr/bin/env python
"""
Lightweight pipeline engine for LSST DESC
Copyright (c) 2018 LSST DESC
http://opensource.org/licenses/MIT
"""
from setuptools import setup

version = open('./ceci/version.py').read().split('=')[1].strip().strip("'")

setup(
    name='ceci',
    version=version,
    description='Lightweight pipeline engine for LSST DESC',
    url='https://github.com/LSSTDESC/ceci',
    maintainer='Joe Zuntz',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
    ],
    packages=['ceci', 'ceci.sites'],
    entry_points={
        'console_scripts':['ceci=ceci.main:main']
    },
    # flask is actually a parsl dependency, but a setuptools bug
    # means that capitalizing "Flask" as written in the parsl
    # setup doesn't work.
    install_requires=['pyyaml']
)

#!/usr/bin/env python
"""
Lightweight pipeline engine for LSST DESC
Copyright (c) 2018 LSST DESC
http://opensource.org/licenses/MIT
"""
from setuptools import setup

setup(
    name='pipette',
    version='0.0.1',
    description='Lightweight pipeline engine for LSST DESC',
    url='https://github.com/LSSTDESC/pipette',
    maintainer='Joe Zuntz',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    packages=['pipette'],
    entry_points={
        'console_scripts':['pipette=pipette.main:main']
    },
    install_requires=['cwlgen','pyyaml','parsl']
)

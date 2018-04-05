#!/usr/bin/env python
"""
Lightweight pipeline engine for LSST DESC
Copyright (c) 2018 LSST DESC
http://opensource.org/licenses/MIT
"""
from setuptools import setup

setup(
    name='ceci',
    version='0.0.1',
    description='Lightweight pipeline engine for LSST DESC',
    url='https://github.com/LSSTDESC/ceci',
    maintainer='Joe Zuntz',
    license='MIT',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    packages=['ceci', 'ceci_example'],
    entry_points={
        'console_scripts':['ceci=ceci.main:main']
    },
    install_requires=['cwlgen','pyyaml','parsl','descformats']
)

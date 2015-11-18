#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="luigi-bigquery",
    version="0.2.0",
    descripition="Luigi integration for Google BigQuery",
    author="Kazuyuki Honda",
    author_email="hakobera@gmail.com",
    url="https://github.com/hakobera/luigi-bigquery",
    install_requires=open("requirements.txt").read().splitlines(),
    packages=find_packages(),
    license="Apache License 2.0",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
    ],
)

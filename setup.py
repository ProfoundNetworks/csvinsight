#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'plumbum',
    'pyyaml',
    'six',
]

setup_requirements = [
    'pytest-runner',
    # TODO(mpenkov): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'mock',
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='csvinsight',
    version='0.2.0',
    description="Fast & simple summary for large CSV files",
    long_description=readme + '\n\n' + history,
    author="Michael Penkov",
    author_email='misha.penkov@gmail.com',
    url='https://github.com/ProfoundNetworks/csvinsight',
    packages=find_packages(include=['csvinsight']),
    entry_points={
        'console_scripts': [
            'csvi=csvinsight.cli:main',
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='csvinsight',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)

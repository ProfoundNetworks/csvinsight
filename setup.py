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
]

test_requirements = [
    'mock',
    'pytest',
    'jupyter',
]

extras_require = {
    'notebook': ['jupyter', ],
}

setup(
    name='csvinsight',
    version='0.3.3',
    description="Fast & simple summary for large CSV files",
    long_description=readme + '\n\n' + history,
    author="Michael Penkov",
    author_email='m@penkov.dev',
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
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
    extras_require=extras_require,
)

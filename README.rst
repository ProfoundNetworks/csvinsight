==========
csvinsight
==========


.. image:: https://img.shields.io/pypi/v/csvinsight.svg
        :target: https://pypi.python.org/pypi/csvinsight

.. image:: https://circleci.com/gh/ProfoundNetworks/csvinsight.svg?style=shield&circle-token=:circle-token
        :target: https://circleci.com/gh/ProfoundNetworks/csvinsight
        :alt: Build Status

.. image:: https://readthedocs.org/projects/csvinsight/badge/?version=latest
        :target: https://csvinsight.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/mpenkov/csvinsight/shield.svg
     :target: https://pyup.io/repos/github/mpenkov/csvinsight/
     :alt: Updates


Fast & simple summary for large CSV files


* Free software: MIT license
* Documentation: https://csvinsight.readthedocs.io.


Features
--------

* Calculates basic stats for each column: max, min, mean length; number of non-empty values
* Calculates exact number of unique values and the top 20 most frequent values
* Supports non-orthogonal data (list fields)
* Works with very large files: does not load the entire CSV into memory
* Fast splitting of CSVs into columns, one file per column
* Multiprocessing-enabled

Example Usage
-------------

Given a CSV file::

    bash-3.2$ cat tests/sampledata.csv
    name|age|fave_color
    Alexey|33|red;yellow
    Boris|31|blue
    Valentina|0|

you can obtain a CsvInsight report with::

    bash-3.2$ cat tests/sampledata.csv | csvi --list-fields fave_color
    CSV Insight Report
    Total # Rows: 3
    Column counts:
            3  columns ->  3 rows

    Report Format:
    Column Number. Column Header -> Uniques: # ; Fills: # ; Fill Rate:
    Field Length: min #, max #, average:
     Top n field values -> Dupe Counts


    1. name -> Uniques: 3 ; Fills: 3 ; Fill Rate: 100.0%
        Field Length:  min 5, max 9, avg 6.67
            Counts      Percent  Field Value
            1           33.33 %  Valentina
            1           33.33 %  Boris
            1           33.33 %  Alexey

    2. age -> Uniques: 3 ; Fills: 3 ; Fill Rate: 100.0%
        Field Length:  min 1, max 2, avg 1.67
            Counts      Percent  Field Value
            1           33.33 %  33
            1           33.33 %  31
            1           33.33 %  0

    3. fave_color -> Uniques: 4 ; Fills: 3 ; Fill Rate: 75.0%
        Field Length:  min 0, max 6, avg 3.25
            Counts      Percent  Field Value
            1           25.00 %  yellow
            1           25.00 %  red
            1           25.00 %  blue
            1           25.00 %  NULL

Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage


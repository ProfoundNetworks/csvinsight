=======
History
=======

Unreleased
----------

* Added --most-common parameter (resolved Issue #14)
* Added --no-tiny parameter
* Refactored temporary file naming
* Improve error message when handling empty CSV files
* Fixed "Argument list too long" error (Issue #15)
* Added --json parameter
* Added --ipynb parameter to generate IPython notebook

0.2.3 (2017-12-09)
------------------

* Fix bug: Unicode column names now work under Py2

0.2.2 (2017-12-04)
------------------

* Fix bug: Unicode characters no longer break CsvInsight on Py2

0.2.1 (2017-11-27)
------------------

* Fix bug: opening gzipped files with Py3 now works

0.2.0 (2017-11-25)
------------------

* Split files using gsplit and process them in parallel for faster processing
* No longer work with streams; works exclusively with files
* Get rid of csvi_summarize and csvi_split entry points
* Integrated plumbum for cleaner pipelines
* Fixed issue #11: added support for more CSV parameters via the --dialect option
* Fixed issue #10: reading from empty files no longer raises StopIteration
* Fixed issue #8: use the correct link to the GitHub project in the documentation
* Fixed issue #2: implemented in-memory mode for smaller files

0.1.0 (2017-10-29)
------------------

* First release on PyPI.

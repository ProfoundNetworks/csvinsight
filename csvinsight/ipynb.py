#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2018
#
"""Generate an IPython notebook to visualize a report."""
import os.path as P

import nbformat
import subprocess


def generate(report):
    """Generate an IPython notebook from the report.

    :param dict report: The report
    :returns: The generated notebook
    :rtype: str
    """
    curr_dir = P.dirname(P.abspath(__file__))
    template_path = P.join(curr_dir, 'ipynb_template.py')
    with open(template_path) as fin:
        template = fin.read()

    template = template.replace('report = None', 'report = %r' % report)

    nbook = nbformat.v3.reads_py(template)
    nbook = nbformat.v4.upgrade(nbook)
    return nbformat.v4.writes(nbook) + "\n"


def execute(path, save_html=False):
    """Execute an IPython notebook in-place.

    :param path str: The path to the notebook.
    """
    command = (
        'jupyter', 'nbconvert', '--execute', path,
        '--to', 'notebook', '--inplace'
    )
    subprocess.check_call(command)
    if save_html:
        command = ('jupyter', 'nbconvert', path, '--to', 'html')
        subprocess.check_call(command)

# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>
import sys

from collections import Counter # flake8: noqa
%matplotlib inline
import matplotlib.pyplot as plt

report = None

# <codecell>

def plot_pie(field, skip_empty=False, figsize=(6, 6)):
    try:
        most_common = report['results'][field]['most_common']
    except KeyError:
        #
        # Quietly fail instead of blowing up
        #
        sys.stderr.write('no such field: %r\n' % field)
        return

    if skip_empty:
        most_common = [(sz, lbl) for (sz, lbl) in most_common if lbl]
        title = '%s (non-empty only)' % field
    else:
        title = field

    plt.figure(figsize=figsize)
    plt.title(title)

    sizes, labels = zip(*most_common)
    patches, texts = plt.pie(sizes, shadow=True)
    plt.legend(patches, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    plt.axis('equal')

# <codecell>

plot_pie('country_code_pn', skip_empty=True)
plot_pie('domain_classification_out')
plot_pie('web_server_type', skip_empty=True)
plot_pie('web_server_count')
plot_pie('ssl_certificate_issuer', skip_empty=True)
plot_pie('ecommerce', skip_empty=True)
plot_pie('web_analytics', skip_empty=True)
plot_pie('social_networks', skip_empty=True)
plot_pie('cms', skip_empty=True)
plot_pie('advertising', skip_empty=True)
plot_pie('web_technology_tools', skip_empty=True)
plot_pie('number_of_domains_linked')
plot_pie('cloud_density')
plot_pie('cloud_provider', skip_empty=True)
plot_pie('saas_company', skip_empty=True)
plot_pie('paas_company', skip_empty=True)
plot_pie('iaas_company', skip_empty=True)
plot_pie('ctj_sptb_matrix_recommendation')
plot_pie('we_country', skip_empty=True)
plot_pie('we_details', skip_empty=True)

# <markdowncell>
#
# If you can read this, reads_py() is no longer broken!
#

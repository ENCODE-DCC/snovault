##############################################################################
#
# Copyright (c) 2006 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Bootstrap a buildout-based project

Simply run this script in a directory containing a buildout.cfg.
The script accepts buildout command-line options, so you can
use the -c option to specify an alternate configuration file.
"""

import sys
import pip
from optparse import OptionParser

__version__ = '4dn-custom'

usage = '''\
[DESIRED PYTHON FOR BUILDOUT] bootstrap.py [options]

Bootstraps a buildout-based project.

Simply run this script in a directory containing a buildout.cfg, using the
Python that you want bin/buildout to use.

'''

parser = OptionParser(usage=usage)
parser.add_option("--version",
                  action="store_true", default=False,
                  help=("Return bootstrap.py version."))
parser.add_option("-c", "--config-file",
                  help=("Specify the path to the buildout configuration "
                        "file to be used."))
parser.add_option("--buildout-version",
                  help="Use a specific zc.buildout version")
parser.add_option("--setuptools-version",
                  help="Use a specific setuptools version")

options, args = parser.parse_args()
if options.version:
    print("bootstrap.py version %s" % __version__)
    sys.exit(0)


######################################################################
# load/install setuptools
if options.setuptools_version is not None:
    pip.main(['install', 'setuptools==%s' % options.setuptools_version])
else:
    pip.main(['install', 'setuptools'])

######################################################################
# Install buildout
if options.buildout_version is not None:
    pip.main(['install', '--upgrade', 'zc.buildout==%s' % options.buildout_version])
else:
    pip.main(['install', '--upgrade', 'zc.buildout'])

######################################################################
# Import and run buildout

if not [a for a in args if '=' not in a]:
    args.append('bootstrap')

# if -c was provided, we push it back into args for buildout' main function
if options.config_file is not None:
    args[0:0] = ['-c', options.config_file]

import zc.buildout.buildout
zc.buildout.buildout.main(args)

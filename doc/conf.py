"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.daf.ingest


_g = globals()
_g.update(build_package_configs(
    project_name='daf_ingest',
    version=lsst.daf.ingest.version.__version__))

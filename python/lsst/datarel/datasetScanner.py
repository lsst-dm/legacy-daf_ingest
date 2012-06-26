# 
# LSST Data Management System
# Copyright 2012 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import os
import os.path
import re
import lsst.daf.butlerUtils

__all__ = ['getMapperClass',
           'parseDataIdRules',
           'HfsScanner',
           'DatasetScanner',
          ]


_mapperClassName = {
    'lsstsim': 'lsst.obs.lsstSim.LsstSimMapper',
    'sdss': 'lsst.obs.sdss.SdssMapper',
    'cfht': 'lsst.obs.cfht.CfhtMapper',
}


def getMapperClass(camera):
    """Return the subclass of lsst.daf.persistence.Mapper
    to use for the camera with the given name (case-insensitive).
    """
    camera = camera.lower()
    if camera not in _mapperClassName:
        raise RuntimeError(str.format("{} is not a valid camera name", camera))
    name = _mapperClassName[camera]
    try:
        pieces = name.split('.')
        cls = reduce(getattr, pieces[1:], __import__('.'.join(pieces[:-1])))
        return cls
    except:
        raise RuntimeError(str.format("Failed to import {}", name))


_keyTypes = {
    'lsstsim': {
        'visit': int,
        'filter': str,
        'sensorName': str,
        'ccdName': str,
        'channelName': str,
        'ampName': str,
        'raft': str,
        'snap': int,
        'exposure': int,
        'ccd': str,
        'sensor': str,
        'amp': str,
        'channel': str,
        'raftId': int,
        'ccdId': int,
        'sensorId': int,
        'ampId': int,
        'channelId': int,
        'skyTile': int,
        'tract': int,
        'patch': str,
    },
    'sdss': {
        'run': int,
        'camcol': int,
        'field': int,
        'filter': str,
        'skyTile': int,
        'tract': int,
        'patch': str,
    },
    'cfht': {
        'visit': int,
        'filter': str,
        'ccdName': str,
        'ampName': str,
        'ccd': int,
        'amp': int,
        'skyTile': int,
        'tract': int,
        'patch': str,
    },
}


def parseDataIdRules(ruleList, camera):
    """A rule is a string in the following format:

    'key=value1[^value2[^value3...]'

    The values may either be strings, or of the form 'int...int'
    (e.g. '1..3') which is interpreted as '1^2^3' (inclusive, unlike a python
    range). So '0^2..4^7..9' is equivalent to '0^2^3^4^7^8^9'.

    This function parses a list of such strings, and returns a dict mapping
    keys to sets of legal values.
    
    ruleList:
        List of rule strings
    camera:
        Camera the rule list applies to (e.g. 'lsstSim' or 'sdss')
    """
    camera = camera.lower()
    if camera not in _keyTypes:
        raise RuntimeError('{} is not a recognized camera name'.format(camera))
    kvs = {}
    for rule in ruleList:
        # process rule for a single key
        key, _, pattern = rule.partition('=')
        if key not in _keyTypes[camera]:
            raise RuntimeError('{} is not a valid dataId key for camera {}'.format(key, camera))
        if len(pattern) == 0:
            continue
        values = set()
        # compute union of all values or value ranges
        for p in pattern.split('^'):
            if _keyTypes[camera][key] == int:
                # check for range syntax
                m = re.search(r'^(\d+)\.\.(\d+)$', p)
                if m:
                    values.update(xrange(int(m.group(1)), int(m.group(2)) + 1))
                else:
                    values.add(int(p))
            else:
                values.add(p)
        if key in kvs:
            kvs[key].update(values)
        else:
            kvs[key] = values
    return kvs


class _FormatKey(object):
    """A key in a path template. Three attributes are provided:

    spec
        Formatting spec for the key, e.g. '%(filter)s'.

    typ
        key value type; int or str

    munge
        A function that takes a key name, key value string and a dictionary.
        This function should return a fresh dictionary including new entries
        derived from the given key, value, and existing entries. The
        _mungeStr and _mungeInt functions are examples.
    """
    def __init__(self, spec, typ, munge):
        self.spec = spec
        self.typ = typ
        self.munge = munge

def _mungeStr(k, v, dataId):
    """Munger for keys with string formats."""
    kv = dataId.copy()
    kv[k] = str(v)
    return kv

def _mungeInt(k, v, dataId):
    """Munger for keys with integer formats."""
    kv = dataId.copy()
    kv[k] = int(v)
    return kv


class _PathComponent(object):
    """A single component (directory or file) of a path template. The
    following attributes are provided:

    keys
        List of key names first occurring in this path component.

    regex
        Compiled regular expression identifying matches to this path
        component unless simple is True; in that case, regex is just
        a string literal

    simple
        True if regex is a simple string literal rather than a pattern.
        In this case, keys will always by None or [].
    """
    def __init__(self, keys, regex, simple):
        self.keys = keys
        self.regex = regex
        self.simple = simple


class HfsScanner(object):
    """A hierarchical scanner for paths matching a template, optionally
    also restricting visited paths to those matching a list of dataId rules.
    """
    def __init__(self, template):
        """Build an FsScanner for given a path template. The path template
        should be a Python string with named format substitution
        specifications, as used in mapper policy files. For example:

        deepCoadd-results/%(filter)s/%(tract)d/%(patch)s/calexp-%(filter)s-%(tract)d-%(patch)s.fits

        Note that a key may appear multiple times. If it does,
        the value for each occurrence should be identical (the formatting
        specs must be identical). Octal, binary, hexadecimal, and floating
        point formats are not supported.
        """
        template = os.path.normpath(template)
        if (len(template) == 0 or
            template == os.curdir or
            template[0] == os.sep or
            template[-1] == os.sep):
            raise RuntimeError(
                'Path template is empty, absolute, or identifies a directory')
        self._formatKeys = {}
        self._pathComponents = []
        fmt = re.compile(r'%\((\w+)\).*?([diucrs])')

        # split path into components
        for component in template.split(os.sep):
            # search for all occurences of a format spec
            simple = True
            last = 0
            regex = ''
            newKeys = []
            for m in fmt.finditer(component):
                simple = False
                spec = m.group(0)
                k = m.group(1)
                seenBefore = self._formatKeys.has_key(k)
                # transform format spec into a regular expression
                regex += re.escape(component[last:m.start(0)])
                last = m.end(0)
                regex += '('
                if seenBefore:
                    regex += '?:'
                if m.group(2) in 'crs':
                    munge = _mungeStr
                    typ = str
                    regex += r'.+)'
                else:
                    munge = _mungeInt
                    typ = int
                    regex += r'[+-]?\d+)'
                if seenBefore:
                    # check consistency of formatting spec across key occurences
                    if spec[-1] != self._formatKeys[k].spec[-1]:
                        raise RuntimeError(
                            'Path template contains inconsistent format type-codes '
                            'for the same key')
                else:
                    newKeys.append(k)
                    self._formatKeys[k] = _FormatKey(spec, typ, munge)
            regex += re.escape(component[last:])
            if simple:
                regex = component # literal match
            else:
                regex = re.compile('^' + regex + '$')
            self._pathComponents.append(_PathComponent(newKeys, regex, simple))

    def walk(self, root, rules=None):
        """Generator that descends the given root directory in top-down
        fashion, matching paths corresponding to the template and satisfying
        the given rule list. The generator yields tuples of the form
        (path, dataId), where path is a dataset file name relative to root,
        and dataId is a key value dictionary identifying the file.
        """
        stack = [(0, root, rules, {})]
        while stack:
            depth, path, rules, dataId = stack.pop()
            if os.path.isfile(path):
                continue
            pc = self._pathComponents[depth]
            if pc.simple:
                # No need to list directory contents
                entries = [pc.regex]
                if not os.path.exists(os.path.join(path, pc.regex)):
                    continue
            else:
                entries = os.listdir(path)
            depth += 1
            for e in entries:
                subRules = rules
                subDataId = dataId
                if not pc.simple:
                    # make sure e matches path component regular expression
                    m = pc.regex.match(e)
                    if not m:
                        continue
                    # got a match - update dataId with new key values (if any)
                    for i, k in enumerate(pc.keys):
                        subDataId = self._formatKeys[k].munge(k, m.group(i + 1), subDataId)
                    if subRules and pc.keys:
                        # have dataId rules and saw new keys; filter rule list
                        for k in subDataId:
                            newRules = []
                            for r in subRules:
                                if k not in r or subDataId[k] in r[k]:
                                    newRules.append(r)
                            subRules = newRules
                        if not subRules:
                            continue # no rules matched
                # Have path matching template and at least one rule
                p = os.path.join(path, e)
                if depth < len(self._pathComponents):
                    # recurse
                    stack.append((depth, p, subRules, subDataId))
                elif depth == len(self._pathComponents):
                    if os.path.isfile(p):
                        # found a matching file, yield it
                        yield os.path.relpath(p, root), subDataId


# -- Camera specific dataId mungers ----

def _mungeLsstSim(k, v, dataId):
    dataId = dataId.copy()
    if k == 'raft':
        r1, r2 = v
        dataId['raft'] = r1 + ',' + r2
        dataId['raftId'] = int(r1) * 5 + int(r2)
    elif k in ('sensor', 'ccd'):
        s1, s2 = v
        dataId['sensor'] = s1 + ',' + s2
        dataId['sensorNum'] = int(s1) * 3 + int(s2)
    elif k in ('channel', 'amp'):
        c1, c2 = v
        dataId['channel'] = c1 + ',' + c2
        dataId['channelNum'] = int(c1) * 8 + int(c2)
    elif k in ('snap', 'exposure'):
        dataId['snap'] = int(v)
    elif _keyTypes['lsstsim'][k] == int:
        dataId[k] = int(v)
    else:
        dataId[k] = v
    return dataId

def _mungeSdss(k, v, dataId):
    dataId = dataId.copy()
    if _keyTypes['sdss'][k] == int:
        dataId[k] = int(v)
    else:
        dataId[k] = v
    return dataId

def _mungeCfht(k, v, dataId):
    dataId = dataId.copy()
    if k == 'ccd':
        dataId['ccd'] = int(v)
        dataId['ccdName'] = v
    elif k == 'amp':
        dataId['amp'] = int(v)
        dataId['ampName'] = v
    elif _keyTypes['sdss'][k] == int:
        dataId[k] = int(v)
    else:
        dataId[k] = v 
    return dataId

_mungeFunctions = {
    'lsstsim': _mungeLsstSim,
    'sdss': _mungeSdss,
    'cfht': _mungeCfht,
}


class DatasetScanner(HfsScanner):
    """File system scanner for a dataset known to a camera mapper.
    """
    def __init__(self, dataset, camera, cameraMapper):
        if not isinstance(cameraMapper, lsst.daf.butlerUtils.CameraMapper):
            raise TypeError('Expecting a lsst.daf.butlerUtils.CameraMapper!')
        if dataset not in cameraMapper.mappings:
            raise NotFoundError('Unknown dataset ' + str(dataset))
        HfsScanner.__init__(self, cameraMapper.mappings[dataset].template)
        camera = camera.lower()
        if camera not in _keyTypes:
            raise RuntimeError('{} camera not supported yet'.format(camera))
        for k in self._formatKeys:
            if k not in _keyTypes[camera]:
                raise RuntimeError('{} is not a valid dataId key for camera {}'.format(k, camera))
            self._formatKeys[k].munge = _mungeFunctions[camera]


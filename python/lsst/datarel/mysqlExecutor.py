# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

from __future__ import with_statement
from contextlib import closing
import getpass
import MySQLdb as sql
import optparse
import os
import subprocess
import sys


class MysqlExecutor(object):
    def __init__(self, host, database, user, port=3306, password=None):
        self.host = host
        self.port = port
        self.user = user
        if password == None:
            self.password = getpass.getpass("%s's MySQL password: " % user)
        else:
            self.password = password
        self.database = database

    def createDb(self, database):
        if not isinstance(database, basestring):
            raise TypeError('database name is not a string')
        subprocess.check_call(['mysql', '-vvv',
                               '-h', self.host,
                               '-P', str(self.port),
                               '-u', self.user,
                               '-p' + self.password,
                               '-e', 'CREATE DATABASE %s;' % database],
                              stdout=sys.stdout, stderr=sys.stderr)
        sys.stdout.flush()
        sys.stderr.flush()


    def execStmt(self, stmt):
        if not isinstance(stmt, basestring):
            raise TypeError('SQL statement is not a string')
        subprocess.check_call(['mysql', '-vvv',
                               '-h', self.host,
                               '-P', str(self.port),
                               '-u', self.user,
                               '-p' + self.password,
                               '-D', self.database,
                               '-e', stmt],
                              stdout=sys.stdout, stderr=sys.stderr)
        sys.stdout.flush()
        sys.stderr.flush()

    def execScript(self, script):
        if not isinstance(script, basestring):
            raise TypeError('Script file name is not a string')
        if not os.path.isfile(script):
            raise RuntimeError(
                'Script %s does not exist or is not a file' % script)
        with open(script, 'rb') as f:
            subprocess.check_call(['mysql', '-vvv',
                                   '-h', self.host,
                                   '-P', str(self.port),
                                   '-u', self.user,
                                   '-p' + self.password,
                                   '-D', self.database],
                                  stdin=f, stdout=sys.stdout, stderr=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    def runQuery(self, query):
        if not isinstance(query, basestring):
            raise TypeError('Query is not a string')
        kw = { 'host': self.host,
               'port': self.port,
               'user': self.user,
               'db': self.database,
               'passwd': self.password }
        with closing(sql.connect(**kw)) as conn:
            with closing(conn.cursor()) as cursor:
                print query
                sys.stdout.flush()
                cursor.execute(query)
                return cursor.fetchall()


def addDbOptions(parser):
    if not isinstance(parser, optparse.OptionParser):
        raise TypeError('Expecting an optparse.OptionParser')
    defUser = (os.environ.has_key('USER') and os.environ['USER']) or None
    parser.add_option(
        "-u", "--user", dest="user", default=defUser,
        help="MySQL database user name (%default).")
    parser.add_option(
        "-H", "--host", dest="host", default="lsst10.ncsa.uiuc.edu",
        help="MySQL database server hostname (%default).")
    parser.add_option(
        "-P", "--port", dest="port", type="int", default=3306,
        help="MySQL database server port (%default).")


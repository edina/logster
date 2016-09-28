###  A sample logster parser file that can be used to count the number
###  of various stats from Digimap.
###
###  This class was copied from SampleLogster.
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia DMWebLogster /var/log/httpd/access_log
###
###
###  Copyright 2011, Etsy, Inc., 2013 University of Edinburgh
###
###  This file is part of Logster.
###
###  Logster is free software: you can redistribute it and/or modify
###  it under the terms of the GNU General Public License as published by
###  the Free Software Foundation, either version 3 of the License, or
###  (at your option) any later version.
###
###  Logster is distributed in the hope that it will be useful,
###  but WITHOUT ANY WARRANTY; without even the implied warranty of
###  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
###  GNU General Public License for more details.
###
###  You should have received a copy of the GNU General Public License
###  along with Logster. If not, see <http://www.gnu.org/licenses/>.
###

import time
import re

from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException

class DfSWebLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.cosmoLogins = {}
        self.cosmoPrints = {}
        self.cosmoMapproxies = {}
        self.cosmoSaveBMs = {}
        self.cosmoLoadBMs = {}

        self.printRespTimes = {};
        self.mapproxyRespTimes = {};

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.regCosmoLogin = re.compile('.*GET /login.* HTTP/\d.\d" (?P<code>\d+) .*')
        self.regCosmoPrint = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*POST /dfs/cosmo-print.* HTTP/\d.\d" (?P<code>\d+) .*')
        self.regCosmoMapproxy = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*/mapproxy/wmsMap.* HTTP/\d.\d" (?P<code>\d+) .*')
        self.regCosmoSaveBMs = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*POST /dfs/cosmo-my-maps.* HTTP/\d.\d" (?P<code>\d+) .*')
        self.regCosmoLoadBMs = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*GET /dfs/cosmo-get-my-map.* HTTP/\d.\d" (?P<code>\d+) .*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regCosmoLoginMatch = False
        if "MONITOR" not in line and "idp.edina.ac.uk" not in line:
            regCosmoLoginMatch = self.regCosmoLogin.match(line)
        regCosmoPrintMatch = self.regCosmoPrint.match(line)
        regCosmoMapproxiesMatch = self.regCosmoMapproxy.match(line)
        regCosmoSaveBMsMatch = self.regCosmoSaveBMs.match(line)
        regCosmoLoadBMsMatch = self.regCosmoLoadBMs.match(line)

        if regCosmoLoginMatch:
          linebits = regCosmoLoginMatch.groupdict()
          code = linebits['code']
          if code in self.cosmoLogins:
            self.cosmoLogins[code] += 1
          else:
            self.cosmoLogins[code] = 1
        elif regCosmoPrintMatch:
          linebits = regCosmoPrintMatch.groupdict()
          code = linebits['code']

          if code in self.cosmoPrints:
            self.cosmoPrints[code] += 1
            self.printRespTimes[code] += int(linebits['response']) / float(1000)
          else:
            self.cosmoPrints[code] = 1
            self.printRespTimes[code] = int(linebits['response']) / float(1000)
        elif regCosmoMapproxiesMatch:
          linebits = regCosmoMapproxiesMatch.groupdict()
          code = linebits['code']

          if code in self.cosmoMapproxies:
            self.cosmoMapproxies[code] += 1
            self.mapproxyRespTimes[code] += int(linebits['response']) / float(1000)
          else:
            self.cosmoMapproxies[code] = 1
            self.mapproxyRespTimes[code] = int(linebits['response']) / float(1000)
        elif regCosmoSaveBMsMatch:
          linebits = regCosmoSaveBMsMatch.groupdict()
          code = linebits['code']
          if code in self.cosmoSaveBMs:
            self.cosmoSaveBMs[code] += 1
          else:
            self.cosmoSaveBMs[code] = 1
        elif regCosmoLoadBMsMatch:
          linebits = regCosmoLoadBMsMatch.groupdict()
          code = linebits['code']
          if code in self.cosmoLoadBMs:
            self.cosmoLoadBMs[code] += 1
          else:
            self.cosmoLoadBMs[code] = 1
        # ignore non-matching lines

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        metricObjects = []
        for code, count in self.cosmoLogins.items():
            metricObjects.append( MetricObject( "logins_count." + code, count, "Schools Logins per minute" ) )
        for code, count in self.cosmoPrints.items():
            metricObjects.append( MetricObject( "prints_count." + code, count, "Schools Prints per minute" ) )
            metricObjects.append( MetricObject( "prints_response." + code, self.printRespTimes[code] / float(count), "Avg Response Time per minute" ) )
        for code, count in self.cosmoMapproxies.items():
            metricObjects.append( MetricObject( "mapproxies_count." + code, count, "Schools Mapproxy Requests per minute" ) )
            metricObjects.append( MetricObject( "mapproxies_response."+code, self.mapproxyRespTimes[code] / float(count), "Avg Response Time per minute" ) )
        for code, count in self.cosmoSaveBMs.items():
            metricObjects.append( MetricObject( "bookmarks_save_count." + code, count, "Schools Save Bookmark Requests per minute" ) )
        for code, count in self.cosmoLoadBMs.items():
            metricObjects.append( MetricObject( "bookmarks_load_count." + code, count, "Schools Load Bookmark Requests per minute" ) )


        return metricObjects

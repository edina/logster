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
        self.cosmoLogins = 0
        self.cosmoPrints = 0
        self.cosmoMapproxies = 0
        self.cosmoSaveBMs = 0
        self.cosmoLoadBMs = 0

        self.printRespTimes = 0;
        self.mapproxyRespTimes = 0;
        self.saveBMRespTimes = 0;
        self.loadBMRespTimes = 0;
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.regCosmoLogin = re.compile('.*GET /cosmo/login.*')
        self.regCosmoPrint = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*POST /cosmo/generatePrint.*')
        self.regCosmoMapproxy = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*/mapproxy/wmsMap.*')
        self.regCosmoSaveBMs = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*/cosmo/saveBookmark.*')
        self.regCosmoLoadBMs = re.compile('.*\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3} (?P<response>\d+) \d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.*/cosmo/bookmarks.*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regCosmoLoginMatch = self.regCosmoLogin.match(line)
        regCosmoPrintMatch = self.regCosmoPrint.match(line)
        regCosmoMapproxiesMatch = self.regCosmoMapproxy.match(line)
        regCosmoSaveBMsMatch = self.regCosmoSaveBMs.match(line)
        regCosmoLoadBMsMatch = self.regCosmoLoadBMs.match(line)

        if regCosmoLoginMatch:
          self.cosmoLogins += 1
        elif regCosmoPrintMatch:
          self.cosmoPrints += 1
          linebits = regCosmoPrintMatch.groupdict()
          self.printRespTimes += int(linebits['response']) / float(1000)
        elif regCosmoMapproxiesMatch:
          self.cosmoMapproxies += 1
          linebits = regCosmoMapproxiesMatch.groupdict()
          self.mapproxyRespTimes += int(linebits['response']) / float(1000)
        elif regCosmoSaveBMsMatch:
          self.cosmoSaveBMs += 1
          linebits = regCosmoSaveBMsMatch.groupdict()
          self.saveBMRespTimes += int(linebits['response']) / float(1000)
        elif regCosmoLoadBMsMatch:
          self.cosmoLoadBMs += 1
          linebits = regCosmoLoadBMsMatch.groupdict()
          self.loadBMRespTimes += int(linebits['response']) / float(1000)
        # ignore non-matching lines

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        metricObjects = []
        metricObjects.append( MetricObject( "logins_count", self.cosmoLogins, "Schools Logins per minute" ) )
        metricObjects.append( MetricObject( "prints_count", self.cosmoPrints, "Schools Prints per minute" ) )
        metricObjects.append( MetricObject( "mapproxies_count", self.cosmoMapproxies, "Schools Mapproxy Requests per minute" ) )
        metricObjects.append( MetricObject( "bookmarks_save_count", self.cosmoSaveBMs, "Schools Save Bookmark Requests per minute" ) )
        metricObjects.append( MetricObject( "bookmarks_load_count", self.cosmoLoadBMs, "Schools Load Bookmark Requests per minute" ) )
        
        '''Response times'''
        if self.cosmoPrints > 0:
          metricObjects.append( MetricObject( "prints_response", self.printRespTimes / float(self.cosmoPrints), "Avg Response Time per minute" ) )
        if self.cosmoMapproxies > 0:
          metricObjects.append( MetricObject( "mapproxies_response", self.mapproxyRespTimes / float(self.cosmoMapproxies), "Avg Response Time per minute" ) )
        if self.cosmoSaveBMs > 0:
          metricObjects.append( MetricObject( "bookmarks_save_response", self.saveBMRespTimes / float(self.cosmoSaveBMs), "Avg Response Time per minute" ) )
        if self.cosmoLoadBMs > 0:
          metricObjects.append( MetricObject( "bookmarks_load_response", self.loadBMRespTimes / float(self.cosmoLoadBMs), "Avg Response Time per minute" ) )

        return metricObjects

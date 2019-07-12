###  A sample logster parser file that can be used to count the number
###  of Tilecache requests in the EDINA Digimap service.
###
###  Assumed layout of mapfiles:
###  <folder>/$collection/<mapfile>.map
###  $collection is the request string that is logged.
###
###  This class was copied from SampleLogster.
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia DMMapLogster /var/log/httpd/access_log
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

class DMMapProxyLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.tile_maps = {}
        self.tile_resp = {}
        self.wms_maps = {}
        self.wms_resp = {}

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.reg = re.compile('(?=.*tiled=true).*/mapproxy/service.*layers=(?P<cache>[\w-]+).* (?P<code>\d+) \d+ Response: (?P<response>\d+).*', re.IGNORECASE)
        self.wmsreg = re.compile('(?!.*tiled=true).*/mapproxy/service.*layers=(?P<cache>[\w-]+).* (?P<code>\d+) \d+ Response: (?P<response>\d+).*', re.IGNORECASE)

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regMatch = self.reg.match(line)
        regWmsMatch = self.wmsreg.match(line)

        if regMatch:
            linebits = regMatch.groupdict()
            self.populate(self.tile_maps, self.tile_resp, linebits, "mp_")
        elif regWmsMatch:
            linebits = regWmsMatch.groupdict()
            self.populate(self.wms_maps, self.wms_resp, linebits, "mpwms_")
        # ignore non-matching lines since our apache log is full of crap

    def populate(self, countDict, responseDict, linebits, cacheName):
        cache_name = cacheName + linebits['cache']
        code = linebits['code']
        response = int(linebits['response']) / float(1000) # convert usec to msec
        if cache_name in self.countDict and code in self.countDict[cache_name]:
            self.countDict[cache_name][code] += 1
            self.responseDict[cache_name][code] += response
        else:
            if cache_name not in self.countDict:
                self.countDict[cache_name] = {}
                self.responseDict[cache_name] = {}
            self.countDict[cache_name][code] = 1
            self.responseDict[cache_name][code] = response

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        metricObjects = []
        self.record_metric(metricObjects, self.tile_maps, self.tile_resp)
        self.record_metric(metricObjects, self.wms_maps, self.wms_resp)

        return metricObjects

    def record_metric(self, metricObjects, countDict, responseDict):
        for cache, code_key in countDict.items():
            for code, count in countDict[cache].items():
                metricObjects.append( MetricObject( cache + "_count." + code, count, "Responses per minute" ) )
        for cache, code_key in responseDict.items():
            for code, response in responseDict[cache].items():
                count = countDict[cache][code]
                avg_response = response / float(count)
                metricObjects.append( MetricObject( cache + "_response." + code, avg_response, "Avg Response Time per minute" ) )

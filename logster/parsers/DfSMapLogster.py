###  A sample logster parser file that can be used to count the number
###  of Mapserver requests in the EDINA Digimap service.
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

class DfSMapLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.mapserverMaps = {}
        self.mapserverResp = {}
        
        self.clivePrints = 0
        self.clivePrintRespTimes = 0
        self.mapstreamWMSs = 0
        self.mapstreamWMSRespTimes = 0
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.mapservReg = re.compile('.*mapserv.*map=mapfiles(/|%2F)(?P<mapcollection>\w+)(/|%2F).*\.map.*Response: (?P<response>\d+).*', re.IGNORECASE)
        self.cliveReg = re.compile('.*clive/clive.*Response: (?P<response>\d+).*', re.IGNORECASE)
        self.mapstreamReg = re.compile('.*mapstream/wms.*Response: (?P<response>\d+).*', re.IGNORECASE)


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        mapservRegMatch = self.reg.match(line)
        cliveRegMatch = self.cliveReg.match(line)
        mapstreamRegMatch = self.mapstreamReg.match(line)

        # FIXME don't like this duplicated code
        if mapservRegMatch:
            linebits = regMatch.groupdict()
            mapCollection = "ms_" + linebits['mapcollection']
            response = int(linebits['response']) / float(1000) # convert usec to msec

            if map_collection in self.mapserverMaps:
              self.mapserverMaps[mapCollection] += 1
              self.mapserverResp[mapCollection] += response
            else:
              self.mapserverMaps[mapCollection] = 1
              self.mapserverResp[mapCollection] = response
        elif cliveRegMatch:
          self.clivePrints += 1
          linebits = cliveRegMatch.groupdict()
          self.clivePrintRespTimes += int(linebits['response']) / float(1000)
        elif mapstreamRegMatch:
          self.mapstreamWMSs += 1
          linebits = mapstreamRegMatch.groupdict()
          self.mapstreamWMSRespTimes += int(linebits['response']) / float(1000)
        # ignore non-matching lines since our apache log is full of crap

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        metricObjects = []
        for product, count in self.mapserverMaps.items():
          metricObjects.append( MetricObject( product + "_count", count, "Responses per minute" ) )
        for product, response in self.mapserverResp.items():
          count = self.mapserverMaps[product];
          avg_response = response / float(count)
          metricObjects.append( MetricObject( product + "_response", avg_response, "Avg Response Time per minute" ) )

        metricObjects.append( MetricObject( "clive_print_count", self.clivePrints, "Clive Prints per minute" ) )
        metricObjects.append( MetricObject( "mapstream_wms_count", self.mapstreamWMSs, "Mapstream WMSs per minute" ) )
        
        metricObjects.append( MetricObject( "clive_print_response", self.clivePrintRespTimes / float(self.clivePrints), "Avg Response Time per minute" ) )
        metricObjects.append( MetricObject( "mapstream_wms_response", self.mapstreamWMSRespTimes / float(self.mapstreamWMSs), "Avg Response Time per minute" ) )

        return metricObjects

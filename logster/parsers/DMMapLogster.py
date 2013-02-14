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

class DMMapLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.mapserver_maps = {}
        self.mapserver_resp = {}
        self.clive_maps = {}
        self.clive_resp = {}
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.reg = re.compile('.*mapserv.*map=mapfiles(/|%2F)(?P<mapcollection>\w+)(/|%2F).*\.map.*Response: (?P<response>\d+).*')
        self.clive_reg = re.compile('.*clive/clive.*product=(?P<product>\w+)&.*Response: (?P<response>\d+).*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regMatch = self.reg.match(line)
        cliveRegMatch = self.clive_reg.match(line)

        # FIXME don't like this duplicated code
        if regMatch:
            linebits = regMatch.groupdict()
            map_collection = "ms_" + linebits['mapcollection']
            response = int(linebits['response']) / float(1000) # convert usec to msec

            if map_collection in self.mapserver_maps:
              self.mapserver_maps[map_collection] += 1
              self.mapserver_resp[map_collection] += response
            else:
              self.mapserver_maps[map_collection] = 1
              self.mapserver_resp[map_collection] = response

        elif cliveRegMatch:
          linebits = cliveRegMatch.groupdict()
          product = "clive_" + linebits['product'].lower();
          response = int(linebits['response']) / float(1000) # convert usec to msec

          if product in self.clive_maps:
            self.clive_maps[product] += 1
            self.clive_resp[product] += response
          else:
            self.clive_maps[product] = 1
            self.clive_resp[product] = response
        # ignore non-matching lines since our apache log is full of crap

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        self.duration = duration / 60.0 # FIXME this is weird, as its always 1

        metricObjects = []
        for product, count in self.mapserver_maps.items():
          metricObjects.append( MetricObject( product + "_count", count / self.duration, "Responses per minute" ) )
        for product, response in self.mapserver_resp.items():
          count = self.mapserver_maps[product];
          avg_response = response / float(count)
          metricObjects.append( MetricObject( product + "_response", avg_response / self.duration, "Avg Response Time per minute" ) )

        for product, count in self.clive_maps.items():
          metricObjects.append( MetricObject( product + "_count", count / self.duration, "Responses per minute" ) )
        for product, response in self.clive_resp.items():
          count = self.clive_maps[product];
          avg_response = response / float(count)
          metricObjects.append( MetricObject( product + "_response", avg_response / self.duration, "Avg Response Time per minute" ) )

        return metricObjects

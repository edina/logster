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

class DMMapServerLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.mapserver_maps = {}
        self.mapserver_resp = {}

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.reg = re.compile('.*mapserv.*map=/etc/mapserver/mapfiles/(?P<mapcollection>.*)\.map.* (?P<code>\d+) \d+ Response: (?P<response>\d+).*', re.IGNORECASE)

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regMatch = self.reg.match(line)

        # FIXME don't like this duplicated code
        if regMatch:
            linebits = regMatch.groupdict()
            map_collection = "ms_" + linebits['mapcollection'].replace("/","-")
            code = linebits['code']
            response = int(linebits['response']) / float(1000) # convert usec to msec

            if map_collection in self.mapserver_maps and code in self.mapserver_maps[map_collection]:
              self.mapserver_maps[map_collection][code] += 1
              self.mapserver_resp[map_collection][code] += response
            else:
              if map_collection not in self.mapserver_maps:
                self.mapserver_maps[map_collection] = {}
                self.mapserver_resp[map_collection] = {}
              self.mapserver_maps[map_collection][code] = 1
              self.mapserver_resp[map_collection][code] = response

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        metricObjects = []
        for product, code_key in self.mapserver_maps.items():
          for code, count in self.mapserver_maps[product].items():
            metricObjects.append( MetricObject( product + "_count." + code, count, "Responses per minute" ) )
        for product, code_key in self.mapserver_resp.items():
          for code, response in self.mapserver_resp[product].items():
            count = self.mapserver_maps[product][code]
            avg_response = response / float(count)
            metricObjects.append( MetricObject( product + "_response." + code, avg_response, "Avg Response Time per minute" ) )

        return metricObjects

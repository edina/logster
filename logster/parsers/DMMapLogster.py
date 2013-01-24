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
        # TODO replace with a dict lookup
        self.agcensus = 0
        self.geology = 0
        self.historic = 0
        self.landuse = 0
        self.marine = 0
        self.os = 0
        self.ukb = 0
        self.unknown = 0
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.reg = re.compile('.*mapserv.*map=mapfiles/(?P<mapcollection>\w+)/.*\.map.*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regMatch = self.reg.match(line)

        if regMatch:
            linebits = regMatch.groupdict()
            map_collection = linebits['mapcollection']

            if (map_collection == 'agcencus'):
              self.agcensus += 1
            elif (map_collection == 'geology'):
              self.geology += 1
            elif (map_collection == 'historic'):
              self.historic += 1
            elif (map_collection == 'landuse'):
              self.landuse += 1
            elif (map_collection == 'marine'):
              self.marine += 1
            elif (map_collection == 'os'):
              self.os += 1
            elif (map_collection == 'ukb'):
              self.ukb += 1
            else:
              self.unknown += 1
        # ignore non-matching lines since our apache log is full of crap
        # TODO: add clive test here

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''
        self.duration = duration

        # Return a list of metrics objects
        return [
            MetricObject("agcensus", (self.agcensus / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("geology", (self.geology / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("historic", (self.historic / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("landuse", (self.landuse / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("marine", (self.marine / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("os", (self.os / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("ukb", (self.ukb / (self.duration / 60.0) ), "Responses per sec"),
            MetricObject("unknown", (self.unknown / (self.duration / 60.0) ), "Responses per sec"),
        ]

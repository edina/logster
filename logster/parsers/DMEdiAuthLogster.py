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

import re

from logster.logster_helper import MetricObject, LogsterParser

class DMEdiAuthLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.logins = {}

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.reg_login = re.compile(r'.*GET /cgi-bin/getSession.* HTTP/\d.\d" (?P<code>\d+) .*')

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        reg_login_match = self.reg_login.match(line)

        if reg_login_match:
            linebits = reg_login_match.groupdict()
            code = linebits['code']
            if code in self.logins:
                self.logins[code] += 1
            else:
                self.logins[code] = 1
        # ignore non-matching lines

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        metric_objects = []
        for code, count in self.logins.items():
            metric_objects.append(MetricObject("ediauth_count." + code, count, "Logins per minute"))

        return metric_objects

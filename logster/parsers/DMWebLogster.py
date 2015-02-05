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

class DMWebLogster(LogsterParser):

    def __init__(self, option_string=None):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.logins = 0
        self.registrations = 0
        self.downloads = 0

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.regLogin = re.compile('.*GET /digimap/login.*')
        self.regRegister = re.compile('.*POST /digimap/registrations/register-user.*')
        self.regDownloads = re.compile('.*POST /datadownload/submitorder.*')


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regLoginMatch = self.regLogin.match(line)
        regRegisterMatch = self.regRegister.match(line)
        regDownloadMatch = self.regDownloads.match(line)

        if regLoginMatch:
          self.logins += 1
        elif regRegisterMatch:
          self.registrations += 1
        elif regDownloadMatch:
          self.downloads += 1
        # ignore non-matching lines

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        metricObjects = []
        metricObjects.append( MetricObject( "logins_count", self.logins, "Logins per minute" ) )
        metricObjects.append( MetricObject( "registrations_count", self.registrations, "Registrations per minute" ) )
        metricObjects.append( MetricObject( "download_submit_count", self.downloads, "Download Submits per minute" ) )

        return metricObjects

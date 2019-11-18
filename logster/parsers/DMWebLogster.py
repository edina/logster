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
        self.logins = {} # logins via EdiAuth/Digimap
        self.loginsResponse = {}
        self.loginsApi = {} # logins via the schools API(DataNation)
        self.loginsApiResponse = {}
        self.registrations = {}
        self.registrationsResponse = {}
        self.downloads = {}
        self.downloadsResponse = {}
        self.mapproxy = {}
        self.mapproxyResponse = {}
        self.mapproxyWms = {}
        self.mapproxyWmsResponse = {}

        self.schoolsV1Mapproxy = {}
        self.schoolsV1MapproxyResponse = {}

        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line.
        self.regLogin = re.compile('.*GET /login.* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regLoginApi = re.compile('.*POST /roam/api/schools/login.* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regRegister = re.compile('.*PUT /api/user/register HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regDownloads = re.compile('.*POST (/roam/api/download/orders|/datadownload/submitorder).* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regMapproxy = re.compile('.*GET /mapproxy/wmsMap.* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regMapproxyWms = re.compile('.*GET /mapproxy/wms/.*GetMap.* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')
        self.regSchoolsV1Mapproxy = re.compile('.*GET /dfsmapproxy/wmsMap.* HTTP/\d.\d" (?P<code>\d+) .* (?P<response>\d+) [\w\d-]+ .$')

    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        # Apply regular expression to each line and extract interesting bits.
        regLoginMatch = False
        if "MONITOR" not in line and "idp.edina.ac.uk" not in line:
          regLoginMatch = self.regLogin.match(line)
        regLoginApiMatch = self.regLoginApi.match(line)
        regRegisterMatch = self.regRegister.match(line)
        regDownloadMatch = self.regDownloads.match(line)
        regMapproxyMatch = self.regMapproxy.match(line)
        regMapproxyWmsMatch = self.regMapproxyWms.match(line)
        regSchoolsV1MapproxyMatch = self.regSchoolsV1Mapproxy.match(line)

        if regLoginMatch:
          linebits = regLoginMatch.groupdict()
          self.populate(self.logins, self.loginsResponse, linebits)
        elif regLoginApiMatch:
          linebits = regLoginApiMatch.groupdict()
          self.populate(self.loginsApi, self.loginsApiResponse, linebits)
        elif regRegisterMatch:
          linebits = regRegisterMatch.groupdict()
          self.populate(self.registrations, self.registrationsResponse, linebits)
        elif regDownloadMatch:
          linebits = regDownloadMatch.groupdict()
          self.populate(self.downloads, self.downloadsResponse, linebits)
        elif regMapproxyMatch:
          linebits = regMapproxyMatch.groupdict()
          self.populate(self.mapproxy, self.mapproxyResponse, linebits)
        elif regMapproxyWmsMatch:
          linebits = regMapproxyWmsMatch.groupdict()
          self.populate(self.mapproxyWms, self.mapproxyWmsResponse, linebits)
        elif regSchoolsV1MapproxyMatch:
          linebits = regSchoolsV1MapproxyMatch.groupdict()
          self.populate(self.schoolsV1Mapproxy, self.schoolsV1MapproxyResponse, linebits)
        # ignore non-matching lines

    def populate(self, countDict, responseDict, linebits):
        code = linebits['code']
        response = int(linebits['response']) / float(1000) # convert usec to msec
        if code in countDict:
            countDict[code] += 1
            responseDict[code] += response
        else:
            countDict[code] = 1
            responseDict[code] = response

    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        metricObjects = []
        self.record_metric(metricObjects, self.logins, self.loginsResponse, "logins", "Logins per minute")
        self.record_metric(metricObjects, self.loginsApi, self.loginsApiResponse, "logins_api", "API Logins per minute")
        self.record_metric(metricObjects, self.registrations, self.registrationsResponse, "registrations", "Registrations per minute")
        self.record_metric(metricObjects, self.downloads, self.downloadsResponse, "download_submit", "Download Submits per minute")
        self.record_metric(metricObjects, self.mapproxy, self.mapproxyResponse, "mapproxy", "Mapproxy tiles per minute")
        self.record_metric(metricObjects, self.mapproxyWms, self.mapproxyWmsResponse, "mapproxy_wms", "Mapproxy WMS requests per minute")
        self.record_metric(metricObjects, self.schoolsV1Mapproxy, self.schoolsV1MapproxyResponse, "schools_v1_mapproxy", "Schools V1 Mapproxy tiles per minute")

        return metricObjects

    def record_metric(self, metricObjects, countDict, responseDict, metricName, description):
        for code, count in countDict.items():
            metricObjects.append( MetricObject( metricName + "_count." + code, count, description ) )
        for code, responseTotal in responseDict.items():
            count = countDict[code]
            response = responseTotal / float(count)
            metricObjects.append( MetricObject( metricName + "_response." + code, response, "Avg Response " + description ) )

#!/usr/bin/env python2.7
# coding: utf-8

import time
import os
import re
import datetime
from stat import ST_MTIME
from logging.handlers import BaseRotatingHandler


class MtTimedFileHandler(BaseRotatingHandler):
    def __init__(self, filename, max_bytes=0, backup_count=0, when='midnight', interval=1,
                 encoding=None, delay=False, utc=False):
        BaseRotatingHandler.__init__(self, filename, 'a', encoding, delay)
        self.mid_night = 24 * 60 * 60
        self.max_bytes = max_bytes
        self.when = when.upper()
        self.backupCount = backup_count
        self.utc = utc
        if self.when == 'S':
            self.interval = 1
        elif self.when == 'M':
            self.interval = 60
        elif self.when == 'H':
            self.interval = 60 * 60
        elif self.when == 'D' or self.when == 'MIDNIGHT':
            self.interval = 60 * 60 * 24
        elif self.when.startswith('W'):
            self.interval = 60 * 60 * 24 * 7
            if len(self.when) != 2:
                raise ValueError("You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s" % self.when)
            if self.when[1] < '0' or self.when[1] > '6':
                raise ValueError("Invalid day specified for weekly rollover: %s" % self.when)
            self.dayOfWeek = int(self.when[1])
        else:
            raise ValueError("Invalid rollover interval specified: %s" % self.when)
        self.suffix = "%Y%m%d%H%M%S%f"
        self.extMatch = re.compile(r"^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}\d{6}$")
        self.interval *= interval
        if os.path.exists(filename):
            t = os.stat(filename)[ST_MTIME]
        else:
            t = int(time.time())
        self.rolloverAt = self.computeRollover(t)

    def computeRollover(self, current_time):
        """
        Work out the rollover time based on the specified time.
        """
        result = current_time + self.interval
        if self.when == 'MIDNIGHT' or self.when.startswith('W'):
            if self.utc:
                t = time.gmtime(current_time)
            else:
                t = time.localtime(current_time)
            current_hour = t[3]
            current_minute = t[4]
            current_second = t[5]
            r = self.mid_night - ((current_hour * 60 + current_minute) * 60 + current_second)
            result = current_time + r
            if self.when.startswith('W'):
                day = t[6]
                if day != self.dayOfWeek:
                    if day < self.dayOfWeek:
                        days_to_wait = self.dayOfWeek - day
                    else:
                        days_to_wait = 6 - day + self.dayOfWeek + 1
                    new_rollover_at = result + (days_to_wait * (60 * 60 * 24))
                    if not self.utc:
                        dst_now = t[-1]
                        dst_at_rollover = time.localtime(new_rollover_at)[-1]
                        if dst_now != dst_at_rollover:
                            if not dst_now:
                                addend = -3600
                            else:
                                addend = 3600
                            new_rollover_at += addend
                    result = new_rollover_at
        return result

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        record is not used, as we are just comparing times, but it is needed so
        the method signatures are the same
        """
        t = int(time.time())
        if t >= self.rolloverAt:
            return 1
        if self.stream is None:
            self.stream = self._open()
        if self.max_bytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.max_bytes:
                return 1
        return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        dir_name, base_name = os.path.split(self.baseFilename)
        file_names = os.listdir(dir_name)
        result = []
        prefix = base_name + "."
        prefix_len = len(prefix)
        for fileName in file_names:
            if fileName[:prefix_len] == prefix:
                suffix = fileName[prefix_len:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dir_name, fileName))
        result.sort()
        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]
        return result

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        file_size = -1
        if self.stream:
            file_size = self.stream.tell()
            self.stream.close()
            self.stream = None
        current_time = int(time.time())
        dst_now = time.localtime(current_time)[-1]
        now = datetime.datetime.now()
        dfn = self.baseFilename + "." + now.strftime(self.suffix)
        if os.path.exists(dfn):
            os.remove(dfn)
        if os.path.exists(self.baseFilename):
            if file_size == 0:
                os.remove(self.baseFilename)
            else:
                os.rename(self.baseFilename, dfn)
        if self.backupCount > -1:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.stream = self._open()
        new_rollover_at = self.computeRollover(current_time)
        while new_rollover_at <= current_time:
            new_rollover_at += self.interval
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dst_at_rollover = time.localtime(new_rollover_at)[-1]
            if dst_now != dst_at_rollover:
                if not dst_now:
                    addend = -3600
                else:
                    addend = 3600
                new_rollover_at += addend
        self.rolloverAt = new_rollover_at

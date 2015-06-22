#!/usr/bin/env python2.7
import datetime
import os, sys, errno
import config

# set import path relative to this file
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__)))
import ctm

## logging debug
import logging
logging.getLogger().setLevel(ctm.config.loglevel)

now = datetime.datetime.now()
for i in range(30):
    yesterday = (now - datetime.timedelta(days=i)) - datetime.timedelta(days=1)
    ymd = yesterday.strftime('%Y/%m/%d')
    line = '/opt/nyt/ctm/scroll-depth-mrjob.py -r emr '+ config.s3_evt_url +'/' + ymd + '/* --conf-path /opt/nyt/ctm/mrjob.conf --output-dir=s3://' + config.s3_bucket + '/' + config.s3_depth_data + '/' + ymd + '/ > /dev/null'
    logging.debug(line)
    sys.stdout.flush()
    os.system(line)

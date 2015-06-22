#!/usr/bin/env python2.7
# 
# MRjob for collecting click data
#
# Output format:
# [0, uid, assetid, n, 0]   0
#                   
#   where n is the number of times this user visited this asset.
#   The zeroes are included so that 
#
# Example commmand for running this script:
# python clicks-mrjob.py -r emr s3://bi.ec2.nytimes.com/prd/event-tracker/datums/page/2015/05/03/* --output-dir=s3://personalization.ec2.nytimes.com/dev/ctm-behavior/05/03
#

from mrjob.job import MRJob
from mrjob.emr import EMRJobRunner
from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol

class MRWordCounter(MRJob):    
    INPUT_PROTOCOL = JSONValueProtocol

    def get_fields_mapper(self, key, line):
        if line.has_key("cookieData") and line.has_key("assetData") and line.has_key('originalData'):
            cookieData = line['cookieData']
            assetData = line['assetData']
            otherData = line['originalData']

            if cookieData.has_key('nyts'):
                nyts = cookieData['nyts']
                if (nyts.has_key('uid') & assetData.has_key("type") & assetData.has_key("assetId")):
                    uid = nyts['uid']
                    typew = assetData['type']
                    aid = assetData['assetId']
                    if typew=='Article' or typew=='Video' or typew=='BlogPost':
                        if 'depth' in otherData:
                            depth = otherData['depth']
                            yield [uid, aid, depth], 1

    def get_fields_reducer(self, key, values):
        sum=0
        for value in values:
            sum = sum+1

        yield key[0], key[1], key[2]

    def steps(self):
        return [self.mr( mapper  = self.get_fields_mapper, 
                         reducer = self.get_fields_reducer)]

if __name__ == '__main__':
    MRWordCounter.run()
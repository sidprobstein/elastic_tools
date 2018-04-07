#!/usr/local/bin/python2.7
# encoding: utf-8
'''
@author:     Sid Probstein
@license:    MIT License (https://opensource.org/licenses/MIT)
@contact:    sidprobstein@gmail.com
'''

import argparse
import glob
import sys
import json

from elasticsearch import Elasticsearch

#############################################    

def main(argv):
    parser = argparse.ArgumentParser(description='Index one or more json files (including lists) into elastic')
    parser.add_argument('-i', '--indexname', help="name of the index to use")
    parser.add_argument('-t', '--type', help="type of item to index")
    parser.add_argument('-f', '--fid', action="store_true", help="use the filename as elastic id")
    parser.add_argument('-r', '--recurse', action="store_true", help="recursively index files in sub-directories")
    parser.add_argument('filespec', help="path to the json files you want to index")
    args = parser.parse_args()

    # initialize
    lstFiles = []
    nFiles = 0
    nSent = 0
    
    if args.filespec:
        lstFiles = glob.glob(args.filespec)
    else:
        sys.exit()

    es = Elasticsearch('http://localhost:9200/')

    for sFile in lstFiles:

        # process the files
        if os.path.isdir(sFile):
            if args.recurse:
                # recurse into directory
                for sNewFile in glob.glob(sFile + '/*'):
                    lstFiles.append(sNewFile)
            continue
        
        print "index.py: reading:", sFile,
        try:
            f = open(sFile, 'r')
        except Exception, e:
            print "error:", e
            continue
        try:
            jData = json.load(f)
        except Exception, e:
            print "error:", e
            f.close()
            continue
        f.close()
        print "ok, indexing",

        if type(jData) != list:
            print "file:",
            # make it a list
            jTmp = jData
            jData = []
            jData.append(jTmp)
        else:
            print "list:",

        nFiles = nFiles + 1
        
        for jRec in jData:
            try:
                if args.fid:
                    res = es.index(index=args.indexname, doc_type=args.type, id=sFile, body=jRec)
                else:
                    res = es.index(index=args.indexname, doc_type=args.type, body=jRec)
                print res['_id'], res['created'],
            except Exception, e:
                print "error:", e
                continue
            nSent = nSent + 1
        print "ok"
        
    # end for
        
    print "index.py: indexed", nSent, "records from", nFiles

# end main

#############################################    
    
if __name__ == "__main__":
    main(sys.argv)

# end
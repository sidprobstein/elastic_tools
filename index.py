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
import os

from elasticsearch import Elasticsearch

#############################################


def main(argv):
    parser = argparse.ArgumentParser(description='Index one or more json files (including lists) into elastic')
    parser.add_argument('-i', '--indexname', help="name of the index to use")
    parser.add_argument('-f', '--fid', action="store_true", help="use the filename as elastic id")
    parser.add_argument('-r', '--recurse', action="store_true", help="recursively index files in sub-directories")
    parser.add_argument('-s', '--server', help="the elastic server to send to in URL format")
    parser.add_argument('filespec', help="path to the json files you want to index")
    args = parser.parse_args()

    # initialize
    n_files = 0
    n_errors = 0
    n_sent = 0
    
    if args.filespec:
        lst_files = glob.glob(args.filespec)
    else:
        sys.exit()

    if args.server:
        elastic = Elasticsearch(args.server)
    else:
        elastic = Elasticsearch('http://localhost:9200/')

    for s_file in lst_files:

        # process the files
        if os.path.isdir(s_file):
            if args.recurse:
                # recurse into directory
                for s_new_file in glob.glob(s_file + '/*'):
                    lst_files.append(s_new_file)
            continue
        
        print "index.py: reading:", s_file,
        try:
            f = open(s_file, 'r')
        except Exception, e:
            print "error:", e
            continue
        try:
            json_data = json.load(f)
        except Exception, e:
            print "error:", e
            f.close()
            continue
        f.close()
        print "ok, indexing",

        if type(json_data) != list:
            print "file:",
            # make it a list
            j_tmp = json_data
            json_data = []
            json_data.append(j_tmp)
        else:
            print "list:",

        n_files = n_files + 1

        for json_record in json_data:
            try:
                if args.fid:
                    res = elastic.index(index=args.indexname, id=s_file, body=json_record)
                else:
                    res = elastic.index(index=args.indexname, body=json_record)
            except elasticsearch.ElasticsearchException, elastic1:
                print "error:", elastic1
                continue
            print res['_id'], res['result'],
            if res['result'] == 'created':
                print "ok"
                n_sent = n_sent + 1
            # else:
            #     n_errors = n_errors + 1
            #     print "*****"

    # end for
        
    print "index.py: indexed", n_sent, "records from", n_files, "with", n_errors, "errors"

# end main

#############################################


if __name__ == "__main__":
    main(sys.argv)

# end

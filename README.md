# elastic_tools

Tools for indexing content into elastic search

---

# index.py

## Usage
```
python index.py [-h] [-d] [-i INDEXNAME] [-t TYPE] filespec
```

## Arguments
```
-h requests help
-d specifies display of additional diagnostic information
-i INDEXNAME specifies the target index
-t TYPE specifies the index type
-r recurse into sub-directories
-f include filename as id
filespec must be the path to one or more json files
```

## Notes
* index.py reports the status and elastic id of every record sent for indexing
* index.py automatically handles single json and json lists (or array) files
* if -f is specified, index.py adds the filename as id to each record
* use -r to traverse sub-directories #fastpbfast#

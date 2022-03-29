# common-crawl-dl
Download all the common crawl CDX files that are relevant for a set of given top level domains.

# Method
Based off https://groups.google.com/g/common-crawl/c/3QmQjFA_3y4/m/vTbhGqIBBQAJ

Extract:

Want to fetch index files for a single top-level domain (here .fr)?

- the file list contains a cluster.idx file
cc-index/collections/CC-MAIN-2018-05/indexes/cluster.idx
- fetch it, e.g.:
wget https://commoncrawl.s3.amazonaws.com/cc-index/collections/CC-MAIN-2018-05/indexes/cluster.idx
- the first field in the cluster.idx contains the SURT representation of the URL,
with the reversed host/domain name:
fr,01-portable)/pal-et-si-internet-nexistait-pas.htm
- it's easy to list the cdx files containing all results from the .fr TLD:
grep '^fr,' cluster.idx | cut -f2 | uniq
cdx-00193.gz
cdx-00194.gz
cdx-00195.gz
cdx-00196.gz
That's only 4 files! I'm sure you're able to find the full path/URL
in the file list. If not, I'm happy to help.
- .com results make more than 50% of the index:
grep '^com,' cluster.idx | cut -f2 | uniq | wc -l
155
Please, fetch the index files directly. That's even much faster
and you can get all .com URLs from a monthly index in about one hour.
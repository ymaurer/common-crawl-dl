#!/usr/bin/python

import re
import pprint
import gzip
import os
from pathlib import Path
from FileDownloader import FileDownloader

url_prefix = 'https://commoncrawl.s3.amazonaws.com/'
DL_FOLDER = 'Downloads/'
tlds = ['lu','fr','dk']
IDX_URL = 0
IDX_LOCALINDEX_PATH = 1
IDX_LOCALCLUSTER_NAME = 2

pp = pprint.PrettyPrinter(indent=4)

dl = FileDownloader()

# get the HTML file listing all the crawls
pat = re.compile('<a href="([^\\"]*cc-index.paths.gz)">([^\\/]*)')
dl.get_file("http://index.commoncrawl.org/", "index-paths.html")
dl.wait_all_finished()

# parse the crawl list and get all the index-paths files which list the locations of the index files
indexes = {}
with open(DL_FOLDER + '/index-paths.html', 'r') as file:
	data = file.read()
	it = pat.finditer(data)
	for match in it:
		url = match.group(1)
		crawl_name = match.group(2)
		localfile = crawl_name + '-cc-index.paths.gz'
		indexes[crawl_name] = [url, localfile]
		dl.get_file(url, localfile)
dl.wait_all_finished()

# get the cluster.idx file per crawl, which will tell us in which cdx is the tld of interest
patCluster = re.compile('indexes\/cluster.idx')
cdxFiles = {}

for i in indexes:
	found = 0
	cdxFiles[i] = {}
	with gzip.open(DL_FOLDER + indexes[i][IDX_LOCALINDEX_PATH], mode='rt') as z:
		for line in z:
			if patCluster.search(line):
				found = 1
				url_cluster = url_prefix + line.rstrip()
				local_cluster_name = i + '-cluster.idx'
				dl.get_file(url_cluster, local_cluster_name)
				indexes[i].append(local_cluster_name)
			else:
				fullpath = line.rstrip()
				fname = os.path.basename(fullpath)
				cdxFiles[i][fname] = fullpath
	if found == 0:
		print(f'ERROR: {f} cluster.idx not found')
dl.wait_all_finished()

# check the cluster.idx files to see which CDX files we need to download for each TLD
for crawl in indexes:
	with open(DL_FOLDER + indexes[crawl][IDX_LOCALCLUSTER_NAME]) as f:
		tld_cdx_list = {}
		for tld in tlds:
			tld_cdx_list[tld] = {}
		for line in f:
			fields = line.split()
			for tld in tlds:
				surt = tld + ','
				if fields[0][0:3] == surt:
					if fields[2] in tld_cdx_list[tld]:
						tld_cdx_list[tld][fields[2]] = tld_cdx_list[tld][fields[2]] + 1
					else:
						tld_cdx_list[tld][fields[2]] = 1
		for tld in tld_cdx_list:
			for cdx in tld_cdx_list[tld]:
				print(f"INFO: Download CDX {crawl} {tld} {cdx} {tld_cdx_list[tld][cdx]}")
				cdx_url = url_prefix + cdxFiles[crawl][cdx]
				outfname = tld + '_' + crawl + '_'  + cdx
				if outfname[-3:] == '.gz':
					outfname = outfname[0:-3] + '.cdx.gz'
				dl.get_file(cdx_url, outfname)
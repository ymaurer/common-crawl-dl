# adapted from https://gist.github.com/stefanfortuin

import os, sys, threading
import requests
import time


class FileDownloader():
	def __init__(self, max_threads=10):
		self.n_threads = max_threads
		self.download_counter = 0
		self.sema = threading.Semaphore(value=self.n_threads)
		self.headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
		self.block_size = 1024

	def t_getfile(self, link, filename, session):
		""" 
		Threaded function that uses a semaphore 
		to not instantiate too many threads 
		"""
		self.sema.acquire()
		self.download_counter = self.download_counter + 1

		filepath = os.path.join(os.getcwd() + '/Downloads/' + str(filename))
		os.makedirs(os.path.dirname(filepath), exist_ok=True)
		
		if not os.path.isfile(filepath):
			self.download_new_file(link, filepath, session)
		else:
			print(f"WARN: {link} downloaded to {filepath} already exists, no retry")
			self.download_counter = self.download_counter - 1
			self.sema.release()
			# uncomment this retun statement to compare filesize of on-disk and on-server files and attempt continued download
			return
			
			current_bytes = os.stat(filepath).st_size

			headers = requests.head(link).headers
			if 'content-length' not in headers:
				print(f"WARN: {link} downloaded to {filepath} already exists, but server doesn't support content-length, so no verification. Aborting download")
				self.download_counter = self.download_counter - 1
				self.sema.release()
				return
			
			total_bytes = int(requests.head(link).headers['content-length'])

			if current_bytes < total_bytes:
				self.continue_file_download(link, filepath, session, current_bytes, total_bytes)
			else:
				print(f"INFO: {link} already done: {filepath}")
		
		self.download_counter = self.download_counter - 1
		self.sema.release()
	
	def download_new_file(self, link, filepath, session):
		print(f"INFO: {link} downloading: {filepath}")
		if session == None:
			try:
				request = requests.get(link, headers=self.headers, timeout=30, stream=True)
				self.write_file(request, filepath, 'wb', link)
			except requests.exceptions.RequestException as e:
				print(f"ERROR: {link} not downloaded - {e}")
		else:
			request = session.get(link, stream=True)
			self.write_file(request ,filepath, 'wb', link)

	def continue_file_download(self, link, filepath, session, current_bytes, total_bytes):
		print(f"INFO: {link} resuming to: {filepath}")
		range_header = self.headers.copy()
		range_header['Range'] = f"bytes={current_bytes}-{total_bytes}"

		try:
			request = requests.get(link, headers=range_header, timeout=30, stream=True)
			self.write_file(request, filepath, 'ab', link)
		except requests.exceptions.RequestException as e:
			print(f"ERROR: {link} not continued downloading - {e}")
	
	def write_file(self, content, filepath, writemode, link):
		with open(filepath, writemode) as f:
			for chunk in content.iter_content(chunk_size=self.block_size):
				if chunk:
					f.write(chunk)

		print(f"SUCCESS: {link} completed file {filepath}", end='\n')
		f.close()
		
	def wait_all_finished(self):
		""" wait until all downloads are finished by checking the counter """
		while self.download_counter > 0:
			time.sleep(0.2)
			

	def get_file(self, link, filename, session=None):
		""" Downloads the file"""
		thread = threading.Thread(target=self.t_getfile, args=(link, filename, session))
		thread.start()
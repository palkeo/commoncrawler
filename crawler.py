import boto
import warc

from boto.s3.key import Key
from gzipstream import GzipStreamFile

import sys
import gzip
import multiprocessing
import datetime
import re

class Process:
    def __init__(self):
        global worker

        self.count = 0
        self.start = datetime.datetime.now()

        sys.stderr.write("Will spawn %s workers.\n" % multiprocessing.cpu_count())
        self.doc_q = multiprocessing.JoinableQueue(100)
        self.stdout_lock = multiprocessing.Lock()

        self.workers = []
        for _ in range(multiprocessing.cpu_count()):
            p = multiprocessing.Process(target=worker, args=(self.doc_q, self.stdout_lock))
            p.start()
            self.workers.append(p)

        try:
            self.run()
        finally:
            sys.stderr.write("Shutting down workers...")
            for i, worker in enumerate(self.workers):
                try:
                    worker.terminate()
                except Exception:
                    sys.stderr.write("Unable to terminate worker %s !" % i)

    def run(self):
        f = gzip.GzipFile('all.warc.gz', 'r')

        for line in f.readlines():
            line = line.strip()
            sys.stderr.write('Retrieving %s.\n' % line)
            self.parse_archive(line)
            sys.stderr.write('%s documents parsed. %s doc/s.\n' % (self.count, self.count // (datetime.datetime.now() - self.start).total_seconds()))

        # Wait for all the workers
        self.doc_q.join()

    def parse_archive(self, line):
        # Connect to Amazon S3 using anonymous credentials
        conn = boto.connect_s3(anon=True)
        pds = conn.get_bucket('aws-publicdatasets')

        # Start a connection to one of the WARC files
        k = Key(pds, line)
        f = warc.WARCFile(fileobj=GzipStreamFile(k))

        for record in f:
            if record['Content-Type'] != 'application/http; msgtype=response':
                continue
            self.doc_q.put(record.payload.read())
            self.count += 1


""" 
From there, you will probably like to change some things :)
"""

UL = re.compile(r"<(ul|dl)[^>]*>(.+?)</\1>", re.IGNORECASE | re.DOTALL)
LI = re.compile(r"<(li|dt)[^>]*>(.+?)</\1>", re.IGNORECASE)
ITEM = re.compile(r"^[\w -]{2,40}$", re.IGNORECASE)

def process_record(payload):
    # The HTTP response is defined by a specification: first part is headers (metadata)
    # and then following two CRLFs (newlines) has the data for the response
    headers, body = payload.split('\r\n\r\n', 1)
    if 'content-type: text/html' not in headers.lower():
        return set()

    all_items = set()

    # ul-li

    for ul in UL.finditer(body):
        lis = [i.strip() for _, i in LI.findall(ul.group(2))]
        if all(ITEM.match(i) for i in lis) and len(lis) > 2:
            all_items.add(tuple(lis))

    return all_items

def worker(doc_q, stdout_lock):
    while True:
        all_items = process_record(doc_q.get())
        with stdout_lock:
            for items in all_items:
                print('|'.join(items))
        doc_q.task_done()

if __name__ == '__main__':
    Process()

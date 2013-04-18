"""This is uploaded to S3 and run by each of the cloud instances. The stuff happening on your computer is in docsplitter.py."""

import os
import random

from aws import AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_BUCKET

import boto
from boto.s3.key import Key
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message


conn_s3 = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
sqsconn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

q = sqsconn.create_queue('todo')

bucket_out = conn_s3.get_bucket(AWS_BUCKET)

rs = q.get_messages()

while len(rs):
    string = rs[0].get_body()
    url = string[string.find(',')+1:]
    id = string[:string.find(',')]    
    q.delete_message(rs[0])
    k=Key(bucket_out,'output/%s' % id)
    os.system('wget --no-check-certificate "%s" -O in%s.pdf' % (url,id))
    os.system('pdftotext -layout in%s.pdf' % id)
    docsplit = False
    try:
        text = open('in%s.txt' % id).read()
        if len(text)<200:
            docsplit = True
    except:
        docsplit = True
    if docsplit:
        os.system('docsplit text in%s.pdf' % id)
        try:
            text = open('in%s.txt' % id).read()
        except:
            text = 'OCR_FAILED'
        print "%s failed" % string
        pass
    k.set_contents_from_string(text, policy='private') 
    os.system('rm in%s.pdf' % id)
    os.system('rm in%s.txt' % id)
    rs = q.get_messages()


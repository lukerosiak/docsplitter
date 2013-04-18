import time
import math

from aws import *

import boto
from boto.s3.key import Key
from boto.ec2.connection import EC2Connection
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message


ami_id = 'ami-57b7013e' #custom docsplit ami instance store
#ami_id = 'ami-6f209506' #custom docsplit ami ebs




class Docsplitter:

    def __init__(self):
        
        self.ec2conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
        self.sqsconn = SQSConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)        
        self.q = self.sqsconn.create_queue('todo')
        
        self.conn_s3 = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
        try:
            self.bucket = self.conn_s3.create_bucket(AWS_BUCKET)
        except:
            self.bucket = self.conn_s3.get_bucket(AWS_BUCKET)


    def destroy_ec2(self):     
        """kill any existing instances -- only call this if you're not using EC2 for unrelated purposes!!!!"""
                                
        ins = self.ec2conn.get_all_spot_instance_requests()
        if len(ins):
            self.ec2conn.cancel_spot_instance_requests([x.id for x in ins])
        all = self.ec2conn.get_all_instances()
        if len(all):
            self.ec2conn.terminate_instances([x.instances[0].id for x in all])
        print "shutting down %s old instances" % len(all)


    def destroy_queue(self):
        """clear the SQS queue of files to process"""
        
        self.sqsconn.delete_queue(self.q, force_deletion=True)
        time.sleep(63) #amazon makes us wait a minute after deleting old queue
        self.q = self.sqsconn.create_queue('todo')
    
        
    def add_to_queue(self,tuples):        
        """Make a queue of files to process, given urlpairs, which is a series of two-tuples containing a primary key for an object and a URL of a PDF."""
        
        for t in tuples:
            string = "%s,%s" % (t[0],t[1]) 
            m = Message()
            m.set_body(string)
            status = self.q.write(m)
        print "added %s to list" % len(tuples)    


    def start(self,docs_per_hour):
        """how many computers should we spin up to process the just-generated list? how many docs can one process per hour?
        this uses medium instances, dual-core, and runs two processes at once."""
        
        instances = int(math.ceil(self.q.count() / docs_per_hour))
        if instances>AWS_MAX_INSTANCES:
            instances=AWS_MAX_INSTANCES

        k=Key(self.bucket,'s3.py')
        k.set_contents_from_filename('s3.py', policy='private') 
        k=Key(self.bucket,'aws.py')
        k.set_contents_from_filename('aws.py', policy='private') 

        
        script = """#!/bin/bash
cd /dev/shm
python -c "import boto
from boto.s3.key import Key
AWS_ACCESS_KEY_ID = '%s'
AWS_SECRET_ACCESS_KEY = '%s'
conn_s3 = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
bucket = conn_s3.get_bucket('%s')

k = bucket.get_key('s3.py')
py = k.get_contents_as_string()
fout = open('s3.py','w')
fout.write(py)
fout.close()

k = bucket.get_key('aws.py')
py = k.get_contents_as_string()
fout = open('aws.py','w')
fout.write(py)
fout.close()"


python s3.py &
p=$!
python s3.py

while kill -0 "$p"; do
    sleep 5
done

sudo shutdown -h now
        """ % (AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_BUCKET)
        
        print "spinning up %s instances for %s docs" % (instances,self.q.count())

        
        spot = self.ec2conn.request_spot_instances(AWS_PRICE_PER_HOUR, ami_id, count=instances, type='one-time', key_name=AWS_KEY_PAIR_NAME, user_data=script, instance_type=AWS_INSTANCE_TYPE)


    def retrieve(self):
        """get an iterator of tuples with (id,OCR'd text)"""
        
        keys = self.bucket.list()

        for key in keys:
            filename = key.name
            if filename.startswith('output/'):
                id = filename[len('output/'):]
                k = Key(self.bucket, filename)
                txt = k.get_contents_as_string()
                yield (id,txt)
            


    def showprogress(self):
        """check in every 5 minutes until everything's done"""

        remaining = self.q.count()
        while remaining>0:
            print "%s docs remaining" % remaining
            time.sleep(5*60)
            remaining = self.q.count()

    def delete_output(self):
        """remove the OCR'd text files from the Amazon S3 bucket after you no longer need them"""

        keys = self.bucket.list()
        for key in keys:
            filename = key.name
            if filename.startswith('output/'):
                k = Key(self.bucket, filename)
                self.bucket.delete_key(k)




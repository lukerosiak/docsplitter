Need to extract text from hundreds or thousands of PDFs? You can emulate the core capabilities of DocumentCloud for OCRing them without abusing its servers and without limitation.

DOCSPLITTER

A Python script for extracting text from large volumes of PDF files using Amazon Web Services (EC2, S3 and SQS).

It spins up a cluster of computers all using a pre-assembled disk image I created that has the PDFtoText and DocSplit software installed. It performs the most substantive portion of what people use DocumentCloud for, but much faster, without requiring an account with them and without putting strain on their servers. 

It will put in a spot request bid at 2 cents an hour per machine, and you will be responsible for the charges. Input your AWS credentials in aws_demo.py and rename it to aws.py.

Then look at demo.py. Basically, just give it one list of tuples with a primary key and URL to PDF, from your database or a CSV, wait a while, then retrieve tuples with the ID and extracted text, to update your database or save somewhere. The text files will also remain on your AWS bucket unless you remove them.

By Luke Rosiak of The Washington Times. Released under the same terms as DocumentCloud.

The only requirement you need on your computer is boto, the python library for interacting with AWS.

from docsplitter import Docsplitter

ds = Docsplitter()
ds.destroy_queue()        

#this SQL stuff isn't necessary for Docsplitter, just get the (primary_key, pdf_url) tuples to it however you want
import psycopg2
conn = psycopg2.connect("dbname=test")
cursor = conn.cursor()
cursor.execute("SELECT id,url FROM oge_document WHERE text='';")
tuples = cursor.fetchall()
#end SQL stuff

#select id,url from items where txt is null
ds.add_to_queue(tuples)        

ds.start(40) #the number of docs you expect one instance to be able to OCR per hour       

ds.showprogress()        

for pair in ds.retrieve():
    #update item set txt=pair[1] where id=pair[0]
    #more optional SQL stuff
    (id,text) = pair
    if id!='': #first record is sometimes blank for some reason
        print "id=%s text=%s" % (id,text[:100].strip())
        cursor.execute("UPDATE oge_document SET text=%s WHERE id=%s;", (text,id))
        conn.commit()
    

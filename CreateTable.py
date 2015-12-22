import Neo4jDrive
import CSVWrite
from py2neo import Graph
import csv
import math
graph = Graph("http://neo4j:vedmathai@localhost:7474/db/data/")

i=5
number=6
with open('../csv/eggs%s.csv'%number,'wb') as csvfile:
    writer=csv.writer(csvfile, delimiter=',',quotechar='{')
    writer.writerow(['Domain Class','CCS Score','DCS Score', 'Table','Overall Score'])
    theTable=[]
    domains={}
    for record in graph.cypher.execute("MATCH (n) where n.hyp='yes' return n.name, n.ccs, n.DCS"):
        ccs=record[1]
        dms=record[2]
        if ccs!=None and dms!=None and ccs!=0 and dms!=0: 
            r=[]
            domain=record[0]
            csvs=math.sqrt((ccs*ccs)+(dms*dms))
            table=Neo4jDrive.tableMembership(domain)
            entropy=-(ccs)/(ccs+dms)*math.log(ccs/(ccs+dms))-(dms)/(ccs+dms)*math.log(dms/(ccs+dms))
            overall=csvs*entropy*table
            domains[domain]=overall
            r.append(domain)    
            r.append(ccs)
            r.append(dms)
            r.append(table) 
            r.append(overall)
        theTable+=[r]
    theTable=sorted(theTable, key=lambda x: x[4],reverse=True)

    for record in theTable:
        writer.writerow(record)
number=7
theTable=[]
with open('../csv/eggs%s.csv'%number,'wb') as csvfile:
    writer=csv.writer(csvfile, delimiter=',',quotechar='{')
    writer.writerow(['Domain Class','Overall Score','Property', 'DMS','LMS','Source Column','Column'])
    
    for record in graph.cypher.execute("MATCH (n)-[r]-(m) where n.type='property' and m.hyp='yes' and r.type='domain' return n.name, m.name"):
        domain=record[1]
        prop=record[0]
        for rec in graph.cypher.execute("MATCH (n)-[r]-(m) where r.name=\"%(prop)s\" and r.type='property_rel' return r.dms, r.lms, n.name, m.name"):
            r=[]
            
            r.append(domain)    
            r.append(domains[domain])
            r.append(prop)
            r.append(rec[0]) 
            r.append(rec[1])
            r.append(rec[2])
            r.append(rec[3])
        theTable+=[r]
    theTable=sorted(theTable, key=lambda x: x[0],reverse=True)

    for record in theTable:
        writer.writerow(record)

        
        


    #CSVWrite.csvWrite(record)
        """
        m=[]
        if record[0] !=u'Broader':
            m.append(record[0])
            print record[1]
            m.append(record[1].properties["name"])
            m.append(record[2].properties["name"])
            m.append(record[2].properties["count"])
            writer.writerow(m)
       """



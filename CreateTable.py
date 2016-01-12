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
    numberOfColumns=Neo4jDrive.findTotalNumberOfColumns()[0][0]
    for record in graph.cypher.execute("MATCH (n) where n.hyp='yes' return n.name, n.ccs, n.DCS"):
        domain=record[0]        
        ccs=record[1]
        dcs=(Neo4jDrive.findNumberOfColumns(domain)[0][0]*1.0)/numberOfColumns
        r=[]
        table=Neo4jDrive.tableMembership(domain)
        if ccs!=None and dcs!=None and ccs!=0 and dcs!=0:  
            csvs=math.sqrt((ccs*ccs)+(dcs*dcs))
            entropy=-(ccs)/(ccs+dcs)*math.log(ccs/(ccs+dcs))-(dcs)/(ccs+dcs)*math.log(dcs/(ccs+dcs))
            overall=csvs*entropy*table
        else:
            overall='-'
        domains[domain]=overall
        r.append(domain)    
        r.append(ccs)
        r.append(dcs)
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
    
    for record in graph.cypher.execute("MATCH (n)-[r1]->(m)-[r2]->(o) where n.type='Column' and m.type='property' and o.hyp='yes' return n.name,r1.dms,r1.lms, m.name,o.name"):
        domain=record[4]
        prop=record[3]
        dms=record[1]
        lms=record[2]
        for rec in graph.cypher.execute("MATCH (n)-[r]->(m) where r.name=\"%(prop)s\" and r.type='property_rel' return n.name, m.name"%{'prop':prop}):
            r=[]
            source=rec[0]
            dest=rec[1]
            r.append(domain)    
            r.append(domains[domain])
            r.append(prop)
            r.append(dms) 
            r.append(lms)
            r.append(source)
            r.append(dest)
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



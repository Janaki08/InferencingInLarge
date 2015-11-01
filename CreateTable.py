import Neo4jDrive
import CSVWrite
from py2neo import Graph
import csv
graph = Graph("http://neo4j:vedmathai@localhost:7474/db/data/")

i=5
number=6
with open('../csv/eggs%s.csv'%number,'wb') as csvfile:
    writer=csv.writer(csvfile, delimiter=',',quotechar='{')
    writer.writerow(['BroaderII','Broader','Skos Class','Support','Column'])

    for record in graph.cypher.execute("MATCH (l)-[m]->(n)-[o]->(p)-[q]->(r) where m.rel_class='sc' and o.rel_class='sb' and q.rel_class='sb2' return l.name, m.support, n.name, p.name, r.name, sum(m.support) as s order by s desc "):
        r=[]
        column=record[0]
        skos=record[3]
        summation=0
        m=0
        #for z in graph.cypher.execute("MATCH (l)-[m]-(n) where l.name='%(col)s' and n.name=\"%(skos)s\" return m.support"%{'col':column,'skos':skos}):
         #   summation+=z[0]
        if record[5]!=0:
            r.append(record[4])    
            r.append(record[3])
            r.append(record[2])
            r.append(summation)
            r.append(record[0])
            writer.writerow(r)
        
        


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



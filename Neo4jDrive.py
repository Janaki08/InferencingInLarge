"""
This is a custom file written for using Py2Neo library.
"""
from py2neo import Graph, Node, Relationship
graph = Graph("http://neo4j:vedmathai@localhost:7474/db/data/")

def insertNode(nameOfNode):
    if type(findNodeByName(nameOfNode))!=type(Node()):
        return graph.create(Node("Node", name=nameOfNode))
    else:
        return findNodeByName(nameOfNode)

def insertNodeAndRelationship(nameOfNode2,Rel_type,nameOfNode1):
    insertNode(nameOfNode1)
    return insertRelationship(nameOfNode2,Rel_type,nameOfNode1)
    

def insertRelationship(a,nameOfRelation,b):
    k=graph.cypher.execute("match (m)-[n]-(o) where m.name='%(a)s' and n.name='%(nameOfRelation)s' and o.name='%(b)s' return n"%{'a':a, 'nameOfRelation':nameOfRelation, 'b':b})
    if len(k)!=0:
        return k[0]
    return graph.create_unique(Relationship(findNodeByName(a),nameOfRelation,findNodeByName(b)))

def findNodeByName(name):
    return graph.find_one("Node", property_key="name", property_value=name)



def findRelationshipsOfNode(name,Rel_type):
    return graph.match(start_node=findNodeByName(name), rel_type=Rel_type, end_node=None, bidirectional=False, limit=None)

def findRelationByName(node1, name, node2):
    return graph.cypher.execute("match (m)-[n]-(o) where m.name=\"%(m)s\" and o.name=\"%(o)s\" and n.rel_class=\"%(name)s\" return m,n,o"%{'m':node1,'o':node2,'name':name})

def findPropertyLink(node1, name, node2):
    return graph.cypher.execute("match (m)-[n]-(o) where m.name=\"%(m)s\" and o.name=\"%(o)s\" and n.name=\"%(name)s\" return m,n,o"%{'m':node1,'o':node2,'name':name})

def findCCNodes(name):
    z=graph.cypher.execute("match (col)-[r]-(n) where col.name=\"%(name)s\" and n.type=\"cc\" return n"%{'name':name})
    return [k[0]['name'] for k in z]

def findAllCCNodes():
    z=graph.cypher.execute("match (n) where n.type=\"cc\" return n")
    return [k[0]['name'] for k in z]

def tableMembership(name):
    z=graph.cypher.execute("MATCH (n)-[r]->(b)-[k]->(c)-[d]->(m) where  d.name='domain' and n.type='table' and c.type='property' and m.name=\"%(name)s\" return distinct n.name"%{'name':name})
    return len(z)

def findCCScore(node):
    z=graph.cypher.execute("Match (n)-[r]->(c) where n.name=\"%(name)s\" return count(r)"%{'name':node})
    return 

def findNumberOfColumns(name):
    z=graph.cypher.execute("Match (n)-[r]->(m)-[t]->(o) where n.type='Column' and m.type='property' and o.hyp='yes' and o.name=\"%(name)s\" return count(distinct n)"%{'name':name})
    return z
def findTotalNumberOfColumns():
    z=graph.cypher.execute("Match (n) where n.type='Column' return count(n)")
    return z

def findIncomingCCLinks(name):
    z=graph.cypher.execute("Match (n)-[r]->(m) where r.rel_class='cc' and m.name=\"%(name)s\" return r"%{'name':name})
    return z

#find the node here, write the cypher query here to return node where type is property.

if __name__=='__main__':
    for k in findIncomingCCLinks("http://dbpedia.org/class/yago/StatesAndTerritoriesOfIndia"):
        print k[0].properties['ccs']

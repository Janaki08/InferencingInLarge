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
    return graph.create_unique(Relationship(findNodeByName(a),nameOfRelation,findNodeByName(b)))

def findNodeByName(name):
    return graph.find_one("Node", property_key="name", property_value=name)



def findRelationshipsOfNode(name,Rel_type):
    return graph.match(start_node=findNodeByName(name), rel_type=Rel_type, end_node=None, bidirectional=False, limit=None)

def findRelationByName(node1, name, node2):
    return graph.cypher.execute("match (m)-[n]-(o) where m.name=\"%(m)s\" and o.name=\"%(o)s\" and n.rel_class=\"%(name)s\" return m,n,o"%{'m':node1,'o':node2,'name':name})

def findCCNodes(name):
    z=graph.cypher.execute("match (col)-[r]-(n) where col.name=\"%(name)s\" and n.type=\"cc\" return n"%{'name':name})
    return [k[0]['name'] for k in z]

def findAllCCNodes():
    z=graph.cypher.execute("match (n) where n.type=\"cc\" return n")
    return [k[0]['name'] for k in z]

def tableMembership(name):
    z=graph.cypher.execute("MATCH (f)-[]->(n)-[r]->(b)-[k]-(c)-[d]-(m) where f.type='table' and  k.name='property' and d.name='domain' and m.name=\"%(name)s\" return distinct f, m"%{'name':name})
    return len(z)

#find the node here, write the cypher query here to return node where type is property.

if __name__=='__main__':
    print insertNodeAndRelationship('River','a','b')

"""
The purpose of this file is to accept sparql queries run them against the online datasets and return a resultset.
The dependancies are SPARQLWrapper and JSON classes. Which in turn are dependent on rdflib for python.
"""

from SPARQLWrapper import SPARQLWrapper, JSON
endpoint = SPARQLWrapper("http://dbpedia.org/sparql")
query1="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dbp: <http://dbpedia.org/property/>

"""
query2="""
SELECT DISTINCT ?c ?b ?a WHERE {
?s ?p ?o.
?s dct:subject ?c.
?c skos:broader ?b.
?b skos:broader ?a.
{{?s rdfs:label ?l. ?l bif:contains "'%(name)s'" . FILTER regex(?l,"^%(name)s$" ,"i"). FILTER
(langMatches(lang(?l), "en"))} UNION
{?s dbp:name ?l1. ?l1 bif:contains "'%(name)s'". FILTER regex(?l1,"^%(name)s$" ,"i"). FILTER
(langMatches(lang(?l1), "en"))}}
}

"""

query3="""
SELECT DISTINCT ?s ?l ?d WHERE{
?s ?p ?o.
?s rdfs:label ?l. ?l bif:contains "'%(name)s'". FILTER regex(?l,"^%(name)s$" ,"i") . FILTER (langMatches(lang(?l), "en")).
?s rdf:type ?d
}

"""

query4="""
SELECT DISTINCT ?s ?p ?o ?d ?r WHERE{
?s rdfs:label ?l. ?l bif:contains "'%(name1)s'" . FILTER regex(?l,"^%(name1)s$" ,"i") . FILTER
(langMatches(lang(?l), "en")).
?o rdfs:label ?l1. ?l1 bif:contains "'%(name2)s'" . FILTER regex(?l1,"^%(name2)s$" ,"i") . FILTER
(langMatches(lang(?l1), "en")).
?s ?p ?o.
OPTIONAL{?p rdfs:domain ?d}.
OPTIONAL{?p rdfs:range ?r}
}
"""

query5="""
SELECT DISTINCT ?s ?p ?o ?d ?r WHERE{
?p rdfs:label ?l. ?l bif:contains "'%(name1)s'" . FILTER regex(?l,"^%(name1)s$" ,"i") . FILTER
(langMatches(lang(?l), "en")).
?s ?p ?o.
OPTIONAL{?p rdfs:domain ?d}.
OPTIONAL{?p rdfs:range ?r}
}
"""
query6="""
SELECT DISTINCT ?s ?p ?o ?d ?r WHERE{
?p rdfs:label ?l. ?l bif:contains "'%(name1)s'" . FILTER regex(?l,"^%(name1)s$" ,"i") . FILTER
(langMatches(lang(?l), "en")).
?s ?p ?o.
?s rdf:type ?d. 
?o rdf:type ?r}
}
"""


def findClass(name):
    query=query1+query2%{'name':name}
    return runSparql(query,{'c':'value','b':'value','a':'value'})

def findBottomUp(name):
    query=query1+query3%{'name':name}
    return runSparql(query,{'s':'value','l':'value','d':'value'})

def findProperty(name1, name2):
    query=query1+query4%{'name1':name1, 'name2':name2}
    return runSparql(query,{'s':'value','p':'value','o':'value','d':'value','r':'value'})

def findPropertyClassesFirst(name1):
    query=query1+query5%{'name1':name1}
    return runSparql(query,{'s':'value','p':'value','o':'value','d':'value','r':'value'})

def findPropertyClassesSecond(name1):
    query=query1+query6%{'name1':name1}
    return runSparql(query,{'s':'value','p':'value','o':'value','d':'value','r':'value'})
    


def runSparql(queryAppend,dictionary):
    queryAppend=query1+queryAppend #Add the select statements and etc. from the calling program
    endpoint.setQuery(queryAppend)
    endpoint.setReturnFormat(JSON)
    results=endpoint.query().convert()
    rlist=[]
    
    for res in results["results"]["bindings"] :
        row=[]
        for k in dictionary.keys():
	    row.append(res[k][dictionary[k]])
        rlist.append(row)
    return rlist



if __name__=='__main__':
    rlist=findBottomUp('Comic')
    for r in rlist:
        print r

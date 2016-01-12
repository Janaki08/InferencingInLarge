import CSVRead
import sparqlQuerypy
import Neo4jDrive
import random
from threading import Thread, Lock
import datetime

hypothesisSet=set()
sample=5

def main():
    columnNames=[]
    colNam={}
    csvitems={}
    size={}
    tables=["StatesandCapitals.csv","RiversandSourceState.csv"] 
    for i, nameOfFile in enumerate(tables):  #pushes each table as a node into the graph along with the columns
        Neo4jDrive.insertNode(nameOfFile)
        node=Neo4jDrive.findNodeByName(nameOfFile)
        node.properties['type']='table'
        node.push() #end of push
        columnNames+=[CSVRead.readCSV(nameOfFile,firstRow=True, choice=[0,1])]
        columnNames[i]=[c.strip() for c in columnNames[i]]
        colNam[nameOfFile]=[c.strip() for c in columnNames[i]]
        for j,name in enumerate(columnNames[i]):
            z=Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)[0]
            node=Neo4jDrive.findNodeByName(name)
            node.properties['type']='Column'
            node.push()
            z.properties['type']="Column"
            z.push() #end of the Column Pushing


        csvitems[nameOfFile]=CSVRead.readCSV(nameOfFile,firstRow=False,choice=[0,1])[1:] #stores each data set in a dictionary of lists
        size[nameOfFile]=[len(csvitems[nameOfFile])] #stores the sizes of the lists in a dictionary called size
        random.shuffle(csvitems[nameOfFile]) #shuffles for randomness
    relationships={}
    iterations=1
    convergence=False #the test flag for whether convergence has been reached
    while(not convergence):
        for table in tables:
            start=sample*(iterations-1)
            end=sample*iterations
            rt=runThread(table, csvitems[table][start:end], colNam[table],end,relationships)
            rt.start()
            rt.join()
        iterations+=1
        if end>5:convergence=True

class runThread(Thread): #The thread which will be run per sample per iteration.
    def __init__(self,table,data,columnNames,totalSize,relationships):
        Thread.__init__(self)
        self.data=data
        self.columnNames=columnNames
        self.totalSize=totalSize
        self.relationships=relationships
        print '---------'


    def run(self):
        #self.ccScores()
        self.dmsScore()





#----------------------CCS SCORE FUNCTION-----------------------#
    def ccScores(self):
        data=self.data
        columnNames=self.columnNames
        totalSize=self.totalSize
        relationships=self.relationships
        size=len(data)
        bitmap={}
        for i,column in enumerate(columnNames):
            relationships[column]={}
            bitmap[column]={} #this is a dictionary which is a set of flags per data value remembering if the increment already happened.
            for element in data:
                item=element[i]
                rlist=sparqlQuerypy.findBottomUp(item.strip())
                print 'number of nodes for', item.strip(), " is ", len(rlist)
                bitmap[column][item]={}
                for r in rlist:
                    if r[0] not in bitmap[column][item].keys():
                        bitmap[column][item][r[0]]=0
                    if r[0] not in relationships[column].keys():
                        relationships[column][r[0]]={}
                    relationships[column][r[0]]['name']='cc'
                    if 'incoming' not in relationships[column][r[0]].keys():
                        relationships[column][r[0]]['incoming']=1
                        relationships[column][r[0]]['cc']=1.0/totalSize
                    else:
                        relationships[column][r[0]]['incoming']+=1 
                        relationships[column][r[0]]['cc']=relationships[column][r[0]]['incoming']*1.0/totalSize
                        bitmap[column][item][r[0]]=1
        classSet=set() # A set to save all the possible cc classes for ease of retrieval later and to streamline it.


        for column in columnNames: #Loop to push the relations and nodes to Neo4j
            for classes in relationships[column].keys():
                classSet.add(classes)
                rel_data=Neo4jDrive.insertNodeAndRelationship(column,'cc',classes)[0]
                rel_data.properties['rel_class']='cc'
                rel_data.properties['fk']=relationships[column][classes]['cc'] 
                rel_data.push()
         
        for classes in classSet: #Loop to update the CCS score for each class after the previous loop is over. CCS=sum(fk)/no(fk) for the node.
            print classes
            cummulative=0 # The accumulator
            linkNumbers=0 # The denominator
            for link in Neo4jDrive.findIncomingCCLinks(classes): #loop to find incoming cc edges.
                cummulative+=link[0].properties['fk']
                linkNumbers+=1
            node=Neo4jDrive.findNodeByName(classes)
            node.properties['ccs']=cummulative*1.0/linkNumbers
            node.properties['type']='cc'
            node.push()

#if there no cc then send to descriminator? and blacklist it.
#-------------------DMS SCORE FUNCTION-----------------#

    def dmsScore(self):
        data=self.data
        columnNames=self.columnNames
        totalSize=self.totalSize
        relationships=self.relationships
        size=len(data)
        cache=[]
        bitmap={}
        
        for i,column1 in enumerate(columnNames):
            if column1 not in relationships.keys():
                relationships[column1]={}
            if column1 not in bitmap.keys():
                bitmap[column1]={}
            for j,column2 in enumerate(columnNames):
                if column2 not in relationships.keys():
                    relationships[column2]={}
                if i==j: continue
                for element in data:
                    print '--------------------'
                    print element[i],'-->',element[j]
                    item=(element[i],element[j])
                    rlist=sparqlQuerypy.findProperty2(element[i].strip(),element[j].strip())
                    cache=[]
                    for r in rlist:
                        
                        if column2 not in relationships[column1].keys():
                            relationships[column1][column2]={}
                        if column2 not in bitmap[column1].keys():
                            bitmap[column1][column2]={}
                        if item not in bitmap[column1][column2]:
                            bitmap[column1][column2][item]={}
                        bitmap[column1][column2][item][r['p']['value']]=0
                        if r['p']['value'] not in relationships[column1][column2].keys():
                            relationships[column1][column2][r['p']['value']]={}
                        if u'd' in r.keys():
                            print 'u d is in r.keys()'
                            relationships[column1][column2][r['p']['value']]['name']='property'
                            if 'count' not in relationships[column1][column2][r['p']['value']].keys():
                                relationships[column1][column2][r['p']['value']]['count']=1.0
                            if bitmap[column1][column2][item][r['p']['value']]==0:
                                relationships[column1][column2][r['p']['value']]['count']+=1
                                bitmap[column1][column2][item][r['p']['value']]=1
                            print relationships[column1][column2][r['p']['value']]['count']
                            relationships[column1][column2][r['p']['value']]['dms']=relationships[column1][column2][r['p']['value']]['count']/totalSize
                            if r['p']['value'] not in relationships[column2].keys():
                                relationships[column2][r['p']['value']]={}
                            relationships[column2][r['p']['value']]['name']='cp'
                            if r['p']['value'] not in relationships.keys():
                                relationships[r['p']['value']]={}
                            if r['d']['value'] not in relationships[r['p']['value']].keys():
                                relationships[r['p']['value']][r['d']['value']]={'name':'domain'}   
                            #-----------------TODO: add to hypothesis-------------#      

                        else:
                            ccClasses=Neo4jDrive.findCCNodes(column2)
                            
                            buildString="("
                            for ii in ccClasses:
                                buildString+='<'+ii+'>,'
                            buildString=buildString[:-1]
                            buildString+=")"

                            if r['p']['value'] not in cache:
                                propertyUsage=sparqlQuerypy.findPropertyClassesSecond(r['p']['value'],buildString)
                                cache+=[r['p']['value']]
                                #bitmap[column1][column2][item][r['p']['value']]=0
                                for domain in (set([k['r']['value'] for k in propertyUsage]) & set(ccClasses)):

                                   relationships[column1][column2][r['p']['value']]['name']='property'
                                   if 'count' not in relationships[column1][column2][r['p']['value']].keys():
                                       relationships[column1][column2][r['p']['value']]['count']=1.0
                                   print "item and r['p']['value'], is", item,r['p']['value']
                                   if bitmap[column1][column2][item][r['p']['value']]==0:
                                       relationships[column1][column2][r['p']['value']]['count']+=1
                                       bitmap[column1][column2][item][r['p']['value']]=1
                                   print relationships[column1][column2][r['p']['value']]['count']
                                   relationships[column1][column2][r['p']['value']]['dms']=relationships[column1][column2][r['p']['value']]['count']/totalSize*1.0
                                   if r['p']['value'] not in relationships[column2].keys():
                                       relationships[column2][r['p']['value']]={}
                                   relationships[column2][r['p']['value']]['name']='cp'
                                   if r['p']['value'] not in relationships.keys():
                                       relationships[r['p']['value']]={}
                                   if item not in relationships[r['p']['value']].keys():
                                       relationships[r['p']['value']][domain]={'name':'domain'}
                bitmap[column1][column2]=None
                         #-------------------------add to Hypothesis----------------------#

                     #-----------------Uploading to Neo4j----------------------------#
        for i,column1 in enumerate(columnNames):
            for j,column2 in enumerate(columnNames):
                if column1==column2: continue
                if column2 not in relationships[column1].keys(): continue 
                for rel in relationships[column1][column2].keys():
                    rel_data=Neo4jDrive.insertNodeAndRelationship(column1,rel,column2)[0]
                    
                    rel_data.properties['type']='property'
                    rel_data.properties['name']=rel
                    if 'dms' in relationships[column1][column2][rel].keys():
                        rel_data.properties['dms']=relationships[column1][column2][rel]['dms']
                    else:
                        rel_data.properties['dms']=0
                    rel_data.push()
                    rel_data=Neo4jDrive.insertNodeAndRelationship(column2,'cp',rel)[0]
                    rel_data.properties['type']='cp'
                    rel_data.push()

                    for domain in relationships[rel].keys():
                       rel_data=Neo4jDrive.insertNodeAndRelationship(rel,'domain',domain)[0]
                       rel_data.properties['type']='domain'
                       rel_data.push()
  

#TODO: the query is already returning property, type of subject, domain. Then instead of doing all the fancy check in whole CC classes. We just take the type of subject and put that straight as domain classes.


#------------------------LMS Score Function-------------------#

    def lmsScore(self):
        relationships=self.relationships 
        totalSize=self.totalSize
        ccClasses=set(Neo4jDrive.findAllCCNodes())
        hypothesis=self.hypothesis
        for column in enumerate(columnNames):
            rlist=sparqlQuerypy.findPropertyClassesThird(column)
            relationships[column]['lms']={}
            ccClassesOfColumn=set(Neo4jDrive.findCCNodes(column))
            for r in rlist:
                rangeList=sparqlQuerypy.findRange(r['s']['value'])
                if len(rangeList)==0: #does not have range
                    objTypeList=set([sparqlQuerypy.findTypeOfObject(r['t']['value']))
                    if len(objTypeList & ccClassesofColumn)==0:
                        continue #discard property if range(types of objects) don't exist in ccClasses.
                
                if (set(rangeList) & ccClassesofColumn)==0:
                    continue #discard property if range(got through Sparql) doesn't exist in ccClasses.
                domainList=sparqlQuerypy.findDomain(r['t']['value'])
                if len(domainList)==0: #does not have a Domain
                    domainList=[k['t']['value'] for k in sparqlQuerypy.findTypeOfSubject(r['s']['value']))]

                for domain in domainList:
                    if r['s']['value'] not in relationships[column]['lms'].keys():
                        relationships[column]['lms'][r['s']['value']]={}
                    
                    if domain in hypothesis:
                        if domain not in relationships[column]['lms'][r['s']['value']].keys():
                            relationships[column]['lms'][r['s']['value']]['d']= {'name':domain}      
                    else:
                        if domain in ccClasses:
                            hypothesis.add(domain)
                            relationships[column]['lms'][r['s']['value']]['d']= {'name':domain}


"""
                    relationships[column]['lms'][r['p']['value']]={}
                if u'r' not in r.keys():
                    buildString="("
                    for i in ccClasses:
                        buildString+='<'+i+'>,'
                    buildString=buildString[:-1]
                    buildString+=")"    
                    propertyUsage=sparqlQuerypy.findPropertyClassesSecond(r['p']['value'],buildString)
                    for item in (set([k['d']['value'] for k in propertyUsage]) & hypothesisSet):
                        relationships[column]['lms'][r['p']['value']]={'name':'dcs'}
                        hypothesis.add(r['p']['value'])#add
                        pr=r['p']['value']
                        for j in range(len(pr)-1,0,-1):
                            if pr[j]=='/':
                                pr=pr[j+1:]
                                break
                        relationships[column]['lms'][r['p']['value']]['score']=self.levenshtein(column,pr)
                    #addProperty
                    #addDItem
                        pass

                    for item in (set([k['d']['value'] for k in propertyUsage]) & set(self.allCC)):
                        pass

   
"""   
    def levenshtein(self,s1, s2):
        if len(s1) < len(s2):
            return levenshtein(s2, s1)

    # len(s1) >= len(s2)
        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1 # j+1 instead of j since previous_row and current_row are one character longer
                deletions = current_row[j] + 1       # than s2
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
    
        return previous_row[-1]



    
if __name__=='__main__':
    main()

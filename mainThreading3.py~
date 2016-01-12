import CSVRead
import sparqlQuerypy
import Neo4jDrive
import random
from threading import Thread, Lock
import datetime

log=open("log.log",'a')
log.write("\n--------------------------------------\n")
log.write(str(datetime.datetime.now()))
log.write("\n")

hypothesisSet=set()
stype=[]
sample=5
data=[]
csvitems=[]
def main():
    csvitems=[]
    data=[]
    tables=["StatesandCapitals.csv","RiversandSourceState.csv"]
    size=[]

    for nameOfFile in tables:
        Neo4jDrive.insertNode(nameOfFile)
        node=Neo4jDrive.findNodeByName(nameOfFile)
        node.properties['type']='table'
        node.push()
        csvitems+=[CSVRead.readCSV(nameOfFile,firstRow=False, choice=[0,1])[1:]]
        size+=[len(csvitems[-1])]
        random.shuffle(csvitems[-1])
    i=k=0          
    while len(csvitems)>0:
        
        for l,item in enumerate(csvitems):
            
            end=k+sample
            s=sample
            if k+sample>len(item):
                s=sample-(end-len(item))
                end=len(item)
            data[i:i+s]=[[it,l] for it in item[k:end]]
            i+=s
            if k+sample>len(item):
               csvitems.remove(item)
        k+=sample
    run(data,tables,size)


def run(data,tables,size):
    support=[[]]
    columnNames=[]
    for i,nameOfFile in enumerate(tables):
        columnNames+=[CSVRead.readCSV(nameOfFile,firstRow=True, choice=[0,1])]
        columnNames[i]=[c.strip() for c in columnNames[i]]
        for j,name in enumerate(columnNames[i]):
            z=Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)[0]
            node=Neo4jDrive.findNodeByName(name)
            node.properties['type']='Column'
            node.push()
            z.properties['type']="Column"
            z.push()
            support[i]+=[CSVRead.getSupport(nameOfFile,j)]
        support+=[[]]
    support=support[:-1]
   
    totalNumberOfValues=CSVRead.getSize(nameOfFile,0)
   
    
    hyplock=Lock()
    stypelock=Lock()
    
    for itemPiece in data:
        indexOfFile=itemPiece[1]
        item=itemPiece[0]
        for column in range(len(columnNames[indexOfFile])):
        #support=CSVRead.getSupport(nameOfFile,column)
        #totalNumberOfValues=CSVRead.numberOfItems(support)
        
            k=ccThread(item[column],columnNames[indexOfFile],column,support[indexOfFile],size[indexOfFile])
            k.start()
            k.join()
    for itemPiece in data:
        indexOfFile=itemPiece[1]
        item=itemPiece[0]
        for column in range(len(columnNames[indexOfFile])):
           #support=CSVRead.getSupport(nameOfFile,column)
           #totalNumberOfValues=CSVRead.numberOfItems(support)

            for perm_column in range(len(columnNames[indexOfFile])):
                if perm_column!=column:
                    k=dmsThread(item[column],item[perm_column],size[indexOfFile],columnNames[indexOfFile],column,perm_column)
                    k.start()
                    k.join()
        
        
    allCC=set(Neo4jDrive.findAllCCNodes())
    for s,c in enumerate(columnNames):
        for column in c:
            k=topDownThread(column,hyplock,stypelock,allCC,size[s])
            k.start()
            k.join()
       
                
    
class ccThread(Thread):
    def __init__(self,item,columnNames,column,support,totalNumberOfValues):
        Thread.__init__(self)
        self.item=item
        self.columnNames=columnNames
        self.column=column
        self.support=support
        self.totalNumberOfValues=totalNumberOfValues


    def run(self):
        support=self.support
        totalNumberOfValues=self.totalNumberOfValues*1.0

        column=self.column
        columnNames=self.columnNames
        item=self.item
        rlist=sparqlQuerypy.findBottomUp(item.strip())

        print 'number of nodes for', item.strip(), " is ", len(rlist)
        log.write('number of nodes for'+str( item.strip())+ " is "+ str(len(rlist))+'\n')
        flag=0
        for r in rlist:
            rel_data=Neo4jDrive.insertNodeAndRelationship(columnNames[column],"cc",r[2])       
            rel_data=rel_data[0]
            node=Neo4jDrive.findNodeByName(r[2])
            if r[2]=='http://dbpedia.org/ontology/PopulatedPlace':
                print columnNames[column], 'Happening'
            
                print 'potato',rel_data
            if rel_data.properties['incoming']==None: #find out why this is not happenings
                rel_data.properties['incoming']=1
                rel_data.properties['ccs']=1/totalNumberOfValues
                rel_data.push()
                #print 'tomato',rel_data
            else:
                if flag==0:
                    rel_data.properties['incoming']+=1
                    rel_data.push()
                    rel_data.properties['ccs']=node.properties['incoming']/totalNumberOfValues
                    flag=1
            node.properties['type']='cc'
            node.properties['ccs']=0
            numberOfLinks=0
            for link in Neo4jDrive.findIncomingCCLinks(r[2]):
                node.properties['ccs']+=link[0].properties['ccs']
                numberOfLinks+=1
            if numberOfLinks>0: node.properties['ccs']/=numberOfLinks
            node.push()
            
            
            
            rel_data.properties['rel_class'] = 'cc'
            #rel_data.properties['ccs']=node.proper/(totalNumberOfValues*1.0)
            rel_data.push()

class dmsThread(Thread):
    def __init__(self,label1,label2,size,columnNames,column,perm_column):
        Thread.__init__(self)
        self.label1=label1.strip()
        self.label2=label2.strip()
        self.size=size
        self.columnNames=columnNames
        self.column=column
	self.perm_column=perm_column

    def run(self):
        rlist=sparqlQuerypy.findProperty2(self.label1,self.label2)
        print '------------------'
        log.write('----------------\n')
        log.write(str(datetime.datetime.now())+'\n')
        log.write(self.label1+self.label2)
        print self.label1,self.label2#,rlist
        
        cache=[]
        propertyUsage=[1]
        for r in rlist:
            if u'd' in r.keys():
                self.addProperty(r['p']['value'])
                rel_data=Neo4jDrive.insertNodeAndRelationship(r['p']['value'],"domain",r['d']['value'])[0]
                rel_data['name']='domain'
                rel_data.push()
            else:
                ccClasses=Neo4jDrive.findCCNodes(self.columnNames[self.perm_column])
                buildString="("
                for i in ccClasses:
                    buildString+='<'+i+'>,'
                buildString=buildString[:-1]
                buildString+=")"
                if r['p']['value'] not in cache:
                    propertyUsage=sparqlQuerypy.findPropertyClassesSecond(r['p']['value'],buildString)
                    cache+=[r['p']['value']]
                
                    print len(propertyUsage),r['p']['value']
                    if len(propertyUsage)<15000:
                        for item in (set([k['r']['value'] for k in propertyUsage]) & set(ccClasses)):
                             self.addProperty(r['p']['value'])
                             rel_data=Neo4jDrive.insertNodeAndRelationship(r['p']['value'],"domain",item)[0]
                             rel_data['name']="domain"
                             rel_data.push()
                             node=Neo4jDrive.findNodeByName(item)
                             node.properties['hyp']='yes'
                             node.properties['type']='cc'
                             node.push()
                             self.incrementDms(rel_data) #for each table we have to put a score on the link between the what and what? The property and its domain? But then how is the score calculated? Is it number of columns in the table by total in that table or is it completely unique?

    
    def incrementDms(self,rel_data):
        if rel_data.properties['DCSinc']==None:
            rel_data.properties['DCSinc']=1
            rel_data.properties['DCS']=1.0/self.size
                
        else:
            rel_data.properties['DCSinc']+=1
            rel_data.properties['DCS']=node.properties['DCSinc']*1.0/self.size
        rel_data.push()
    




    def addProperty(self,p):
        rel_data=Neo4jDrive.insertNodeAndRelationship(self.columnNames[self.column],"property",p)
        hypothesisSet.add(p)
        node=Neo4jDrive.findNodeByName(p)
        if node.properties['dcsincoming']==None:
            node.properties['dcsincoming']=1
            node.properties['dcs']=1/(self.size*1.0)
        else:
            node.properties['dcsincoming']+=1
            node.properties['dcs']=node.properties['dcsincoming']/(self.size*1.0)
        node.properties['type']='property'
        node.push()
        rel=Neo4jDrive.insertRelationship(self.columnNames[self.column], p, self.columnNames[self.perm_column])[0]
        if rel.properties['propCount']==None:    
            rel.properties['type']='property_rel'
            rel.properties['name']=p
            rel.properties['count']=1
            rel.properties['dms']=rel.properties['count']/(self.size*1.0)
        else:
            rel.properties['count']+=1
            rel.properties['dms']=rel.properties['count']/(self.size*1.0)
        rel.push()

class topDownThread(Thread):
    def __init__(self, item1, hyplock, stypelock, allCC,size):
        Thread.__init__(self)
        self.a=item1.strip()
        self.hyplock=hyplock
        self.stypelock=stypelock
        self.allCC=allCC
        self.size=size
   
    def run(self):
        count=0
        objtypes=[]
        rlist=sparqlQuerypy.findPropertyClassesFirst(self.a)
        
        for r in rlist:
            if u'r' not in r.keys():
                ccClasses=Neo4jDrive.findCCNodes(self.a)
                buildString="("
                for i in ccClasses:
                    buildString+='<'+i+'>,'
                buildString=buildString[:-1]
                buildString+=")"
                propertyUsage=sparqlQuerypy.findPropertyClassesSecond(r['p']['value'],buildString)
                for item in (set([k['d']['value'] for k in propertyUsage]) & hypothesisSet):
                    #rel=Neo4jDrive.insertNodeAndRelationship(self.a ,'cp', r['p']['value'])
                    #self.hyplock.acquire()
                    #hypothesisSet.add(r['p']['value'])
                    #self.hyplock.release()
                    #temp=Neo4jDrive.findNodeByName(r['p']['value'])
                    #temp.properties['hyp']='yes'
                    #temp.push()
                    self.addProperty(r['p']['value'])
                    rel=Neo4jDrive.insertNodeAndRelationship(r['p']['value'], 'd', item)
                for item in (set([k['d']['value'] for k in propertyUsage]) & set(self.allCC)):
                    #rel=Neo4jDrive.insertNodeAndRelationship(self.a, 'cp', r['p']['value'])
                    #self.hyplock.acquire()
                    #hypothesisSet.add(r['p']['value'])
                    #self.hyplock.release()
                    #temp=Neo4jDrive.findNodeByName(r['p']['value'])
                    #temp.properties['hyp']='yes'
                    #temp.push()
                    self.addProperty(r['p']['value'])
                    rel=Neo4jDrive.insertNodeAndRelationship(r['p']['value'], 'd', item)
                        
                    

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

                

    def addProperty(self,p):
        print self.a, p
        rel_data=Neo4jDrive.insertNodeAndRelationship(self.a,"cp",p)[0]
        rel_data.properties['type']='cp'
        self.hyplock.acquire()
        hypothesisSet.add(p)
        self.hyplock.release()
        node=Neo4jDrive.findNodeByName(p)
        if rel_data.properties['incoming']==None:
            rel_data.properties['incoming']=1
            rel_data.properties['dms']=1/(self.size*1.0)
            pr=p
            for j in range(len(pr)-1,0,-1):
                if pr[j]=='/':
                    pr=pr[j+1:]
                    break
            rel_data.properties['lms']=self.levenshtein(self.a,pr)
        else:
            rel_data.properties['incoming']+=1
            rel_data.properties['dms']=node.properties['incoming']/(self.size*1.0)
        rel_data.push()
        node.properties['type']='property'
        node.properties['hyp']='yes'
        node.push()


        

if __name__=='__main__':
    main()



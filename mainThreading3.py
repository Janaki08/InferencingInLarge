import CSVRead
import sparqlQuerypy
import Neo4jDrive
import random
from threading import Thread, Lock
nameOfFile="RiversandSourceState.csv"
hypothesisSet=[]
stype=[]

def main():
    cache=[]
    Neo4jDrive.insertNode(nameOfFile)
    columnNames=CSVRead.readCSV(nameOfFile,firstRow=True, choice=[0,1])
    columnNames=[c.strip() for c in columnNames]
    csvitems=CSVRead.readCSV(nameOfFile,firstRow=False, choice=[0,1])
    size=len(csvitems)
    indices=range(size)
    random.shuffle(indices)

    hyplock=Lock()
    stypelock=Lock()
    for name in columnNames:
        Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)
    
    #support=CSVRead.getSupport(nameOfFile,0)
    #totalNumberOfValues=CSVRead.numberOfItems(support)

    for column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
        support=CSVRead.getSupport(nameOfFile,column)
        totalNumberOfValues=CSVRead.numberOfItems(support)
     
        count=0
        for index in range(size):
            #if item[column] in cache:
                #continue
            #else:
                #cache+=[item[column]]
              #  print cache
            item=csvitems[indices[index]]
            #k=ccThread(item[column],columnNames,column,support,size)
            #k.start()
            #k.join()

    for column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
        support=CSVRead.getSupport(nameOfFile,column)
        totalNumberOfValues=CSVRead.numberOfItems(support)

        for index in range(size):
            item=csvitems[indices[index]]
            for perm_column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
                if perm_column!=column:
                    k=dmsThread(item[column],item[perm_column],size,columnNames,column,perm_column)
                    k.start()
                    k.join()
        #k=topDownThread(columnNames(column),hyplock,stypelock)
        #k.start()
        #k.join()
        
    
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

        print 'size is', len(rlist)
        for r in rlist:
            rel_data=Neo4jDrive.insertNodeAndRelationship(columnNames[column],"cc",r[2])       
            node=Neo4jDrive.findNodeByName(r[2])
            if node.properties['incoming']==None:
                node.properties['incoming']=1
                node.properties['ccs']=1/totalNumberOfValues
            else:
                node.properties['incoming']+=1
                node.properties['ccs']=node.properties['incoming']/totalNumberOfValues
            node.properties['type']='cc'
            node.push()
            
            
            rel_data=rel_data[0]
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
        print self.label1,self.label2#,rlist
        print '------------------'
        for r in rlist:
            if u'd' in r.keys():
                self.addProperty(r['p']['value'])
                rel_data=Neo4jDrive.insertNodeAndRelationship(self.columnNames[self.column],"property",r['d']['value'])[0]
                rel_data['name']='property'
                rel_data.push()
            else:
                ccClasses=Neo4jDrive.findCCNodes(self.columnNames[self.perm_column])
                buildString="("
                for i in ccClasses:
                    buildString+='<'+i+'>,'
                buildString=buildString[:-1]
                buildString+=")"
                propertyUsage=sparqlQuerypy.findPropertyClassesSecond(r['p']['value'],buildString)
                print len(propertyUsage)
                if len(propertyUsage)<1500:
                    for k in propertyUsage:
                        if k['r']['value'] in ccClasses:
                            self.addProperty(r['p']['value'])
                            rel_data=Neo4jDrive.insertNodeAndRelationship(self.columnNames[self.column],"property",k['d']['value'])[0]
                            rel_data['name']="domain"

                            rel_data.push()








    def addProperty(self,p):
        rel_data=Neo4jDrive.insertNodeAndRelationship(self.columnNames[self.column],"property",p)
        node=Neo4jDrive.findNodeByName(p)
        if node.properties['incoming']==None:
            node.properties['incoming']=1
            node.properties['dms']=1/(self.size*1.0)
        else:
            node.properties['incoming']+=1
            node.properties['dms']=node.properties['incoming']/(self.size*1.0)
        node.properties['type']='property'
        node.push()
        rel=Neo4jDrive.insertRelationship(self.columnNames[self.column], p, self.columnNames[self.perm_column])[0]
            
        rel.properties['type']='property_rel'
        rel.properties['name']=p
        rel.push()

class topDownThread(Thread):
    def __init__(self, item1, hyplock, stypelock):
        Thread.__init__(self)
        self.a=item1
        self.hyplock=hyplock
        self.stypelock=stypelock
   
    def run(self):
        objtypes=[]
        rlist=sparqlQuerypy.findPropertyClassesFirst(self.a)
        for r in rlist:
            flag=False
            if r[4]==None:
                objtypes=[k[4] for k in sparqlQuerypy.findPropertyClassesSecond(self.a)]
            objtypes+=r[4]
            allCC= Neo4jDrive.findCC(self.a)   
            for i in objtypes: 
                if i in allCC:        
                    flag=True
                    break
            if flag:
                if r[3]!=None:
                    #self.hyplock.acquire()
                    hypothesisSet+=[r[3]]
                    #self.hyplock.release()
                else:
                    rlist1=sparqlQuerypy.findPropertyClassesSecond(self.a)
                    for rl in rlist1:
                        node1=Neo4jDrive.findNodeByName(rl[3])
                        if node1 != None:
                            #self.stype.acquire()
                            stype+=[[r[1],r[3]]]
                            #self.stype.release()
            else:
                rlist1=sparqlQuerypy.findPropertyClassesSecond(name1)        
                



        

if __name__=='__main__':
    main()
    #rel_data=Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column","Home")
    #Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)



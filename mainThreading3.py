import CSVRead
import sparqlQuerypy
import Neo4jDrive
import random
from threading import Thread
nameOfFile="agmarkrice2001modified.csv"
hypothesisSet=[]
stype=[]
def main():
    Neo4jDrive.insertNode(nameOfFile)
    columnNames=CSVRead.readCSV(nameOfFile,firstRow=True, choice=[0,1,2,3,4])
    csvitems=CSVRead.readCSV(nameOfFile,firstRow=False, choice=[0,1,2,3,4])
    size=len(csvitems)
    indices=[0:range(size)-1]
    random.shuffle(indices)
    for name in columnNames:
        Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)
    
    #support=CSVRead.getSupport(nameOfFile,0)
    #totalNumberOfValues=CSVRead.numberOfItems(support)

    for column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
        support=CSVRead.getSupport(nameOfFile,column)
        totalNumberOfValues=CSVRead.numberOfItems(support)
     
         
        for item in csvitems:
            k=ccThread(item[column],columnNames,column,support,size)
            k.start()
            k.join()
            for perm_column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
                if perm_column!=column:
                    k=dmsThread(item[column],perm_column,size)
                    k.start()
                    k.join()
        k=topDownThread(columnNames(column))
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
        totalNumberOfValues=self.totalNumberOfValues

        column=self.column
        columnNames=self.columnNames
        item=self.item
        rlist=sparqlQuerypy.findBottomUp(item)
        for r in rlist:
          
            rel_data=Neo4jDrive.insertNodeAndRelationship(columnNames[column],"cc",r[2])       
            node=Neo4jDrive.findNodeByName(r[2])
            if node.properties['incoming']==None:
                node.properties['incoming']=1
                node.properties['ccs']=0
            else:
                node.properties['incoming']+=1
                node.properties['ccs']=node.properties['incoming']
            node.properties['type']='cc'
            node.push()
            
            
            rel_data=rel_data[0]
            rel_data.properties['rel_class'] = 'cc'
            #rel_data.properties['ccs']=node.proper/(totalNumberOfValues*1.0)
            rel_data.push()

class dmsThread(Thread):
    def __init__(self,label1,label2,size):
        self.label1=label1
        self.label2=label2
        self.size=size


    def run(self):
        rlist=sparqlQuerypy.findProperty(self.label1,self.label2)
        for r in rlist:
            rel_data=Neo4jDrive.insertNodeAndRelationship(columnNames[column],"property",r[1])
            if node.properties['incoming']==None:
                node.properties['incoming']=1
                node.properties['dms']=0
            else:
                node.properties['incoming']+=1
                node.properties['dms']=node.properties['incoming']/this.size*1.0
            node.properties['type']='property'
            node.push()
            rel=Neo4jDrive.insertRelationship(label1,r[1],label2)
            rel.properties['type']='property_rel'
            rel.push()

class topDownThread(Thread):
    def __init__(self, item1,column):
        self.a=item1
        self.column
   
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
                    self.lock.acquire()
                    hypothesisSet+=[r[3]]
                    self.lock.release()
                else:
                    rlist1=sparqlQuerypy.findPropertyClassesSecond(self.a)
                    for rl in rlist1:
                        node1=Neo4jDrive.findNodeByName(rl[3])
                        if node1 != None:
                            self.lock.acquire()
                            stype+=[[r[1],r[3]]]
                            self.lock.release()
            else:
                rlist1=sparqlQuerypy.findPropertyClassesSecond(name1)        
                



        

if __name__=='__main__':
    main()
    #rel_data=Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column","Home")
    #Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)



import CSVRead
import sparqlQuerypy
import Neo4jDrive
from threading import Thread
nameOfFile="agmarkrice2001modified.csv"
def main():
    Neo4jDrive.insertNode(nameOfFile)
    columnNames=CSVRead.readCSV(nameOfFile,firstRow=True, choice=[0,1,2,3,4])
    for name in columnNames:
        Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)
    
    #support=CSVRead.getSupport(nameOfFile,0)
    #totalNumberOfValues=CSVRead.numberOfItems(support)
    for column in range(sum([1 for _ in Neo4jDrive.findRelationshipsOfNode(nameOfFile,"Column")])):
        support=CSVRead.getSupport(nameOfFile,column)
        totalNumberOfValues=CSVRead.numberOfItems(support)
        
        #print i.end_node
        #cNode=Neo4jDrive.findNodeByName(columnNames[column])
         
        for item in support.keys():
            k=itemThread(item,columnNames,column,support,totalNumberOfValues)
            k.start()
            k.join()
            
class itemThread(Thread):
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
            else:
                node.properties['incoming']+=1
            node.properties['type']='type'
            node.push()
            
            
            rel_data=rel_data[0]
            rel_data.properties['rel_class'] = 'cc'
            rel_data.properties['support']=support[item]/(totalNumberOfValues*1.0)
            rel_data.push()
        

if __name__=='__main__':
    main()
    #rel_data=Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column","Home")
    #Neo4jDrive.insertNodeAndRelationship(nameOfFile,"Column",name)



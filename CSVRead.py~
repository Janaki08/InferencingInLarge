import csv
result=[]

def readCSV(nameOfFile,firstColumn=0,lastColumn=-9999, choice=[], firstRow=False):
    result1=[]
    with open(nameOfFile, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            if len(choice)==0:
                if lastColumn<0:
                    result1.append(row)
                else:
                    result1.append(row[firstColumn:lastColumn])
            else:
                r=[]
                for i in choice:
                    r.append(row[i])
                result1.append(r)
        if firstRow:
            csvfile.close()
            result=result1
            return result1[0]
        else:
            csvfile.close()
            result=result1
            return result1

def getSupport(nameOfFile,column):
    result=readCSV(nameOfFile,choice=[column])
    resultCount={}
    for r in result:
      
        if r[0] not in resultCount.keys():
            resultCount[r[0]]=1
        else:
            resultCount[r[0]]+=1
    del resultCount[result[0][0]]
    return resultCount

def numberOfItems(dictionary):
    return sum(dictionary.values())
    

if __name__=='__main__':
    nameOfFile="RiversandSourceState.csv"
    print readCSV(nameOfFile,firstRow=False, choice=[0,1])

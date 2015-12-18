import csv
result=[]

def readCSV(nameOfFile,firstColumn=0,lastColumn=-9999, choice=[], firstRow=False):
    with open(nameOfFile, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            if len(choice)==0:
                if lastColumn<0:
                    result.append(row)
                else:
                    result.append(row[firstColumn:lastColumn])
            else:
                r=[]
                for i in choice:
                    r.append(row[i])
                result.append(r)
        if firstRow:
            return result[0]
        else:
            return result

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

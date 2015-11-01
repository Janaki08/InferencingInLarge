import csv
def csvWrite(row):
    number=5
    with open('../csv/eggs%s.csv'%number,'wb') as csvfile:
        writer=csv.writer(csvfile, delimiter=',',quotechar='|')
        writer.writerow(row)

if __name__=='__main__':
    row=range(5,11)
    csvWrite(row)

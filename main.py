from urllib.request import urlopen
from bs4 import BeautifulSoup
import json

request=urlopen("https://nwis.waterdata.usgs.gov/nwis/uv?cb_all_=on&cb_00010=on&cb_00045=on&cb_00045=on&cb_00060=on&cb_00065=on&cb_00095=on&cb_00300=on&cb_00400=on&cb_72255=on&format=html&site_no=02397530&period=&begin_date=2018-06-10&end_date=2018-07-11")
text=request.read()
bsObj=BeautifulSoup(text,'lxml')
table=bsObj.find('table',{'class':'dataListWithSuperscript'})
headers=table.findAll('th')

#The dict will act as our final JSON output
finalDict={"entries":[]}

##creating an array to store table headers for later use as JSON keys
keys=[]
for header in headers:
    try:
        keys.append(header.get_text().strip())
    except:
        pass
    
##reading the table on website and writing to JSON
tableRows=table.findAll('tr')[1:] ##removed the first row as it contains only headers
for tableRow in tableRows:
    dataCells=tableRow.findAll('td') ##finding all cells in a row
    for iterator in range(len(keys)):
        finalDict["entries"].append({})
        finalDict["entries"][-1][keys[iterator]]=dataCells[iterator].get_text().replace("A\xa0","").replace("\xa0","") ##storing the data into dict and removing uninterpreted signs from the readings
        
file=open("storage_file.json","w+")
file.write(json.dumps(json.loads(str(finalDict).replace("'","\"")))) ##storing JSON after ensuring the format is in correct JSON syntax!
file.close()

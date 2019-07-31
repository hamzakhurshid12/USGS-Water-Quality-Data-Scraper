from urllib.request import urlopen
from bs4 import BeautifulSoup
import json
import datetime

jumpDays=120 #to query data of this much days in one request [0-120]

now=datetime.datetime.now()
end=now-datetime.timedelta(days=1)
start=now-datetime.timedelta(days=jumpDays)

#The dict will act as our final JSON output
finalDict={"entries":[]}

for x in range(1): #an almost infinite loop for get data as long as we can get.
    start_date=start.strftime("%Y-%m-%d")
    end_date=end.strftime("%Y-%m-%d")
    site_url="https://nwis.waterdata.usgs.gov/nwis/uv?cb_all_=on&cb_00010=on&cb_00045=on&cb_00045=on&cb_00060=on&cb_00065=on&cb_00095=on&cb_00300=on&cb_00400=on&cb_72255=on&format=html&site_no=02397530&period=&begin_date="+start_date+"&end_date="+end_date
    request=urlopen(site_url)
    text=request.read()
    bsObj=BeautifulSoup(text,'lxml')
    try:
        table=bsObj.find('table',{'class':'dataListWithSuperscript'})
    except: #when we reach to a point where there is no further past data available
        print("Data finished..!")
        break
    headers=table.findAll('th')
    
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
        finalDict["entries"].append({})
        dataCells=tableRow.findAll('td') ##finding all cells in a row
        for iterator in range(len(keys)):
            finalDict["entries"][-1][keys[iterator]]=dataCells[iterator].get_text().replace("A\xa0","").replace("P\xa0","").replace("\xa0","") ##storing the data into dict and removing uninterpreted signs from the readings
    
    #changing dates for next iteration
    end=start-datetime.timedelta(days=1)
    start=end-datetime.timedelta(days=jumpDays)
    print(start_date+" to "+end_date)

            
file=open("storage_file.json","w+")
file.write(json.dumps(json.loads(str(finalDict).replace("'","\"")))) ##storing JSON after ensuring the format is in correct JSON syntax!
file.close()

from urllib.request import urlopen
from bs4 import BeautifulSoup
import json
import datetime
from multiprocessing.pool import ThreadPool
import os


def getRecords(ID,start,end):
    if idExists(ID):
        print("Already Exists: "+ID,end=',')
        return


    print("Starting: "+ID)    
    
    while True:
        #The dict will act as our final JSON output
        finalDict={"entries":[]}
    
        start_date=start.strftime("%Y-%m-%d")
        end_date=end.strftime("%Y-%m-%d")
        site_url="https://nwis.waterdata.usgs.gov/nwis/uv?cb_00010=on&cb_00045=on&cb_00060=on&cb_00065=on&cb_00095=on&cb_00300=on&cb_00400=on&cb_63680=on&cb_72255=on&format=html&site_no="+ID+"&period=&begin_date="+start_date+"&end_date="+end_date
        request=urlopen(site_url)
        text=request.read()
        bsObj=BeautifulSoup(text,'lxml')
        try:
            table=bsObj.find('table',{'class':'dataListWithSuperscript'})
            headers=table.findAll('th')
        except: #when we reach to a point where there is no further past data available
            print("ID Done: "+ID)
            break
        
        #creating an array to store table headers for later use as JSON keys
        keys=[]
        for header in headers:
            try:
                keys.append(header.get_text().strip())
                
                #replacing longer key names with shorter ones
                if str(keys[-1]).startswith("Date"):
                    keys[-1]="datetime"
                elif keys[-1].startswith("Dis-charge"):
                    keys[-1]="discharge"
                elif keys[-1].startswith("Gageheight"):
                    keys[-1]="gageHeight"
                elif keys[-1].startswith("Mean water"):
                    keys[-1]="meanWaterVelocity"
                elif keys[-1].startswith("Temper-"):
                    keys[-1]="temperature"
                elif keys[-1].startswith("Specif"):
                    keys[-1]="conducitivity"
                elif keys[-1].startswith("Dis-"):
                    keys[-1]="dissolvedOxygen" 
                elif keys[-1].startswith("Precip"):
                    keys[-1]="precipitation"
                elif keys[-1].startswith("pH"):
                    keys[-1]="pH"
                elif keys[-1].startswith("Turbid"):
                    keys[-1]="turbidity"
            except:
                pass
            
        #reading the table on website and writing to JSON
        tableRows=table.findAll('tr')[1:] ##removed the first row as it contains only headers
        for tableRow in tableRows:
            finalDict["entries"].append({})
            dataCells=tableRow.findAll('td') ##finding all cells in a row
            for iterator in range(len(keys)):
                finalDict["entries"][-1][keys[iterator]]=dataCells[iterator].get_text().replace("A\xa0","").replace("P\xa0","").replace("\xa0","") ##storing the data into dict and removing uninterpreted signs from the readings
        
        #changing dates for next iteration
        end=start-datetime.timedelta(days=1)
        start=end-datetime.timedelta(days=jumpDays)
        #print(end_date+" to "+start_date)
        
        #creating a temp file
        tempFile=open('temp/'+ID+".tmp","a+")
        tempFile.write(json.dumps(json.loads(str(finalDict).replace("'","\"")))+'-'*10) ##appending to temp file with a seperator '-'
        tempFile.close()
        
    #converting temp file chunks to one data file
    tempFile=open('temp/'+ID+".tmp","r+")
    tempText=tempFile.read()
    tempFile.close()
    os.remove('temp/'+ID+".tmp") #We don't need temp file after this
    
    chunks=tempText.split('-'*10) #splitting chunks in the temp file
    chunks.pop(-1) #removing last chunk as it will always be empty
    tempFinalDict={"entries":[]}
    for chunk in chunks: ##adding up all chunks to make one
        chunk=json.loads(chunk)
        tempFinalDict["entries"]+=chunk["entries"]
    
    #writing the final data to JSON file
    file=open(ID+".json","w+")
    file.write(json.dumps(json.loads(str(tempFinalDict).replace("'","\"")))) ##storing JSON after ensuring the format is in correct JSON syntax!
    file.close()

def idExists(ID): ##for checking if an ID has been already scrapped
    files=os.listdir()
    for x in range(len(files)):
        files[x]=files[x].replace(".json","")
    return ID in files

jumpDays=120 #to query data of this much days in one request [0-120]
numOfThreads=20 #number of threads to run at a time for multiprocessing


#creating a temp folder for storing temporary data
if not os.path.exists("temp"):
    os.makedirs("temp")
else: #emptying temp if it already exists
    filelist = [ f for f in os.listdir("temp")]
    for f in filelist:
        os.remove(os.path.join("temp", f))


##getting list of IDs of monitoring stations from main webpage of USGS
mainObj=BeautifulSoup(urlopen("https://nwis.waterdata.usgs.gov/nwis/current/?type=quality").read(),"lxml")
records=mainObj.findAll('table')[1].findAll('tr',{'valign':'top'})
all_ids=[]

##running loop for all ids for scraping complete set of all stations
for record in records:
    try:
        all_ids.append(record.find('a').get_text())
    except:
        pass


#for ID in all_ids:
threads=[]
pool = ThreadPool(processes=numOfThreads)

for ID in all_ids:
    now=datetime.datetime.now()
    end=now-datetime.timedelta(days=1)
    start=now-datetime.timedelta(days=jumpDays)
    #getRecords(ID,start,end)
    threads.append(pool.apply_async(getRecords, (ID,start,end,)))

pool.close()
pool.join()
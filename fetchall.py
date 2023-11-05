from logging import DEBUG, basicConfig
from datafetcher import retrieveData
from datetime import datetime, timezone
import json
import pytesseract
from zipfile import ZipFile, ZIP_DEFLATED
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
import os
from dotenv import load_dotenv
load_dotenv()

warnings.simplefilter(action='ignore', category=FutureWarning)

pytesseract.pytesseract.tesseract_cmd = os.environ['TESSERACT_EXE']

fetchersFile = os.environ['FETCHERS_FILE']
zipFileDirectory= os.environ['OUTPUT_ZIP_DIRECTORY']
resultsFileDirectory= os.environ['OUTPUT_RESULTS_DIRECTORY']

basicConfig(level=DEBUG, format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s")

class Job:
  def __init__(self, command):
    self.command = command
    self.ran = None
    self.success = None
    self.started = None
    self.ended = None
    

def fetchAllData():
   
    f = open(fetchersFile, "r", encoding="utf8")
    jobs = []

    for line in f:
        if line is not None and len(line) > 10:
            jobs.append(Job(line))

    batchProcess(jobs, 8)
    return ""

def runFetcher(zipFileLocation, job, startTime, lock):

    job.ran = 'true'
    job.started = datetime.now(timezone.utc)

    args = job.command.split(" ")
    zone = args[1].strip()
    dataType = args[2].strip()
    targetDateTime = None

    try:
        res = retrieveData(zone, dataType, targetDateTime)
        outputFileName = '' + zone + '_' + dataType + '_' + startTime.replace('+00:00','').replace(':','-') 
        
        if targetDateTime is not None:
            outputFileName = '' + outputFileName + '_' + targetDateTime.isoformat(timespec="seconds").replace('+00:00','').replace(':','-').replace('T', ' ')
        
        linesToSave = str(res)
        with lock:
            with ZipFile(zipFileLocation, 'a', compression=ZIP_DEFLATED) as myzip:
                myzip.writestr(outputFileName + ".txt", linesToSave)

        job.success = 'true'

    except Exception as e:
        print(f"No data retrieved for {zone} {dataType}") 
        print(e)
        job.success = 'false'
    
    job.ended = datetime.now(timezone.utc)

    return job   


def batchProcess(jobs, numThreads=8):   

    startTime = datetime.now(timezone.utc).isoformat(timespec="seconds")
    zipFileLocation = zipFileDirectory + "ElectricData_" + startTime.replace('+00:00','').replace(':','-') + ".zip" 

    with ZipFile(zipFileLocation, 'a', compression=ZIP_DEFLATED) as myzip:
        myzip.writestr("StartDate.txt", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    
    lock = Lock()
    results=[]
    with ThreadPoolExecutor(max_workers=numThreads) as executor:
        futures=[]
        for job in jobs:
            future = executor.submit(runFetcher, zipFileLocation, job, startTime, lock)
            futures.append(future)
        for f in futures:
            results.append(f.result())
    
    Path(resultsFileDirectory).mkdir(parents=True, exist_ok=True)
    outfilePath = resultsFileDirectory + 'Results_' + startTime.replace('+00:00','').replace(':','-').replace('T', ' ') + ".txt"

    with open(r''+outfilePath, 'w') as fp:
        fp.write("Command\tRan\tSuccess\tStarted\tEnded\tTime\n")
        for res in results:

            startedText = ""
            endedText = ""
            timeDiff = ""

            if not res.started is None:
                startedText = res.started.strftime("%Y/%m/%d %H:%M:%S")

            if not res.ended is None:
                endedText = res.ended.strftime("%Y/%m/%d %H:%M:%S")

            if not res.started is None and not res.ended is None:
                timeDiff = int((res.ended - res.started).total_seconds() * 1000)

            
            fp.write("" + res.command.strip() + "\t" + res.ran + "\t" + res.success + "\t" + startedText + "\t" + endedText + "\t" + str(timeDiff) + "\n")
            

if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    print(fetchAllData())

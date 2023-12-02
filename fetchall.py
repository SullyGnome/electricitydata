from logging import DEBUG, basicConfig
from datafetcher import retrieveData
from datetime import datetime, timezone, timedelta
import json
import pytesseract
from zipfile import ZipFile, ZIP_DEFLATED
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from pathlib import Path
from threading import Lock
import click

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

@click.command()
@click.argument("method")
def startFetching(method: str):
    print(method)
    if method == "bydate":
        print("Fetching by date")
        fetchDataForDates()
    if method == "current":
        print("Fetching current data")
        fetchAllData()

def fetchDataForDates():

    startDateTime = datetime.fromisoformat('2022-01-01 00:00')
    endDateTime = datetime.fromisoformat('2022-01-01 01:00')

    while startDateTime < endDateTime :
        fetchAllData(startDateTime.isoformat())
        startDateTime = startDateTime + timedelta(seconds=60*60)
    return     

def fetchAllData(targetTime: Optional[str]):
   
    f = open(fetchersFile, "r", encoding="utf8")
    jobs = []

    for line in f:
        if line is not None and len(line) > 10:
            jobs.append(Job(line))
        
    batchProcess(jobs, targetTime, 8)
    return ""

def runFetcher(zipFileLocation, job, startTime, targetTime, lock):

    job.ran = 'true'
    job.started = datetime.now(timezone.utc)

    args = job.command.split(" ")
    zone = args[1].strip()
    dataType = args[2].strip()

    try:
        res = retrieveData(zone, dataType, targetTime)
        outputFileName = '' + zone + '_' + dataType + '_' + startTime.isoformat(timespec="seconds").replace('+00:00','').replace(':','-').replace('T', ' ')
                
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

def batchProcess(jobs, targetTime, numThreads=8):   

    startTime = datetime.now(timezone.utc)

    if not targetTime is None:
        startTime = datetime.fromisoformat(targetTime)

    zipFullDirectory = zipFileDirectory + str(startTime.year) + "/" + str(startTime.month) + "/"
    Path(zipFullDirectory).mkdir(parents=True, exist_ok=True)
    zipFileLocation = zipFullDirectory + "ElectricData_" + startTime.isoformat(timespec="seconds").replace('+00:00','').replace(':','-') + ".zip" 

    with ZipFile(zipFileLocation, 'a', compression=ZIP_DEFLATED) as myzip:
        myzip.writestr("StartDate.txt", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    
    lock = Lock()
    results=[]
    with ThreadPoolExecutor(max_workers=numThreads) as executor:
        futures=[]
        for job in jobs:
            future = executor.submit(runFetcher, zipFileLocation, job, startTime, targetTime, lock)
            futures.append(future)
        for f in futures:
            results.append(f.result())
    
    Path(resultsFileDirectory).mkdir(parents=True, exist_ok=True)
    outfilePath = resultsFileDirectory + 'Results_' + startTime.isoformat(timespec="seconds").replace('+00:00','').replace(':','-').replace('T', ' ') + ".txt"

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
    print(startFetching())

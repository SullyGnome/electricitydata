from datetime import datetime, timezone
from logging import DEBUG, basicConfig, getLogger, ERROR
from typing import Any, Callable, Dict, List, Optional, Union
import os
from pathlib import Path
import json 
from electricitymap.contrib.lib.types import ZoneKey
from parsers.lib.parsers import PARSER_KEY_TO_DICT
from parsers.lib.quality import (
    ValidationError,
    validate_consumption,
    validate_exchange,
    validate_production,
)
from dotenv import load_dotenv
load_dotenv()

outputBaseDirectory = os.environ['OUTPUT_RAW_DIRECTORY']

logger = getLogger(__name__)
basicConfig(level=ERROR, format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s")

def retrieveData(zone: ZoneKey, data_type: str, target_datetime: Optional[str]):

    print(f"Retrieving {zone} {data_type}")

    begin_time = datetime.now(timezone.utc).isoformat(timespec="seconds")
    parsed_target_datetime = None
    if target_datetime is not None:
        parsed_target_datetime = datetime.fromisoformat(target_datetime)

    if not data_type:
        data_type = "exchange" if "->" in zone else "production"

    parser: Callable[
        ..., Union[List[Dict[str, Any]], Dict[str, Any]]
    ] = PARSER_KEY_TO_DICT[data_type][zone]
    

    if data_type in ["exchange", "exchangeForecast"]:
        args = zone.split("->")
    else:
        args = [zone]
    
    res = parser(
        *args, target_datetime=parsed_target_datetime, logger=getLogger(__name__)
    )

    if not res:
        raise ValueError(f"Error: parser returned nothing ({res})")

    if isinstance(res, (list, tuple)):
        res_list = list(res)
    else:
        res_list = [res]

    try:
        dts = [e["datetime"] for e in res_list]
    except:
        raise ValueError(
            f"Parser output lacks `datetime` key for at least some of the output. Full output: \n\n{res}\n"
        )

    assert all(
        [type(e["datetime"]) is datetime for e in res_list]
    ), "Datetimes must be returned as native datetime.datetime objects"

    assert (
        any(
            [
                e["datetime"].tzinfo is None
                or e["datetime"].tzinfo.utcoffset(e["datetime"]) is None
                for e in res_list
            ]
        )
        == False
    ), "Datetimes must be timezone aware"

    last_dt = datetime.fromisoformat(f"{max(dts)}").astimezone(timezone.utc)
    max_dt_warning = ""
    if not target_datetime:
        now_string = datetime.now(timezone.utc).isoformat(timespec="seconds")
        max_dt_warning = (
            f" :( >2h from now !!! (now={now_string} UTC)"
            if (datetime.now(timezone.utc) - last_dt).total_seconds() > 2 * 3600
            else f" -- OK, <2h from now :) (now={now_string} UTC)"
        )

    #pp = pprint.PrettyPrinter(width=120)
    #pp.pprint(res)

    outputFileName = '' + zone + '_' + data_type + '_' + begin_time.replace('+00:00','').replace(':','-') 
    
    if parsed_target_datetime is not None:
        outputFileName = '' + outputFileName + '_' + parsed_target_datetime.isoformat(timespec="seconds").replace('+00:00','').replace(':','-').replace('T', ' ')
    
    outputFileName = outputFileName + '.txt'
    outputDirectory = outputBaseDirectory + zone + "/" + data_type + "/"
    outputFileName = outputDirectory + outputFileName

    if isinstance(res, dict):
        res = [res]
    for event in res:
        try:
            if data_type == "production":
                validate_production(event, zone)
            elif data_type == "consumption":
                validate_consumption(event, zone)
            elif data_type == "exchange":
                validate_exchange(event, zone)
        except ValidationError as e:
            logger.warning(f"Validation failed @ {event['datetime']}: {e}")

    if os.environ['OUTPUT_RAW'] == 'true':
        Path(outputDirectory).mkdir(parents=True, exist_ok=True)
        if isinstance(res, dict):
            with open(r''+outputFileName, 'w') as fp:
                for item in res:
                    fp.write("%s\n" % item)
        else:
            with open(r''+outputFileName, 'w') as fp:
                fp.write(res)
                

    return res

if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    print(retrieveData())

from logging import DEBUG, basicConfig
import json
from parsers.lib.parsers import PARSER_KEY_TO_DICT

outputFile = "D:/Testing/electricitymaps-contrib-master/metadata.txt"
localesFile = "D:/Testing/electricitymaps-contrib-master/web/public/locales/en.json"

basicConfig(level=DEBUG, format="%(asctime)s %(levelname)-8s %(name)-30s %(message)s")

def getMetadata():
   
    f = open(localesFile, "r", encoding="utf8")
    loadedFile = f.read()
    localesData = json.loads(loadedFile)

    with open(r''+outputFile, 'w', encoding="utf8") as fp:
        for parserKey in PARSER_KEY_TO_DICT:
            for zoneKey in PARSER_KEY_TO_DICT[parserKey]:

                if zoneKey is not None:
                    zoneName = ''
                    try:
                        zone =  localesData['zoneShortName'].get(zoneKey)
                        if zone is not None:
                            zoneName = zone.get('zoneName')
                    except AttributeError:
                        zoneName = ''

                    try:
                        if parserKey is not None and zoneKey is not None and zoneName is not None:
                            #print(parserKey + '\t' + zoneKey + '\t' + zoneName)
                            outString = '\n' + parserKey + '\t' + zoneKey + '\t' + zoneName
                            fp.write(outString)
                    except AttributeError:    
                        a = "b"


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    print(getMetadata())

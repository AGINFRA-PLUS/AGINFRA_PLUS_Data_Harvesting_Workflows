#Harvest data from Openaire's api and store them in different directories based on data type
import json
from os import path
import requests
import math
sources=["http://api.openaire.eu/search/publications?format=json&keywords=agriculture&page=","http://api.openaire.eu/search/datasets?format=json&keywords=agriculture&page=", "http://api.openaire.eu/search/software?format=json&keywords=agriculture&page=","http://api.openaire.eu/search/projects?format=json&keywords=agriculture&page="]
for base_url in sources:
    response = requests.get(str(base_url))
    if base_url=="http://api.openaire.eu/search/publications?format=json&keywords=agriculture&page=":
        folder=str("publication")
    elif base_url=="http://api.openaire.eu/search/datasets?format=json&keywords=agriculture&page=":
        folder = str("datasets")
    elif base_url=="http://api.openaire.eu/search/software?format=json&keywords=agriculture&page=":
        folder = str("software")
    elif base_url == "http://api.openaire.eu/search/projects?format=json&keywords=agriculture&page=":
        folder = str("projects")
    else:
        folder = str("other")


    response = requests.get(base_url)
    print(base_url)
    data = response.text
    print(response.status_code)
    parsed = json.loads(data)
    total = parsed["response"]["header"]["total"]["$"]
    if total<=10:
        page_num=total
    else:
        page_num = math.ceil(total / 10)

    for page in range(1,page_num):

        url =  str(base_url) + str(page)
        response = requests.get(
            str(base_url) + str(page))
        print(response.status_code)
        if response.status_code == 409 :
            continue
        if response.status_code == 400 or response.status_code == 404:
            break
        # print(parsed['response']['results']['result'])
        print(url)
        try:
            data = response.json()['response']['results']['result']
        except:
            continue
        for idx, i in enumerate(data):
            id = i["header"]["dri:objIdentifier"]["$"].replace(':', '_')

            if path.exists(str(folder)+"/"+str(id) + ".json") is False:

            # print(idx)

                parsed1 = i["metadata"]
                print(parsed1)
                print(folder)
                open(str(folder)+'/' + str(id)+ ".json", 'w').write(json.dumps(parsed1))

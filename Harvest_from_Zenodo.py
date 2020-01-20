#Harvest data from Zenodo's api and store them in different directories based on data type
from os import path
import requests
import json
types = ["publication", "image", "software", "dataset", "presentation", "lesson"]
for type in types:
    url = 'https://zenodo.org/api/records?page=1&size=10000&q=Agriculture&type=' + str(type)
    response = requests.get(url)
    print(response.status_code)

    print(url)
    response = requests.get(url)
    print(response.status_code)

    try:
        data = response.text
        parsed = json.loads(data)
        parsed1 = parsed["hits"]["hits"]
    except:
        continue
    print(response.status_code)
    for idx, i in enumerate(parsed1):

        id = i["id"]
        # quit(0)
        print(type)
        if path.exists(str(type) + "/" + str(id) + ".json") is False:
            open(str(type) + '/' + str(id) + ".json", 'w').write(json.dumps(i))

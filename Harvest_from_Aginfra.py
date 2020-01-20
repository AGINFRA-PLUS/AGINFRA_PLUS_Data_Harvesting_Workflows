#Harvest data from Aginfra's ckan & gcat and store them in a 2 different directories

import json
from os import path
import requests
response = requests.get("https://ckan-aginfra.d4science.org/api/3/action/package_list")
data = response.text
parsed = json.loads(data)
headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
aginfra_data = []
aginfra_terms = []

for idx, i in enumerate(parsed['result']):
    if path.exists("stored_from_ckan/" + str(i) + ".json") is False:
        response = requests.get("https://ckan-aginfra.d4science.org/api/3/action/package_show?id=" + str(i))
        data = response.text
        parsed1 = json.loads(data)

        open('stored_from_ckan/' + str(i) + ".json", 'w').write(json.dumps(parsed1))

for k in range(0, 1000000000, 1):

    querystring = {"q": "", "offset": k}

    headers = {
        'Authorization': "",
        'User-Agent': "PostmanRuntime/7.18.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "",
        'Host': "gcat.d4science.org",
        'Accept-Encoding': "gzip, deflate",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }

    response = requests.get("https://gcat.d4science.org/gcat/items?", headers=headers, params=querystring)

    data = response.text

    if data == "HTTP 403 Forbidden":
        continue
    parsed = json.loads(data)
    print(parsed)
    if parsed == []:
        break

    for idx, i in enumerate(parsed):
        if path.exists("stored_from_gcan/" + str(i) + ".json") is False:
            print(i)
            print(idx)

            response = requests.get("https://gcat.d4science.org/gcat/items/" + str(i), headers=headers)
            data = response.text
            print("lathos")
            print(data)
            if data == "HTTP 403 Forbidden":
                continue

            print(data)

            parsed1 = json.loads(data)
            print(parsed1)


            open('stored_from_gcan/' + str(i) + ".json", 'w').write(json.dumps(parsed1))
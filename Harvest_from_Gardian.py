#Harvest data from GARDIAN's api and store them in different directories based on data type

import math
from os import path
import requests
import json
datasets=["publication", "dataset"]
for dataset in datasets:

    url = 'https://gardian.bigdata.cgiar.org/php_elastic/search_'+str(dataset)+'_advanced.php'
    headers = {'Accept': 'application/json, text/plain, */*', 'TE': 'Trailers',
               'Referer': 'https://gardian.bigdata.cgiar.org/search.php',
               'Content-Type': 'application/x-www-form-urlencoded', 'Origin': 'https://gardian.bigdata.cgiar.org'}
    data = requests.post(url,
                         data='keywords=&from=1&facet=1&country=none&soption=and&year=none&center=none&field=_all&type=none&sort=relevance&top_term=none&size=1000',
                         headers=headers)
    parsed = data.json()

    if str(dataset)=="publication":
        folder="publications"
    else:
        folder="datasets"
    parsed1 = parsed[str(folder)]["hits"]["hits"]
    publication_len = parsed[str(folder)]["hits"]["total"]
    # print(publication_len)
    pages = math.ceil(publication_len / 10)


    for page in range(1, pages):
        print(page)
        # from_page=page*1000
        response = requests.post(url,
                             data='keywords=&from='+str(page)+'&facet=1&country=none&soption=and&year=none&center=none&field=_all&type=none&sort=relevance&top_term=none&size=1000',
                             headers=headers)
        parse = response.json()[str(folder)]["hits"]["hits"]
        print(response.status_code)
        for idx, i in enumerate(parse):

            id = i["_id"].replace('-', '_')
    #
            if path.exists(str(dataset) + "/" + str(id) + ".json") is False:
                print(i["_source"])
                open(str(dataset) + '/' + str(id) + ".json", 'w').write(json.dumps(i["_source"]))

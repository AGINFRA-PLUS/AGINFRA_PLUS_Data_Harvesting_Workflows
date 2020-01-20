#Transform data to particular schema and send them to elastic index

import json
import math
import os
import requests


folders = ["publication", "dataset", "image", "lesson", "software", "presentation"]
for folder in folders:
    cwd = str(os.getcwd() + "/" + str(folder))

    dataArray = []
    for i in os.listdir(cwd):
        parsed1 = json.loads(open(str(cwd) + "/" + str(i), 'r').read())
        # print(parsed1)
        # quit(0)
        from datetime import date

        today = date.today()
        row = parsed1
        if "files" in row:
            url=row["files"][0]["links"]["self"]
        else:
            url=None
        if "publication_date" in row["metadata"]:
            if row["metadata"]["publication_date"] == None or row["metadata"]["publication_date"] == "":
                createdOn = today
            else:
                createdOn = str(row["metadata"]['publication_date'])
        else:
            createdOn = today

        if "description" in row["metadata"]:

            if row["metadata"]["description"] == None or row["metadata"]["description"] == "":
                description = None
            else:
                if isinstance(row["metadata"]["description"], list):
                    description = str(row["metadata"]['description'][0])
                else:
                    description = str(row["metadata"]['description'])
        else:
            description = None

        if "title" in row["metadata"]:
            if isinstance(row["metadata"]["title"], list):
                title = str(row["metadata"]['title'][0]).lower()
            else:
                title = str(row["metadata"]['title']).lower()
        else:
            title = None

        if "resource_type" in row["metadata"]:
            if isinstance(row["metadata"]["resource_type"], list):
                type = str(row["metadata"]['resource_type'][0]["title"])
            else:
                type = str(row["metadata"]['resource_type']["title"])
        else:
            try:
                type = str(row["metadata"]['resource_type']["type"])
            except:
                type = None

        if "language" in row["metadata"]:
            if isinstance(row["metadata"]["language"], list):
                language = str(row["metadata"]['language'][0]).lower()
            else:
                language = str(row["metadata"]['language']).lower()
        else:
            language = None

        if "creators" in row["metadata"]:
            try:
                creator = str(row["metadata"]['creators']["name"])
            except:
                creator = str(row["metadata"]['creators'][0]["name"])
        else:
            creator = None

        if "access_right" in row["metadata"]:

            license_title = str(row["metadata"]['access_right']).lower()
        else:
            license_title = None

        if "communities" in row["metadata"]:
            try:
                publisher = str(row["metadata"]['communities'][0]["id"])
            except:
                publisher = str(row["metadata"]['communities'])


        else:
            publisher = None

        updateon = str(today)


        if "keywords" in row["metadata"]:
            tags = row["metadata"]['keywords']
        else:
            tags = []
        zonodoData = {
            "createdOn": str(createdOn),
            "entityType": "",
            "dataSource": "ZENODO",
            "description": description,

            "id": "zenodo_" + str(row["id"]),
            "information": {

                'type': type.title(),

                'language': language,
                'author': creator,
                'url':url,
                'organization_name': publisher,
                'license_title': license_title

            },
            "published": True,

            "tags": tags,
            "title": title,
            "updatedOn": updateon
        }

        publication = ["publication", "Publication", "Project Milestone","journal article", "journal-article","scientific publication", "report / factsheet",
                       "book / monograph", "book section", "book chapter", "working paper", "conference paper",
                       "poster / presentation", "deliverable", "thesis", "presentation", "technical note",
                       "preprint", "book","project deliverable","report","book-chapter","Journal-Article", "Monograph","proposal","Books / Monographs", "Book-Chapter","taxonomic treatment", "software documentation", "Conference Paper", "Taxonomic Treatment", "Report", "Project Deliverable", "Proposal", "Book-Chapter", "Books / Monographs", "Thesis","Presentation", "Book","Technical Note", "Monograph","Preprint","Journal-Article", "Book Section", "Working Paper"]
        dataset = ["dataset","Dataset", "MaizeExperiment", "WheatExperiment"]
        model = ["FSKXModel", "OpenFSMR", "DataMiner Process", "Notebook"]
        research = ["Figure","Lesson", "diagram","Diagram", "drawing","Drawing", "digital material", "ResearchObject", "lesson",
                    "VirtualResearchEnvironment", "Webinar", "Document", "FoodOutbreakRecord","Research object"]
        semantic = ["RAKIPTerm", "DEMETER-Term", "GlossaryTerm"]
        services = ["External Services", "ExternalServices", "Software", "software"]
        if type.lower() in publication:
            zonodoData['entityType'] = 'Publications'
        elif type in dataset:
            zonodoData['entityType'] = 'Datasets'
        elif type in research:
            zonodoData['entityType'] = 'Research Objects'
        elif type in model:
            zonodoData['entityType'] = 'Models'
        elif type in semantic:
            zonodoData['entityType'] = 'Semantic Resources'
        elif type in services:
            zonodoData['entityType'] = 'Services'
        elif type in "Projects":
            zonodoData['entityType'] = 'Projects'
        else:
            zonodoData['entityType'] = 'Others'
        dataArray.append(zonodoData)


    SubDataToSend = 100
    numSubDataToSend = math.ceil(len(dataArray) / SubDataToSend)
    for count in range(numSubDataToSend):
        print(json.dumps(dataArray[count * SubDataToSend:(count + 1) * SubDataToSend]))
        url = "put smart-scheme import"
        headers = "headers"
        resp = requests.put(url,
                            data=json.dumps(dataArray[count * SubDataToSend:(count + 1) * SubDataToSend]),
                            headers=headers)
        print(resp.status_code)
        if resp.status_code == 400:
            quit(0)

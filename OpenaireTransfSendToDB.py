#Transform data to particular schema and send them to elastic index

import json
import math
import os

import requests

folders = ["publication", "datasets", "projects", "software"]
for folder in folders:

    cwd = str(os.getcwd() + "/" + str(folder))
    # print(folder)
    dataArray = []
    for i in os.listdir(cwd):
        parsed1 = json.loads(open(str(cwd) + "/" + str(i), 'r').read())
        # print(parsed1)
        # quit(0)
        if folder == "publication" or folder == "datasets" or folder == "software":
            from datetime import date

            today = date.today()
            if "@xsi:schemaLocation" in parsed1['oaf:entity']:
                url= str( parsed1['oaf:entity']['@xsi:schemaLocation'])
            else:
                url=None

            row = parsed1['oaf:entity']['oaf:result']

            if "dateofacceptance" in row:
                if row["dateofacceptance"] == None:
                    createdOn = today
                else:
                    createdOn = str(row['dateofacceptance']['$'])
            else:
                createdOn =today


            if "description" in row:

                if row["description"] == None:
                    description = None
                else:
                    if isinstance(row["description"], list):
                        description = str(row['description'][0]["$"])
                    else:
                        description = str(row['description']["$"])
            else:
                description = None

            if "title" in row:
                if isinstance(row["title"], list):
                    title = str(row['title'][0]["$"]).lower()
                else:
                    title = str(row['title']["$"]).lower()
            else:
                title = None

            if "resulttype" in row:
                if isinstance(row["resulttype"], list):
                    type = str(row['resulttype'][0]["@classid"]).lower()
                else:
                    type = str(row['resulttype']["@classid"]).lower()
            else:
                type = None

            if "subject" in row:
                if isinstance(row["subject"], list):
                    subject = str(row['subject'][0]["$"]).lower()
                else:
                    try:
                        subject = str(row['subject']["$"]).lower()
                    except:
                        subject = str(row['subject']["@schemename"]).lower()

            else:
                subject = None

            if "language" in row:
                if isinstance(row["language"], list):
                    language = str(row['language'][0]["@classname"]).lower()
                else:
                    language = str(row['language']["@classname"]).lower()
            else:
                language = None

            if "creator" in row:
                if isinstance(row["creator"], list):
                    creator = str(row['creator'][0]["$"]).lower()
                else:
                    creator = str(row['creator']["$"]).lower()
            else:
                creator = None

            if "publisher" in row:
                if row["publisher"] == None:
                    publisher = None
                else:
                    if isinstance(row["publisher"], list):
                        publisher = str(row['publisher'][0]["$"]).lower()
                    else:
                        publisher = str(row['publisher']["$"]).lower()
            else:
                publisher = None

            if "collectedfrom" in row:
                if row["collectedfrom"] == None:
                    publisher = None
                else:
                    if isinstance(row["collectedfrom"], list):
                        publisher = str(row["collectedfrom"][0]["@name"].lower())
                    else:
                        publisher = str(row['collectedfrom']["@name"]).lower()
            else:
                publisher = None




            if "bestaccessright" in row:
                if isinstance(row["bestaccessright"], list):
                    license_title = str(row['bestaccessright'][0]["@classname"]).lower()
                else:
                    license_title = str(row['bestaccessright']["@classname"]).lower()
            else:
                license_title = None

            if "country" in row:
                if isinstance(row["country"], list):
                    country = str(row['country'][0]["@classname"]).lower()
                else:
                    country = str(row['country']["@classname"]).lower()
            else:
                country = None


            if "programmingLanguage" in row:
                if isinstance(row["programmingLanguage"], list):
                    programmingLanguage = str(row['programmingLanguage'][0]["@classname"]).lower()
                else:
                    programmingLanguage = str(row['programmingLanguage']["@classname"]).lower()
            else:
                programmingLanguage = None

            # tags = [tag['display_name'].lower().strip() for tag in row['tags']]
            tags = []
            openairData = {
                "createdOn": str(createdOn),
                "dataSource": "openAIRE",
                "description": description,
                "entityType": "",
                "id": "openAIRE_" + str(i),
                "information": {

                    'type': type.title(),
                    'subject': subject,
                    'language': language,
                    'author': creator,
                    'organization_name': publisher,
                    'license_title': license_title,
                    'country': country,
                    "url":url,
                    'programmingLanguage': programmingLanguage

                },
                "published": True,
                "tags": tags,
                "title": title,
                "updatedOn": str(today)
            }

            publication = ["publication", "Publication", "journal article", "Journal Article", "scientific publication",
                           "report / factsheet",
                           "book / monograph", "book section", "book chapter", "working paper", "conference paper",
                           "poster / presentation", "deliverable", "thesis", "presentation", "technical note",
                           "preprint", "Journal-Article", "Monograph", "proposal", "Books / Monographs", "Book-Chapter",
                           "taxonomic treatment", "software documentation", "Conference Paper", "Taxonomic Treatment",
                           "Report", "Project Deliverable", "Proposal", "Book-Chapter", "Books / Monographs", "Thesis",
                           "Presentation", "Book", "Technical Note", "Monograph", "Preprint", "Journal-Article",
                           "Book Section", "Working Paper"]
            dataset = ["dataset", "Dataset", "MaizeExperiment", "WheatExperiment"]
            model = ["FSKXModel", "OpenFSMR", "DataMiner Process", "Notebook"]
            research = ["Figure", "Lesson", "diagram", "Diagram", "drawing", "Drawing", "digital material",
                        "ResearchObject", "lesson",
                        "VirtualResearchEnvironment", "Webinar", "Document", "FoodOutbreakRecord", "Research object"]
            semantic = ["RAKIPTerm", "DEMETER-Term", "GlossaryTerm"]
            services = ["External Services", "ExternalServices", "Software", "software"]
            if type.lower() in publication:
                openairData['entityType'] = 'Publications'
            elif type in dataset:
                openairData['entityType'] = 'Datasets'
            elif type in research:
                openairData['entityType'] = 'Research Objects'
            elif type in model:
                openairData['entityType'] = 'Models'
            elif type in semantic:
                openairData['entityType'] = 'Semantic Resources'
            elif type in services:
                openairData['entityType'] = 'Services'
            elif type in "Projects":
                openairData['entityType'] = 'Projects'
            else:
                openairData['entityType'] = 'Others'
            dataArray.append(openairData)

            print(openairData)





        elif folder == "projects":
            from datetime import date

            today = date.today()
            if "@xsi:schemaLocation" in parsed1['oaf:entity']:
                url = str( parsed1['oaf:entity']['@xsi:schemaLocation'])
            else:
                url = None

            row = parsed1['oaf:entity']['oaf:project']

            if "enddate" in row:
                if row["enddate"] == None:
                    createdOn = today
                else:
                    createdOn = str(row['enddate']['$'])
            else:
                createdOn =today


            if "contracttype" in row:

                if row["contracttype"] == None:
                    description = None
                else:
                    if isinstance(row["contracttype"], list):
                        description = str(row['contracttype'][0]["@classname"])
                    else:
                        description = str(row['contracttype']["@classname"])
            else:
                description = None

            if "title" in row:
                if isinstance(row["title"], list):
                    title = str(row['title'][0]["$"]).lower()
                else:
                    title = str(row['title']["$"]).lower()
            else:
                title = None


            type = "Projects"

            if "subjects" in row:
                if isinstance(row["subjects"], list):
                    subject = str(row['subjects'][0]["$"]).lower()
                else:
                    try:
                        subject = str(row['subjects']["$"]).lower()
                    except:
                        subject = str(row['subjects']["@schemename"]).lower()

            else:
                subject = None


            language = None

            if "contactfullname" in row:
                if row["contactfullname"] == None:
                    creator=None
                else:
                    if isinstance(row["contactfullname"], list):
                        creator = str(row['contactfullname'][0]["$"]).lower()
                    else:
                        creator = str(row['contactfullname']["$"]).lower()
            else:
                creator = None

            if "fundingtree" in row:
                if row["fundingtree"] == None:
                    publisher = None
                else:
                    if isinstance(row["fundingtree"], list):
                        publisher = str(row['fundingtree'][0]["funder"]["name"]["$"]).lower()
                    else:
                        publisher = str(row['fundingtree']["funder"]["name"]["$"]).lower()
            else:
                publisher = None

            if "collectedfrom" in row:
                if row["collectedfrom"] == None:
                    publisher = None
                else:
                    if isinstance(row["collectedfrom"], list):
                        publisher = str(row["collectedfrom"][0]["@name"].lower())
                    else:
                        publisher = str(row['collectedfrom']["@name"]).lower()
            else:
                publisher = None




            if "acronym" in row:
                if row["acronym"] == None:
                    acronym = None
                else:
                    if isinstance(row["acronym"], list):
                        acronym = str(row['acronym'][0]["$"]).lower()
                    else:
                        acronym = str(row['acronym']["$"]).lower()
            else:
                acronym = None





            # tags = [tag['display_name'].lower().strip() for tag in row['tags']]
            tags = [""]
            openairData = {
                "createdOn": str(createdOn),
                "dataSource": "openAIRE",
                "description": description,
                "entityType": "",
                "id": "openAIRE_" + str(i).replace('.json',''),
                "information": {

                    'type': type.title(),
                    'subject': subject,
                    'language': language,
                    'author': creator,
                    'organization_name': publisher,
                    'acronym': acronym,
                    "url":url

                },
                "published": True,
                "tags": tags,
                "title": title,
                "updatedOn": str(today)
            }

            publication = ["publication", "Publication", "journal article", "Journal Article", "scientific publication",
                           "report / factsheet",
                           "book / monograph", "book section", "book chapter", "working paper", "conference paper",
                           "poster / presentation", "deliverable", "thesis", "presentation", "technical note",
                           "preprint", "Journal-Article", "Monograph", "proposal", "Books / Monographs", "Book-Chapter",
                           "taxonomic treatment", "software documentation", "Conference Paper", "Taxonomic Treatment",
                           "Report", "Project Deliverable", "Proposal", "Book-Chapter", "Books / Monographs", "Thesis",
                           "Presentation", "Book", "Technical Note", "Monograph", "Preprint", "Journal-Article",
                           "Book Section", "Working Paper"]
            dataset = ["dataset", "Dataset", "MaizeExperiment", "WheatExperiment"]
            model = ["FSKXModel", "OpenFSMR", "DataMiner Process", "Notebook", "Software", "software"]
            research = ["Figure", "Lesson", "diagram", "Diagram", "drawing", "Drawing", "digital material",
                        "ResearchObject", "lesson",
                        "VirtualResearchEnvironment", "Webinar", "Document", "FoodOutbreakRecord", "Research object"]
            semantic = ["RAKIPTerm", "DEMETER-Term", "GlossaryTerm"]
            services = ["External Services", "ExternalServices"]
            if type.lower() in publication:
                openairData['entityType'] = 'Publication'
            elif type in dataset:
                openairData['entityType'] = 'Dataset'
            elif type in research:
                openairData['entityType'] = 'Research Objects'
            elif type in model:
                openairData['entityType'] = 'Models'
            elif type in semantic:
                openairData['entityType'] = 'Semantic Resources'
            elif type in services:
                openairData['entityType'] = 'Services'
            elif type in "Projects":
                openairData['entityType'] = 'Projects'
            else:
                openairData['entityType'] = 'Other'
            dataArray.append(openairData)
            # print(openairData)
    print(dataArray)
    SubDataToSend = 100
    numSubDataToSend = math.ceil(len(dataArray) / SubDataToSend)
    for count in range(numSubDataToSend):
        print(json.dumps(dataArray[count * SubDataToSend:(count + 1) * SubDataToSend]))
        url="put smart-scheme import"
        headers="headers"
        resp = requests.put(url,
                                data=json.dumps(dataArray[count * SubDataToSend:(count + 1) * SubDataToSend]), headers=headers)
        print(resp.status_code)




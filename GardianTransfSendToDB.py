#Transform data to particular schema and send them to elastic index


import json
import math
import os
import requests


folders = ["publication", "dataset"]
for folder in folders:

    cwd = str(os.getcwd() + "/" + str(folder))

    dataArray = []
    for i in os.listdir(cwd):
        row = json.loads(open(str(cwd) + "/" + str(i), 'r').read())

        if folder == "publication" or folder == "dataset":
            from datetime import date

            today = date.today()

            if "cg.date.production" in row:
                if row["cg.date.production"] == None or row["cg.date.production"] == "":
                    createdOn = today
                else:
                    createdOn = str(row['cg.date.production'])
            else:
                createdOn = today

            if "cg.description" in row:

                if row["cg.description"] == None or row["cg.description"] == "":
                    description = None
                else:
                    if isinstance(row["cg.description"], list):
                        description = str(row['cg.description'][0])
                    else:
                        description = str(row['cg.description'])
            else:
                description = None

            if "cg.title" in row:
                if row["cg.title"] == None or row["cg.title"] == "":
                    title = None
                else:
                    if isinstance(row["cg.title"], list):
                        title = str(row['cg.title'][0]).lower()
                    else:
                        title = str(row['cg.title']).lower()
            else:
                title = None

            if "cg.type" in row:
                if row["cg.type"] == None:
                    type = None
                else:
                    if isinstance(row["cg.type"], list):
                        type = str(row['cg.type'][0]).lower()
                    else:
                        type = str(row['cg.type']).lower()
            else:
                type = None

            if "cg.subject.topics" in row:
                if row["cg.subject.topics"] == None:
                    subject = None
                else:
                    subject = str(row['cg.subject.topics']).lower()
            else:
                subject = None

            if "cg.language" in row:
                if row["cg.language"] == None:
                    language = None
                else:
                    if isinstance(row["cg.language"], list):
                        language = str(row['cg.language'][0]).lower()
                    else:
                     language = str(row['cg.language']).lower()
            else:
                language = None

            if "cg.creator" in row:

                if isinstance(row["cg.creator"], list):
                    creator = str(row['cg.creator'][0]).lower()
                else:
                    creator = str(row['cg.creator']).lower()
            else:
                creator = None

            if "cg.contributor" in row:
                if row["cg.contributor"] == None:
                    publisher = None
                else:
                    if isinstance(row["cg.contributor"], list):
                        if len(row["cg.contributor"])==0:
                            publisher = None
                        else:
                            publisher = str(row['cg.contributor'][0]).lower()
                    else:
                        publisher = str(row['cg.contributor']).lower()
            else:
                publisher = None

            if "cg.rights" in row:
                if isinstance(row["cg.rights"], list):
                    if len(row["cg.rights"])==0:
                        license_title = None
                    else:
                        license_title = str(row['cg.rights'][0]).lower()
                else:
                    license_title = str(row['cg.rights']).lower()
            else:
                license_title = None

            if "cg.country" in row:
                if isinstance(row["cg.country"], list):
                    if len(row["cg.country"])==0:
                        country = None
                    else:
                        country = str(row['cg.country'][0]).lower()
                else:
                    country = str(row['cg.country']).lower()
            else:
                country = None

            if "publication_id" in row:
                if isinstance(row["publication_id"], list):
                    id = str(row['publication_id'][0]).lower()
                else:
                    id = str(row['publication_id']).lower()
            elif "dataset_id" in row:
                id = str(row['dataset_id']).lower()
            else:
                id = str(i).replace('.json',''),

            if "cg.identifier.citation" in row:
                if isinstance(row["cg.identifier.citation"], list):
                    citation = str(row['cg.identifier.citation'][0]).lower()
                else:
                    citation = str(row['cg.identifier.citation']).lower()
            else:
                citation = None
            if "cg.identifier.url" in row:
                if isinstance(row["cg.identifier.url"], list):
                    if len(row["cg.identifier.url"])==0:
                        url = None
                    else:
                        url = str(row['cg.identifier.url'][0]).lower()
                else:
                    url = str(row['cg.identifier.url']).lower()
            else:
                url = None

            # tags = [tag['display_name'].lower().strip() for tag in row['tags']]
            tags = row['cg.subject.agrovoc']
            gardianData = {
                "createdOn": str(createdOn),
                "dataSource": "GARDIAN",
                "description": description,
                "entityType": "",
                "id": "GARDIAN_" + str(id),
                "information": {

                    'type': type.title(),
                    'subject': subject,
                    'language': language,
                    'author': creator,
                    'organization_name': publisher,
                    'license_title': license_title,
                    'country': country,
                    'url': url,
                    'citation': citation

                },
                "published": True,
                "tags": tags,
                "title": title,
                "updatedOn": str(today)
            }

            publication = ["publication", "Publication", "journal article", "journal-article", "scientific publication",
                           "report / factsheet", "monograph", "book-chapter", "books / monographs","book / monograph",
                           "book / monograph", "book section", "book chapter", "working paper", "conference paper",
                           "poster / presentation", "deliverable", "thesis", "presentation", "technical note",
                           "preprint", "Journal-Article", "Monograph", "proposal", "Books / Monographs", "Book-Chapter",
                           "taxonomic treatment", "software documentation", "Conference Paper", "Taxonomic Treatment",
                           "report", "Project Deliverable", "Proposal", "Book-Chapter", "Books / Monographs", "Thesis",
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
                gardianData['entityType'] = 'Publications'
            elif type in dataset:
                gardianData['entityType'] = 'Datasets'
            elif type in research:
                gardianData['entityType'] = 'Research Objects'
            elif type in model:
                gardianData['entityType'] = 'Models'
            elif type in semantic:
                gardianData['entityType'] = 'Semantic Resources'
            elif type in services:
                gardianData['entityType'] = 'Services'
            elif type in "Projects":
                gardianData['entityType'] = 'Projects'
            else:
                gardianData['entityType'] = 'Others'
            dataArray.append(gardianData)

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
        if(resp.status_code==400):

            quit(0)

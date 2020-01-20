#Transform data to particular schema and send them to elastic index
import json
import os
import requests
folders = ["stored_from_ckan", "stored_from_gcan"]

for folder in folders:
    path = "path of folders/" + str(folder)

    for i in os.listdir(path):
        try:
            parsed1 = json.loads(open(str(folder) + '/' + str(i), 'r').read())
        except:
            continue
        print(parsed1)
        try:
            row = parsed1['result']
        except:
            row = parsed1
        if "metadata_created" in row:
            createdOn = str(row['metadata_created'])
        else:
            createdOn = None
        if "private" in row:
            if str(row['private']) == True:
                published = False
            else:
                published = True

        else:
            published = True

        if "notes" in row:
            description = str(row['notes'])
        else:
            description = None

        if "license_title" in row:
            license_title = str(row['license_title'])
        else:
            license_title = None

        if "author" in row:
            author = str(row['author'])
        else:
            author = None

        if "created" in row['organization']:
            organization_created = str(row['organization']['created'])
        else:
            organization_created = None
        if "name" in row['organization']:
            organization_name = str(row['organization']['name'])
        else:
            organization_name = None
        if "id" in row['organization']:
            organization_aginfra_id = str(row['organization']['id'])
        else:
            organization_aginfra_id = None
        if "title" in row['organization']:
            organization_title = str(row['organization']['title'])
        else:
            organization_aginfra_id = None
        if "title" in row:
            title = str(row['title'])
        else:
            title = None

        if "metadata_modified" in row:
            updatedOn = str(row['metadata_modified'])
        else:
            updatedOn = None

        tags = [tag['display_name'].lower().strip() for tag in row['tags']]

        aginfraData = {
            "createdOn": createdOn,
            "dataSource": "AGINFRA",
            "description": description,
            "entityType": "",
            "id": "AGINFRA_" + row['id'],
            "information": {
                'license_title': license_title,
                'author': author,
                'organization_created': organization_created,
                'organization_name': organization_name,
                'organization_title': organization_title,
                'type': "",
                'url': ""
            },
            "published": published,
            "tags": tags,
            "title": title,
            "updatedOn": updatedOn
        }
        if 'extras' in row:
            for k in row['extras']:
                if str(k['value']) and str(k["key"]) != "null":
                    if str(k['key']) == "system:type":
                        aginfraData["information"]['type'] = str(k['value'])
                        type = aginfraData["information"]['type']

                        publication = ["publication", "Publication", "journal article", "Journal Article",
                                       "scientific publication", "report / factsheet",
                                       "book / monograph", "book section", "book chapter", "working paper",
                                       "conference paper",
                                       "poster / presentation", "deliverable", "thesis", "presentation",
                                       "technical note",
                                       "preprint", "Journal-Article", "Monograph", "proposal", "Books / Monographs",
                                       "Book-Chapter", "taxonomic treatment", "software documentation",
                                       "Conference Paper", "Taxonomic Treatment", "Report", "Project Deliverable",
                                       "Proposal", "Book-Chapter", "Books / Monographs", "Thesis", "Presentation",
                                       "Book", "Technical Note", "Monograph", "Preprint", "Journal-Article",
                                       "Book Section", "Working Paper"]
                        dataset = ["dataset", "Dataset", "MaizeExperiment", "WheatExperiment"]
                        model = ["FSKXModel", "OpenFSMR", "DataMiner Process", "Notebook"]
                        research = ["Figure", "Lesson", "diagram", "Diagram", "drawing", "Drawing", "digital material",
                                    "ResearchObject", "lesson", "Method",
                                    "VirtualResearchEnvironment", "Webinar", "Document", "FoodOutbreakRecord",
                                    "Research object"]
                        semantic = ["RAKIPTerm", "DEMETER-Term", "GlossaryTerm"]
                        services = ["External Services", "ExternalService", "Software", "software"]
                        if type.lower() in publication:
                            aginfraData['entityType'] = 'Publications'
                            aginfraData["information"]['type'] = type.title()
                        elif type in dataset:
                            aginfraData['entityType'] = 'Datasets'
                            if type == "MaizeExperiment":
                                aginfraData["information"]['type'] = "Maize Experiment"
                            elif type == "WheatExperiment":
                                aginfraData["information"]['type'] = "Wheat Experiment"
                            else:
                                aginfraData["information"]['type'] = type.title()
                        elif type in research:
                            aginfraData['entityType'] = 'Research Objects'
                            if type == "VirtualReasearchEnvironment":
                                aginfraData["information"]['type'] = "Virtual Reasearch Environment"
                            elif type == "FoodOutbreaksRecord":
                                aginfraData["information"]['type'] = "Food Outbreaks Record"
                            elif type == "ReasearchObject":
                                aginfraData["information"]['type'] = "(Aginfra+)Reasearch Object"
                            else:
                                aginfraData["information"]['type'] = type.title()

                        elif type in model:
                            aginfraData['entityType'] = 'Models'
                            if type == "FSKXModel":
                                aginfraData["information"]['type'] = "FSKX Model"
                            elif str(k['value']) == "OpenFSMR":
                                aginfraData["information"]['type'] = "Open FSMR"
                            else:
                                aginfraData["information"]['type'] = type.title()

                        elif type in semantic:
                            aginfraData['entityType'] = 'Semantic Resources'
                            if type == "GlossaryTerm":
                                aginfraData["information"]['type'] = "(Aginfra+)Glossary Terms"
                            elif type == "RAKIPTerms":
                                aginfraData["information"]['type'] = "RAKIP Terms"
                            elif type == "DEMETER-Term":
                                aginfraData["information"]['type'] = "DEMETER Term"

                        elif type in services:
                            aginfraData['entityType'] = 'Services'
                        else:

                            aginfraData['entityType'] = 'Others'

                    elif str(k['key']) == "Item URL":
                        aginfraData["information"]['url'] = str(k['value'])
                    else:
                        key = str(k['key']).replace(':', '_')
                        aginfraData['information'][key] = str(k['value'])
        print(json.dumps([aginfraData]))
        url = "put smart-scheme import"
        headers = "headers"
        resp = requests.put(url,
                            data=json.dumps([aginfraData]), headers=headers)
        print(resp.status_code)

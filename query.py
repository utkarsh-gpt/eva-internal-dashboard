from database import db
import pandas as pd
from datetime import datetime
from utils import check_date_format

limit = 25
skip = 1
linkedin = db.linkedin
taglists = db.taglists
stakeholders = db.stakeholders
members = db.members
accounts = db.accounts
companyData = db.companyData
feedCollectionNormalization = db.feedCollectionNormalization
metaphorCompanyInsights = db.metaphorCompanyInsights



def troubleshootNews(stakeholderEmail: str, userEmail: str):

    pipeline = [
        {"$match": {"identity.email": stakeholderEmail }},
        {
            "$lookup": {
            "from": "linkedin",
            "localField": "identityLinkedin.company_profile",
            "foreignField": "linkedinId",
            "as": "linkedin_data"
            }
        },
        {"$unwind": "$linkedin_data"},
        {"$project": {"stakeholderCompanyName": "$linkedin_data.data.name","_id":0}},
        {'$skip': (skip - 1) * limit},
        {"$limit":limit}
    ]

    results = list(stakeholders.aggregate(pipeline))
    stakeholderCompanyName = results[0]['stakeholderCompanyName'].lower()
    #stakeholderCompanyName

    pipeline = [
        {"$match": {"identityContact.email": userEmail }},
        {
            "$lookup": {
            "from": "linkedin",
            "localField": "identityLinkedin.company_profile",
            "foreignField": "linkedinId",
            "as": "linkedin_data"
            }
        },
        {"$unwind": "$linkedin_data"},
        {"$project": {"userCompanyName": "$linkedin_data.data.name","_id":0}},
        {'$skip': (skip - 1) * limit},
        {"$limit":limit}
    ]

    results = list(members.aggregate(pipeline))
    userCompanyName = results[0]['userCompanyName'].lower()
    #userCompanyName

    #stakeholderCompanyName, userCompanyName

    factSignalBasedNews = metaphorCompanyInsights.find({"stakeholderCompanyName":stakeholderCompanyName,
                                                    "userCompanyName":userCompanyName,
                                                    "type":"userCompanySpecificFact"})

    genericNews = metaphorCompanyInsights.find({"stakeholderCompanyName":stakeholderCompanyName,
                                                    "userCompanyName":userCompanyName,
                                                    "type":"generic"})
    buyingSignalBasedNews = metaphorCompanyInsights.find({"stakeholderCompanyName":stakeholderCompanyName,
                                                    "userCompanyName":userCompanyName,
                                                    "type":"userCompanySpecific"})

    return {"genericNews":pd.DataFrame(genericNews),"buyingSignalBasedNews":pd.DataFrame(buyingSignalBasedNews),"factSignalBasedNews":pd.DataFrame(factSignalBasedNews)}

def troubleshootInsights(stakeholderEmail,userEmail):

    userData = members.find_one({"identityContact.email":userEmail},
                            {"userId":1,"organizationId":1,"identityLinkedin.company_profile":1,"_id":0})

    userId = userData['userId']
    organizationId = userData['organizationId']
    userCompanyLinkedinId = userData['identityLinkedin']['company_profile']

    stakeholderData = stakeholders.find_one({"identity.email":stakeholderEmail,"organizationId":organizationId},
                            {"stakeholderId":1,"identityLinkedin.company_profile":1,"_id":0})

    stakeholderCompanyLinkedinId = stakeholderData['identityLinkedin']['company_profile']


    insights  = feedCollectionNormalization.find({"stakeholderCompanyLinkedinId":stakeholderCompanyLinkedinId,
                                                  "opportunityOwnerCompanyLinkedinId":userCompanyLinkedinId,
                                                  "organizationId":organizationId,
                                                  "nudgeCategory":{"$in":["companyInsight","publicCompanyNews","privateCompanyNews"]}})

    return pd.DataFrame(insights)

def prospectLinkedinData(prospect_identifier: str):
 
    is_email = '@' in prospect_identifier
    is_linkedin_url = 'linkedin.com' in prospect_identifier

    if is_email:
        pipeline = [
            {"$match": {"identity.email":prospect_identifier}},
            {
                "$lookup": {
                    "from": "linkedin",
                    "localField": "identityLinkedin.person_profile",
                    "foreignField": "linkedinId",
                    "as": "person_data"
                }
            },
            {
                "$lookup": {
                    "from": "linkedin",
                    "localField": "identityLinkedin.company_profile",
                    "foreignField": "linkedinId",
                    "as": "company_data"
                }
            },
            {
                "$project": {
                    "prospect_personal_data": { "$arrayElemAt": ["$person_data.data", 0] },
                    "prospect_company_data": { "$arrayElemAt": ["$company_data.data", 0] },
                    "_id": 0
                }
            }
        ]

        result = list(stakeholders.aggregate(pipeline))
    
    elif is_linkedin_url:

        pipeline = [
            {"$match": {"url": prospect_identifier}},
            {
                "$lookup": {
                    "from": "stakeholders",
                    "localField": "linkedinId",
                    "foreignField": "identityLinkedin.person_profile",
                    "as": "person_data"
                }
            },
            {"$unwind": "$person_data"},
            {
                "$lookup": {
                    "from": "linkedin",
                    "localField": "person_data.identityLinkedin.company_profile",
                    "foreignField": "linkedinId",
                    "as": "company_data"
                }
            },
            {
                "$project": {
                    "prospect_personal_data": "$data",
                    "prospect_company_data": { "$arrayElemAt": ["$company_data.data", 0]},
                    "_id": 0
                }
            }
        ]
        
        result = list(linkedin.aggregate(pipeline))

    return {item:pd.DataFrame([result[0][item]]) for item in result[0]} if prospect_identifier else None
    

def sort_date(collection = "promptResponse", start_date = None,end_date = None):

    pipeline = [
        {"$match": {"createdAt": {"$exists": True}}},
        {"$addFields": {"createdAt": {"$dateFromString": {"dateString": "$createdAt"}}}}
    ]
    
    if start_date and end_date:
        if check_date_format(start_date) and check_date_format(end_date):
            start_date = datetime.fromisoformat(start_date)
            end_date = datetime.fromisoformat(end_date)
            
            pipeline.append({"$match": {"createdAt": {"$gte": start_date, "$lte": end_date}}})
        else:
            raise ValueError("Invalid date format. Please use 'YYYY-MM-DDTHH:MM:SS.ssssss' format.")

    # Query the database with aggregation
    documents = collection.aggregate(pipeline)

    return pd.DataFrame(documents)
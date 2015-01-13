import requests
from dateutil.parser import parse as dateParse
import json

from girder.api.rest import Resource
from girder.api.describe import Description

config = {
    'gridUrl': 'http://grid.ecohealth.io'
}


def getTerm(param, gridField):
    return {
        "fuzzy_like_this" : {
            "fields": [ gridField ],
            "like_text": param
        }
    }


class GRIDDatabase(Resource):
    def gridSearch(self, params):

        terms = []
        if params.get('disease'):
            terms.append(getTerm(params.get('disease'), 'diseaseVal'))
        if params.get('host'):
            terms.append(getTerm(params.get('host'), 'hostVal'))
        if params.get('country'):
            terms.append(getTerm(params.get('country'), 'locationNation'))
        if params.get('start'):
            dateTerm = {
                "range": {
                    "startDateISO": {
                        "gte": params.get('start')
                    }
                }
            }
            terms.append(dateTerm)
        if params.get('end'):
            dateTerm = {
                "range": {
                    "startDateISO": {
                        "lte": params.get('end')
                    }
                }
            }
            terms.append(dateTerm)

        eidTerm = [{
            "match_phrase": {
                "eidVal": 1
            }
        }]

        if params.get('minimum_should_match'):
            minimum_should_match = int(params.get('minimum_should_match'))
        else:
            minimum_should_match = len(terms)
        
        boolQuery = {
            "must": [eidTerm],
            "should": terms,
            "minimum_should_match": minimum_should_match
        }

        query = {
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ],
            "query": {
                "filtered": {
                    "query": {
                        "bool": boolQuery
                    }
                }
            }
        }
        
        options = {'size': 400, 'from': 0}
        params = {'query': query, 'options': options}
        request = requests.post(config['gridUrl'] + '/search', data=json.dumps(params), headers={'Content-type': 'application/json'})
        result = json.loads(request.text)

        for event in result.get('results'):
            event['link'] = config['gridUrl'] + '/event/' + event.get('id')

        return result

    gridSearch.description = (
        Description("Perform a query on the GRID historic event database.")
        .param(
            "disease",
            "The disease of the event",
            required=False
        )
        .param(
            "host",
            "The host of the event",
            required=False
        )
        .param(
            "country",
            "The country where the event occurred",
            required=False
        )
        .param(
            "start",
            "The start date of the query (inclusive)",
            required=False
        )
        .param(
            "end",
            "The end date of the query (inclusive)",
            required=False
        )
        .param(
            "minimum_should_match",
            "Include results that match at least this number of the parameters (default - results match all parameters)",
            required=False,
            dataType='int'
        )
        .errorResponse()
    )


def load(info):
    db = GRIDDatabase()
    info['apiRoot'].resource.route('GET', ('grid',), db.gridSearch)


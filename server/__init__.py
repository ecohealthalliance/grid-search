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

        mustTerms = [{
            "match_phrase": {
                "eidVal": 1
            }
        }]
        if params.get('disease'):
            mustTerms.append(getTerm(params.get('disease'), 'diseaseVal'))
        if params.get('host'):
            mustTerms.append(getTerm(params.get('host'), 'hostVal'))
        if params.get('country'):
            mustTerms.append(getTerm(params.get('country'), 'locationNation'))

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
                        "bool": {
                            "must": mustTerms
                        }
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
        .errorResponse()
    )


def load(info):
    db = GRIDDatabase()
    info['apiRoot'].resource.route('GET', ('grid',), db.gridSearch)


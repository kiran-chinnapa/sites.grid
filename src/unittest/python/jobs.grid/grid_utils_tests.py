import unittest
from grid_utils import global_search


class GridUtilsTest(unittest.TestCase):

    def test_grid_global_search(self):
        auth_id = 'ed0444ad-9e34-43b5-b0f5-5d08351a3def'
        grid_id = '5e8fe2cdc9d0821a1c9290e0/share/5e9b979ec9d0821a1c93f46e'
        query = '{ "query": { "selectColumnNames": [ "Company Website", "Company Name"], ' \
        '"pagination": { "startRow": 1, "rowCount": 56 },'\
                       '"sendRowIdsInResponse": true, "showColumnNamesInResponse": true}}'
        result = global_search(grid_id, auth_id, query,env='prod')

        # print (result)
        self.assertIsNotNone(result['rows'])

    def test_grid_global_search_count(self):
        auth_id = 'ed0444ad-9e34-43b5-b0f5-5d08351a3def'
        grid_id = '5e8fe2cdc9d0821a1c9290e0/share/5e9b979ec9d0821a1c93f46e'
        query = '''{
    "query": {
        "globalFilter": {
            "filters": [
                {
                    "operator": "LIKE",
                    "keyword": "[a-zA-Z0-9]"
                }
            ]
        }
    }
}'''
        result = global_search(grid_id, auth_id, query,'search_count',env='prod')

        # print (result)
        self.assertIsNotNone(result['totalRowCount'])






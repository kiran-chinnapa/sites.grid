from crawlers.utils import grid_utils

regex = r'engineer$|developer$|programmer$|architect$|engineering$|analyst$|coder$'

from_grid = '6200f79f94a0302330aa7e9a'
to_grid = '622734a594a0306ca6afb084'

search_query = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Job Title",
                    "operator": "LIKE",
                    "keyword": "engineer$|developer$|programmer$|architect$|engineering$|analyst$|coder$"
                }
            ]
        }, 
  	"showColumnNamesInResponse": true,
    "pagination": {
    "startRow": 1,
    "rowCount": 300
       }
    }
}'''

insertDataDict = {"insert": {"rows": []}}

search_result = grid_utils.global_search(from_grid,grid_utils.qa_auth_id,search_query)

for row in search_result['rows']:
    insertDataDict["insert"]["rows"].append(row)
    grid_utils.add_row(to_grid, grid_utils.qa_auth_id, insertDataDict)
    insertDataDict["insert"]["rows"].clear()

print('all rows moved between grids')


import json
import os
import time
import traceback
import requests
import logging

def global_search(grid_id, auth_id, search_query, search_type='search', env='qa'):
    if "1" == os.getenv("disable_grid"):
        return
    domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
    try:
        # json_str = json.dumps(json.loads(search_query))
        headers = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        resp = requests.post('https://{}/api/v2/grid/{}/{}'.format(domain, grid_id, search_type),
                                        headers=headers, data=search_query)
        if resp.status_code == 200:
            logging.info("Search results found")
            return json.loads(resp.text)
        else:
            print(resp.text)
            logging.error("Failed search:retrying after sleep: ")
            logging.info(search_query)
            time.sleep(5)
            global_search(grid_id, auth_id, search_query, "search")
    except:
        traceback.print_exc()



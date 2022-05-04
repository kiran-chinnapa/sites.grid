# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# useful for handling different item types with a single interface

import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from crawlers.utils import grid_utils


class JobsJsonPipeline:

    def open_spider(self, spider):
        self.file = open('job_items.jl', 'a')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(ItemAdapter(item).asdict()) + "\n"
        self.file.write(line)
        return item


class JobsGridPipeline:

    def __init__(self):
        self.insertDataDict = {"insert": {"rows": []}}

    # def close_spider(self, spider):
    #     self.insertDataDict["insert"]["rows"] = []

    def process_item(self, item, spider):
        self.insertDataDict["insert"]["rows"].append(ItemAdapter(item).asdict())
        grid_utils.add_row(grid_utils.qa_grid_id, grid_utils.qa_auth_id, self.insertDataDict, 0)
        return item


# removes duplicates
class CareerDupDetector:

    career_cnt_dict = json.loads(grid_utils.search_column_filter)

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()
        keyword = item_dict[spider.unique_column].replace("?","\\?")
        CareerDupDetector.career_cnt_dict['query']['columnFilter']['filters'][0]['column'] = spider.unique_column
        CareerDupDetector.career_cnt_dict['query']['columnFilter']['filters'][0]['keyword'] = keyword
        # totalRowCount = {}
        total_row_cnt = grid_utils.global_search(grid_id=spider.grid_id, auth_id=grid_utils.qa_auth_id,
                                                 search_query=json.dumps(CareerDupDetector.career_cnt_dict),
                                                 search_type='search_count')
        # totalRowCount['totalRowCount'] = 0
        if total_row_cnt['totalRowCount'] > 0:
            raise DropItem('Duplicate detected in grid: {}'.format(item_dict[spider.unique_column]))
        else:
            return item



# inserts the career page dict.
class CareerGridPipeline:

    def __init__(self):
        self.insertDataDict = {"insert": {"rows": []}}

    # def close_spider(self, spider):
    #     self.insertDataDict["insert"]["rows"] = []

    def process_item(self, item, spider):
        try:
            self.insertDataDict["insert"]["rows"] = []
            self.insertDataDict["insert"]["rows"].append(ItemAdapter(item).asdict())
            grid_utils.add_row(spider.grid_id, grid_utils.qa_auth_id, self.insertDataDict, 0)
            return item
        except Exception as e:
            spider.logger.error('error in CareerGridPipeline.process_item:{}'.format(e))
            return item
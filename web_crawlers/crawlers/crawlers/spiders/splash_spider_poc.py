# # https://www.youtube.com/watch?v=lKIXRAtromY 1:40 min — you are the man
# # https://github.com/scrapy-plugins/scrapy-splash
# import scrapy
# from scrapy_splash import SplashRequest
#
#
# class CiteSpider(scrapy.Spider):
#     name = "teksystems"
#     allowed_domains = ["teksystems.com"]
#     start_urls = [
#         'http://careers.teksystems.com'
#     ]
#
#     script = """
#         function main(splash)
#           local url = splash.args.url
#           assert(splash:go(url))
#           assert(splash:wait(0.5))
#           splash:runjs('document.querySelectorAll("a.gs_nph[aria-controls=gs_cit]")[0].click()')
#           splash:wait(3)
#           local href = splash:evaljs('document.querySelectorAll(".gs_citi")[0].href')
#           assert(splash:go(href))
#           return {
#             html = splash:html(),
#             png = splash:png(),
#             href=href,
#           }
#         end
#         """
#
#     def parse(self, response):
#         yield SplashRequest(self.start_urls[0], self.parse_click_results,
#                             endpoint="execute",
#                             args={"lua_source": self.script})
#
#     def parse_click_results(self, response):
#         filename = response.url.split("/")[-2] + '.html'
#         with open(filename, 'wb') as f:
#             f.write(response.css("body > pre::text").extract()[0])
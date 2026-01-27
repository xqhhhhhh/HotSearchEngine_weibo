import scrapy


class WeiboHotItem(scrapy.Item):
    keyword = scrapy.Field()
    rank_peak = scrapy.Field()
    hot_value = scrapy.Field()
    last_exists_time = scrapy.Field()
    durations = scrapy.Field()
    host_name = scrapy.Field()
    category = scrapy.Field()
    location = scrapy.Field()
    icon = scrapy.Field()
    trend_first_time = scrapy.Field()
    trend_last_time = scrapy.Field()
    trend_duration_days = scrapy.Field()

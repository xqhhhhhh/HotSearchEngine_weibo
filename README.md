# Weibo 热搜（微博平台-总榜）Scrapy 爬虫
https://weibo.zhaoyizhe.com/




## 功能
- 仅抓取「微博平台-总榜」（平台=微博，维度=总榜）
- 时间范围：2019-10-25 ～ 2025-12-31（可通过参数调整）
- 自动翻页，全量抓取首页基础数据
- 获取热搜走势：初次上榜时间、最后上榜时间、共计在榜天数————这步比较难，需要抓包，破解，计算得到这几个字段；主要依赖 “趋势 5 进程退避运行”
- 基础数据与热榜走势解耦，再依据keywords合并

## xqh
1.时间筛选时DATE_STEP_DAYS=1  需要这样设置，否则会有数据缺失
2.抓包破解的数据相对来说满，可以开多个进程，但需要控制每个进程的参数，防止被限速
3.sqlite可以进行缓存，服务于断点爬取
4.cookie放入env
5.这个网站在获取热搜走势时需要不断向网站发送抓包请求，连续较高频抓去一段时间会被拒绝ConnectionRefused；但重现再运行就又可以爬取；所以python scripts/run_trend_parallel_backoff.py --keywords output/keywords.txt --out output/trend.jsonl --shards 5
中设置了遇到超时或者被拒绝时，暂停一段时间再开始发送请求
6.为了防止中途错误后停留一段时间继续爬取有遗漏，最后再重新运行一下python scripts/run_trend_parallel_backoff.py --keywords output/keywords.txt --out output/trend.jsonl --shards 5

rm -rf jobdir
rm -rf output
rm trend_cache.sqlite


python scripts/extract_to_excel.py \
  -i output/weibo_total_20191025_20251231.jsonl \
  -o output/weibo_total_extract.xlsx
  
## 并行加速（5 路分片）
使用脚本按日期自动分片并行运行：
```
python scripts/run_parallel.py --shards 5
```
如需失败分片自动清理（方便下次重跑）：
```
python scripts/run_parallel.py --shards 5 --reset-failed
```
如需超时自动停爬并退避重试：
```
python scripts/run_parallel_backoff.py --shards 5
```
会生成：
- 输出：`output/part1.jsonl` ... `output/part5.jsonl`
- 断点：`jobdir_1` ... `jobdir_5`
- 走势缓存：`trend_cache_part1.sqlite` ...
失败分片会记录在：`output/failed_shards.txt`

合并输出：
```
cat output/part*.jsonl > output/weibo_total_20191025_20251231.jsonl
```

## 解耦爬取（列表 / 走势）
1) 先爬列表（只抓页面列表字段，直接运行即可）：
```
OUTPUT_JSONL=output/list.jsonl scrapy crawl weibo_list
```
2) 提取 keyword：
```
python scripts/keywords_from_list.py --list output/list.jsonl --out output/keywords.txt
```
3) 爬走势（只抓分钟级走势三字段，直接运行即可）：
```
OUTPUT_JSONL=output/trend.jsonl scrapy crawl weibo_trend -a keywords_file=output/keywords.txt
```
4) 按 keyword 合并：
```
python scripts/join_by_keyword.py --list output/list.jsonl --trend output/trend.jsonl --out output/joined.jsonl
```

## 单独趋势退避运行
```
python scripts/run_trend_backoff.py --keywords output/keywords.txt --out output/trend.jsonl
```
超时或连接拒绝会每次暂停 1 分钟后无限重试。

## 趋势 5 进程退避运行
```
python scripts/run_trend_parallel_backoff.py --keywords output/keywords.txt --out output/trend.jsonl --shards 5
```
会生成：
- `output/trend_part1.jsonl` ... `output/trend_part5.jsonl`
- `jobdir_trend_1` ... `jobdir_trend_5`
- `output/keywords_part1.txt` ...
合并：
```
cat output/trend_part*.jsonl > output/trend.jsonl
```

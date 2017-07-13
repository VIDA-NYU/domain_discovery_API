from os import environ

if environ.get('ACHE_FOCUSED_CRAWLER_SERVER'):
    ache_focused_crawler_server = environ['ACHE_FOCUSED_CRAWLER_SERVER']
else:
    ache_focused_crawler_server = 'localhost'

if environ.get('ACHE_FOCUSED_CRAWLER_PORT'):
    ache_focused_crawler_port = environ['ACHE_FOCUSED_CRAWLER_PORT']
else:
    ache_focused_crawler_port = "8080"

if environ.get('ACHE_DEEP_CRAWLER_SERVER'):
    ache_deep_crawler_server = environ['ACHE_DEEP_CRAWLER_SERVER']
else:
    ache_deep_crawler_server = 'localhost'

if environ.get('ACHE_DEEP_CRAWLER_PORT'):
    ache_deep_crawler_port = environ['ACHE_DEEP_CRAWLER_PORT']
else:
    ache_deep_crawler_port = "8080"
    

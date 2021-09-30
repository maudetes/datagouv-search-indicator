import csv
import requests
import tqdm

MIN_REFERRALS = 4


class MatomoQueryDatasetBuilder:

    base_param = {
        'module': 'API',
        'format': 'json',
        'token_auth': 'anonymous',
        'period': 'range',
        'date': 'previous30',
    }

    def __init__(self, stats_domain, dataset_domain, site_id, scheme, dataset_path):
        self.stats_domain = stats_domain
        self.dataset_domain = dataset_domain
        self.base_param['idSite'] = site_id
        self.scheme = scheme
        self.base_url = f'{self.scheme}://{self.stats_domain}/index.php'
        self.dataset_path = dataset_path

    def get_pages_following_site_searches(self):
        '''Get the most popular fr/datasets pages that follow a site search'''
        r = requests.get(self.base_url, params={**self.base_param,
                                                'method': 'Actions.getPageUrlsFollowingSiteSearch',
                                                'expanded': 1,
                                                'filter_limit': 10})

        r.raise_for_status()
        pages = r.json()
        pages = next(elem for elem in pages if elem['label'] == 'fr')['subtable']
        pages = next(elem for elem in pages if elem['label'] == 'datasets')['subtable']
        pages = [elem['label'] for elem in pages if elem['label'] not in ['/index', '/?q=', 'Others']]
        return pages

    def get_page_id(self, page):
        '''Get page id from the page name'''
        r = requests.get(f'{self.scheme}://{self.dataset_domain}/api/1/datasets/' + page + '/')
        r.raise_for_status()
        return r.json()['id']


    def get_page_searches(self, page):
        '''Get searches that led to the page with a number of referrals >= MIN_REFERRALS'''
        r = requests.get(self.base_url, params={**self.base_param,
                                                'method': 'Transitions.getTransitionsForPageUrl',
                                                'pageUrl': f'https://{self.dataset_domain}/fr/datasets/' + page + '/',
                                                'filter_limit': 50,
                                                'limitBeforeGrouping': 50,
                                                })
        r.raise_for_status()    
        queries = r.json()['previousSiteSearches'][:-1]
        queries = [elem['label'] for elem in queries if elem['referrals'] >= MIN_REFERRALS]
        return queries


    def save_query_dataset(self, page_query_dict, page_ids):
        '''Save the queries with their associated pages'''
        with open(f'data/{self.dataset_path}', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for page in page_query_dict:
                writer.writerows([(query, None, page_ids[page]) for query in page_query_dict[page]])


    def create_query_dataset(self):
        '''Create a test dataset with queries and their expected datasets based on matomo search logs'''
        pages = self.get_pages_following_site_searches()

        page_ids = {page: self.get_page_id(page) for page in pages}

        page_query_dict = {}
        for page in tqdm.tqdm(pages):
            queries = self.get_page_searches(page)
            page_query_dict[page] = queries

        self.save_query_dataset(page_query_dict, page_ids)

        count_queries = sum([len(page_query_dict[page]) for page in page_query_dict])
        return count_queries

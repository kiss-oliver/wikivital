import networkx as nx
import json
import requests as re
import pandas as pd
from bs4 import BeautifulSoup as bs
from collections import Counter
from tqdm import tqdm
from dataclasses import dataclass

@dataclass
class wikipage:
    urls:list
    url:str

    def get_content(self):
        self.content = re.get(self.url)

    def parse_content(self):
        self.soup = bs(self.content.content, "html.parser")

    def collect_links(self):
        self.links = Counter([x["href"] for x in self.soup.findAll("a") if x.has_attr("href") and "https://en.wikipedia.org{}".format(x["href"]) in self.urls])

    def collect_page_views(self):
        headers = re.utils.default_headers()
        headers.update({'User-Agent': 'WikiVital Network Data Collection Script- contact kiss_oliver (at) phd (dot) ceu (dot) edu',})
        self.pageviews = re.get("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{}/daily/2015100100/2021031600".format(self.url.split("/wiki/")[-1].replace("/","")), headers=headers).content

@dataclass
class wikipages:
    wikipages:list

    def __post_init__(self):
        self.urls = ["https://en.wikipedia.org{}".format(x["href"]) for x in self.wikipages]
        self.id_to_url = {i:self.urls[i] for i in range(len(self.urls))}
        self.url_to_id = {self.urls[i]:i for i in range(len(self.urls))}
        self.wikis = [wikipage(self.urls, url) for url in self.urls]

    def fetch(self):
        for wiki in tqdm(self.wikis, "Fetching content"):
            wiki.get_content()
            wiki.parse_content()
            wiki.collect_links()

@dataclass
class wikicollection:
    url:str

    def fetch_wikis(self):
        main = re.get(self.url)
        soup = bs(main.content, "html.parser")
        self.wikis = [x for x in soup.findAll("a") if x.has_attr("href") and not ":" in x["href"] and not "#" in x["href"] and x["href"].startswith("/wiki") and "Main_Page" not in x["href"]]
        self.content = wikipages(self.wikis)

    def collect_edges(self):
        if not hasattr(self, "content"):
            self.fetch_wikis()
            self.content.fetch()
        elif not hasattr(self.content.wikis[0],"links"):
            self.content.fetch()

        self.edges = []
        for wiki in self.content.wikis:
            self.edges = self.edges + [(self.content.url_to_id[wiki.url],self.content.url_to_id["https://en.wikipedia.org{}".format(target)] , {"weight": weight}) for target, weight in wiki.links.items()]

    def generate_network(self):
        if not hasattr(self, "edges"):
            self.collect_edges()

        self.network = nx.DiGraph(self.edges)

    def collect_attributes(self):
        if not hasattr(self, "content"):
            self.fetch_wikis()
        self.pageviews=[]
        for wiki in tqdm(self.content.wikis, "Collecting page views"):
            wiki.collect_page_views()
            views = json.loads(wiki.pageviews)
            for datapoint in views["items"]:
                self.pageviews.append({"id":self.content.url_to_id[wiki.url], "timestamp":datapoint["timestamp"],"views":datapoint["views"]})

    def write_network(self, out_path):
        if not hasattr(self, "network"):
            self.generate_network()
        nx.write_edgelist(self.network, out_path)

    def write_attributes(self, out_path):
        if not hasattr(self, "pageviews"):
            self.collect_attributes()
        open(out_path, "w").write(json.dumps(self.pageviews))

    def write_wikis(self, out_path):
        data = []
        for wiki in self.wikis:
            data.append({"url":"https://en.wikipedia.org{}".format(wiki["href"]),
            "title":wiki["title"],
            "id":self.content.url_to_id["https://en.wikipedia.org{}".format(wiki["href"])]})
        pd.DataFrame(data).to_csv(out_path, index=False)
        

for topic in ["Technology"]:
    collection = wikicollection("https://en.wikipedia.org/wiki/Wikipedia:Vital_articles/Level/5/{}".format(topic))
    collection.write_attributes("{}.json".format(topic))
    collection.write_network("{}.txt".format(topic))
    collection.write_wikis("{}.csv".format(topic))

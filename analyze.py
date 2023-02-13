import json
import pandas as pd
from ethnicolr import pred_wiki_name
import os


def clean_author_data(authors):
    authors = authors.split('\t')[1]
    authors = authors.replace('\"\"','"')
    authors = authors.replace('\t','')
    authors = authors.replace('\n','')
    authors = str(authors)
    authors = authors[1:-1]
    return authors


def process_data(data):
    name_list = []
    for datum in data:
        authors = json.loads(clean_author_data(datum))
        for author in authors:
            name_list.append({'first': author['given'], 'last': author['family']})
    return name_list



directory = os.fsencode("/mnt/c/Users/sean/Documents/author_tallies")

for file in os.listdir(directory):
    with open(file, encoding="utf8") as f:
        data = f.readlines()[1:]
    name_list_processed = process_data(data)

    author_df = pd.DataFrame(name_list)
    odf = pred_wiki_name(author_df,'last', 'first', conf_int=0.9)



print(odf)

end = time.time()

print(end-start)
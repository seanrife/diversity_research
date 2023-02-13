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

with open("test_data.tsv", encoding="utf8") as f:
    data = f.readlines()[1:]

name_list = []

for i, datum in enumerate(data):
    authors = json.loads(clean_author_data(datum))
    print('....' + str(i), end=' ')

    for author in authors:
        name_list.append({'first': author['given'], 'last': author['family']})

    #print(author_df)


author_df = pd.DataFrame(name_list)
odf = pred_wiki_name(author_df,'last', 'first', conf_int=0.9)

print(odf)

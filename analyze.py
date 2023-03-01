import json
import pandas as pd
from ethnicolr import pred_wiki_name
import os
import gender_guesser.detector as gender
import time
from multiprocessing import Pool
import random


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def clean_author_data(authors):
    authors = authors.split('\t')[1]
    authors = authors.replace('\"\"','"')
    authors = authors.replace('\t','')
    authors = authors.replace('\n','')
    authors = str(authors)
    authors = authors[1:-1]
    return authors


def process_data(data):
    out_list = []
    for datum in data:
        authors = clean_author_data(datum)
        if len(authors) > 2:
            authors = json.loads(authors)
            datum = datum.split('\t')
            for author in authors:
                out_list.append({'doi': datum[0], 'first': author['given'], 'last': author['family'], 'sup': datum[2], 'men': datum[3], 'con': datum[4], 'tot': datum[5][:-1]})
    return out_list


d = gender.Detector()

directory = "/mnt/c/Users/sean/Documents/author_tallies"


def process_multiple(files):
    for file in files:
        process(file)


def process(file):
    with open('output/' + file, 'a') as o:
        o.write('doi\tauth_number\teth_count\tsex_count\tsup\tmen\tcon\ttot\n')

    start = time.time()
    print("Processing " + file)
    with open("/mnt/c/Users/sean/Documents/author_tallies/" + file, encoding="utf8") as f:
        data = f.readlines()[1:]

    list_processed = process_data(data)

    author_df = pd.DataFrame.from_dict(list_processed)

    print("Getting names...")

    eth_df = pred_wiki_name(author_df,'last', 'first', conf_int=0.9)

    unique_dois = eth_df['doi'].unique()

    for unique_doi in unique_dois:
        rows = eth_df.loc[eth_df['doi'] == unique_doi]
        eth_sex_summary = []
        auth_number = 0
        eth_count = 0
        sex_count = 0
        eths = []
        sexes = []
        sup = rows['sup'].iloc[0]
        men = rows['men'].iloc[0]
        con = rows['con'].iloc[0]
        tot = rows['tot'].iloc[0]
        for i, row in rows.iterrows():
            auth_number = auth_number + 1
            if row['race'] not in eths:
                eth_count = eth_count + 1
                eths.append(row['race'])
            
            sex = d.get_gender(row['first']).replace('mostly_', '')

            if sex not in sexes and sex != 'unknown' and sex != 'andy':
                sex_count = sex_count + 1
                sexes.append(sex)

        eth_sex_summary.append({'doi': unique_doi[1:-1], 'auth_number': auth_number,
                                'eth_count': eth_count, 'sex_count': sex_count,
                                'sup': sup, 'men': men, 'con': con, 'tot': tot})
        
        write_record = ""
        for record in eth_sex_summary:
            write_record = (write_record + record['doi'] + '\t' + str(record['auth_number']) + '\t' +
                            str(record['eth_count']) + '\t' + str(record['sex_count']) + '\t' +
                            str(record['sup']) + '\t' + str(record['men']) + '\t' +
                            str(record['con']) + '\t' + str(record['tot']) + '\n')

        with open('output/' + file, 'a') as o:
            o.write(write_record)
        
    os.remove("/mnt/c/Users/sean/Documents/author_tallies/" + file)

    end = time.time()
    print(end-start)

dir_list = []

for file in os.listdir(directory):
    dir_list.append(file)

random.shuffle(dir_list)

process_multiple(dir_list)


"""
n = 5

x = list(divide_chunks(dir_list, n))

with Pool(n) as p:
    p.map(process_multiple, x)
    time.sleep(20)

"""
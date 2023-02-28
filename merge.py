import os


directory = "./output"

header = 'doi\tauth_number\teth_count\tsex_count\tsup\tmen\tcon\ttot\n'

with open("merged.tsv", "w") as m:
    m.write(header)

for file in os.listdir(directory):
    print(file)
    with open("./output/" + file) as f:
        data = f.readlines()
        with open("merged.tsv", "a") as m:
            for i, datum in enumerate(data):
                if i == 0:
                    continue
                m.write(datum)


import os
import pandas as pd

# move to ./exported_files dir
os.chdir("./exported_files")

filenames = ["Opportunities_for_srno_0{}.csv".format(i) for i in range(1, 3)]
print(filenames)

# combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in filenames])

# export to csv
combined_csv.to_csv("./combined_csv.csv", index=False, encoding='utf-8')

# open file after combining
os.system("code ./combined_csv.csv")

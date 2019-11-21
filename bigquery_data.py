import pandas as pd
import random

file = 'out/data_malteria.csv'
df = pd.read_csv(file)


def random_ages(x):
    if x["Edad"] == "18 a 24":
        return random.randint(1, 24)
    if x["Edad"] == "25 a 34":
        return random.randint(25, 34)
    if x["Edad"] == "> 35":
        return random.randint(35, 70)


df["Edad"] = df[["Edad"]].apply(random_ages, axis=1)
df.to_csv('out/data_bigquery.csv',index=False)

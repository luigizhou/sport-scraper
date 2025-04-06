import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import tarfile

def main():
    normalize_incidents("/Users/luigizhou/repos/sofascore/data/", "2024","basketball")
    normalize_statistics("/Users/luigizhou/repos/sofascore/data/", "2024","basketball")
    normalize_graphs("/Users/luigizhou/repos/sofascore/data/", "2024","basketball")
    normalize_votes("/Users/luigizhou/repos/sofascore/data/", "2024","basketball")


def normalize_votes(basepath: str, year: str, sport: str):
    """
    Go through all files and normalize in parquet
    """
    
    months = sorted(os.listdir(os.path.join(basepath, year)))
    for month in months:
        days = sorted(os.listdir(os.path.join(basepath, year, month)))
        for day in days:
            fullpath = os.path.join(basepath, year, month, day, sport)
            frames = []
            for file in sorted(os.listdir(fullpath)):
                if file.endswith("votes.json.tar.gz"):
                    filpath = os.path.join(fullpath, file)
                    print("file", filpath)
                    with tarfile.open(filpath, "r:gz") as tar:
                        for member in tar.getmembers():
                            f = tar.extractfile(member)
                            if f is not None:
                                json_obj = json.load(f)
                                if json_obj.get("error", None):
                                    continue
                                df = pd.json_normalize(json_obj)
                                frames.append(df)
            result = pd.concat(frames)
            result.to_parquet(os.path.join(fullpath, 'votes.parquet.gzip'), compression='gzip')

def normalize_incidents(basepath: str, year: str, sport: str):
    """
    Go through all files and normalize in parquet
    """
    
    months = sorted(os.listdir(os.path.join(basepath, year)))
    for month in months:
        days = sorted(os.listdir(os.path.join(basepath, year, month)))
        for day in days:
            fullpath = os.path.join(basepath, year, month, day, sport)
            frames = []
            for file in sorted(os.listdir(fullpath)):
                if file.endswith("incidents.json.tar.gz"):
                    filpath = os.path.join(fullpath, file)
                    print("file", filpath)
                    with tarfile.open(filpath, "r:gz") as tar:
                        for member in tar.getmembers():
                            f = tar.extractfile(member)
                            if f is not None:
                                json_obj = json.load(f)
                                if json_obj.get("error", None):
                                    continue
                                df = pd.DataFrame(json_obj["incidents"])
                                df['eventId'] = f.name.split('/')[-1].split('-')[0]
                                ndf = pd.json_normalize(df.to_dict(orient="records"))
                                ndf = ndf[ndf.columns.drop(list(ndf.filter(regex='.*fieldTranslations.*')))]
                                frames.append(ndf)
            result = pd.concat(frames)
            result.to_parquet(os.path.join(fullpath, 'incidents.parquet.gzip'), compression='gzip')


def normalize_statistics(basepath: str, year: str, sport: str):
    """
    Go through all files and normalize in parquet
    """
    months = sorted(os.listdir(os.path.join(basepath, year)))
    for month in months:
        days = sorted(os.listdir(os.path.join(basepath, year, month)))
        for day in days:
            fullpath = os.path.join(basepath, year, month, day, sport)
            frames = []
            for file in sorted(os.listdir(fullpath)):
                if file.endswith("statistics.json.tar.gz"):
                    filpath = os.path.join(fullpath, file)
                    print("file", filpath)
                    with tarfile.open(filpath, "r:gz") as tar:
                        for member in tar.getmembers():
                            f = tar.extractfile(member)
                            if f is not None:
                                json_obj = json.load(f)
                                if json_obj.get("error", None):
                                    continue
                                df = pd.json_normalize(
                                    json_obj,
                                    record_path=['statistics', 'groups', 'statisticsItems'],
                                    meta=[
                                        ['statistics', 'period'],
                                        ['statistics', 'groups', 'groupName']
                                    ],
                                    errors='ignore'
                                )
                                df['eventId'] = f.name.split('/')[-1].split('-')[0]
                                frames.append(df)
            result = pd.concat(frames)
            result.to_parquet(os.path.join(fullpath, 'statistics.parquet.gzip'), compression='gzip')

def normalize_graphs(basepath: str, year: str, sport: str):
    """
    Go through all files and normalize in parquet
    """
    months = sorted(os.listdir(os.path.join(basepath, year)))
    for month in months:
        days = sorted(os.listdir(os.path.join(basepath, year, month)))
        for day in days:
            fullpath = os.path.join(basepath, year, month, day, sport)
            frames = []
            for file in sorted(os.listdir(fullpath)):
                if file.endswith("graph.json.tar.gz"):
                    filpath = os.path.join(fullpath, file)
                    print("file", filpath)
                    with tarfile.open(filpath, "r:gz") as tar:
                        for member in tar.getmembers():
                            f = tar.extractfile(member)
                            if f is not None:
                                json_obj = json.load(f)
                                if json_obj.get("error", None):
                                    continue
                                df = pd.DataFrame(json_obj["graphPoints"])
                                df['eventId'] = f.name.split('/')[-1].split('-')[0]
                                df["periodTime"] = json_obj["periodTime"]
                                df["periodCount"] = json_obj["periodCount"]
                                frames.append(df)
            result = pd.concat(frames)
            result.to_parquet(os.path.join(fullpath, 'graph.parquet.gzip'), compression='gzip')

if __name__ == "__main__":
    main()

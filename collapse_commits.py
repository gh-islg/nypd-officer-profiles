
#!/usr/bin/env reform
#%%
import git 
import pandas as pd
import json
import time
import requests
from tqdm.auto import tqdm
import string
import os
# %%
#%%
def get_all_commits(data_path="nypd-profiles-A.json"):
    '''
    Get the latest commit diff of a file.
    '''
    repo = git.Repo()
    commits = list(repo.iter_commits(paths=data_path))
    return commits
#%%
def parse_training(commit, officer_letter):
    '''
    Take each officer JSON file and get their training history. Consists of training name and the date it was taken.
    '''
    print(f'{officer_letter}') 
    json_url = f"https://raw.githubusercontent.com/gh-islg/nypd-officer-profiles/{commit}/nypd-profiles-{officer_letter}.json"
    response = requests.get(json_url)
    json_data = json.loads(response.text)
    # with open(json_path, "rb") as f:
    #     json_data = json.load(f)
    officer_training_history_df = pd.DataFrame()
    for officer in tqdm(range(len(json_data))):
        tax_id = json_data[officer]['taxid']
        reports_data = json_data[officer]['reports']
        try:
            training_data = reports_data['training']
            training_df = pd.DataFrame.from_dict(training_data)
        except:
            print(f'No training data for officer {tax_id}!') 
            training_df = pd.DataFrame()
        training_df['taxid'] = tax_id
        training_df['rank'] = json_data[officer]['rank']
        officer_training_history_df = pd.concat([officer_training_history_df, training_df])
    officer_training_history_df = officer_training_history_df.reset_index(drop=True)
    return officer_training_history_df
#%%
all_commits = get_all_commits()
letters = list(string.ascii_uppercase)
#%%
# figure out diescrepancy between git
# - not difference between committer vs. authored/...
# is it the offsets?
[_.committer_tz_offset for _ in all_commits]

#%%
monthly_commits = all_commits[20:][::30]
for i in monthly_commits[23:]:
    print(f'Scraping commit: {i}') 
    results = pd.DataFrame()
    for j in letters:
        result = parse_training(commit = i, officer_letter = j)
        result['commit_id'] = str(i)
        result['commit_date'] = i.committed_datetime
        result['officer_letter'] = j
        # put letters together for each commit day
        results = pd.concat([results, result])
    # save
    results.to_parquet(f'officer_training/commit-{i.committed_date}.parquet.gzip', compression='gzip')
#%%
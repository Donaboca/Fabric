#!/usr/bin/env python
# coding: utf-8

# ## Nb write json file
# 
# New notebook

# In[1]:


# Write Json file
import json
file_api_path = '/lakehouse/default/Files/pipeline_information/'
file_name = 'first_run.json'

# 1 = alkuhaku
new_data = {
    "first_run": 1
}

json_data = json.dumps(new_data, indent=4)

with open(file_api_path + file_name, 'w') as file:
    file.write(json_data)


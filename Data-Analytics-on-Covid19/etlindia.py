import requests
import json                 # Used to load data into JSON format
import pandas as pd
import ast
pd.set_option('display.max_rows', 100000)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

def state_replace_func(df):
    df["Statecode"].replace("AN","Andaman and Nicobar Islands",inplace=True)
    df["Statecode"].replace("AP","Andhra Pradesh",inplace=True)
    df["Statecode"].replace("AR","Arunachal Pradesh",inplace=True)
    df["Statecode"].replace("AS","Assam",inplace=True)
    df["Statecode"].replace("BR","Bihar",inplace=True)
    df["Statecode"].replace("CH","Chandigarh",inplace=True)
    df["Statecode"].replace("CT","Chattisgarh",inplace=True)
    df["Statecode"].replace("DL","Delhi",inplace=True)
    df["Statecode"].replace("DN","dadra and Nagar Haveli and daman and diu",inplace=True)
    df["Statecode"].replace("GA","Goa",inplace=True)
    df["Statecode"].replace("GJ","Gujarat",inplace=True)
    df["Statecode"].replace("HP","Himachal Pradesh",inplace=True)
    df["Statecode"].replace("HR","Haryana",inplace=True)
    df["Statecode"].replace("JH","Jharkand",inplace=True)
    df["Statecode"].replace("JK","Jammu and Kashmir",inplace=True)
    df["Statecode"].replace("KA","Karnataka",inplace=True)
    df["Statecode"].replace("KL","kerala",inplace=True)
    df["Statecode"].replace("LA","Ladakh",inplace=True)
    df["Statecode"].replace("MH","Maharashtra",inplace=True)
    df["Statecode"].replace("ML","Meghalaya",inplace=True)
    df["Statecode"].replace("MN","Manipur",inplace=True)
    df["Statecode"].replace("MP","Madhya Pradesh",inplace=True)
    df["Statecode"].replace("MZ","Mizoram",inplace=True)    
    df["Statecode"].replace("NL","Nagaland",inplace=True)
    df["Statecode"].replace("OR","Odisha",inplace=True)
    df["Statecode"].replace("PB","Punjab",inplace=True)
    df["Statecode"].replace("PY","Puducherry",inplace=True)
    df["Statecode"].replace("RJ","Rajasthan",inplace=True)
    df["Statecode"].replace("SK","Sikkim",inplace=True)
    df["Statecode"].replace("TG","Telangana",inplace=True)
    df["Statecode"].replace("TN","Tamil Nadu",inplace=True)
    df["Statecode"].replace("TR","Tripura",inplace=True)
    df["Statecode"].replace("TT","Total count",inplace=True)
    df["Statecode"].replace("UP","Uttar Pradesh",inplace=True)
    df["Statecode"].replace("UT","Uttarkhand",inplace=True)
    df["Statecode"].replace("WB","West Bengal",inplace=True)  

def preprocessing(df):
    for i in range(len(df)):
        if(len(state_total_cases[i].keys())==1):
            if ("confirmed") in state_total_cases[i].keys():
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            if "deceased" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["recovered"]=0
            if ("migrated") in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            if "recovered" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
            if ("tested") in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            
        
        if(len(state_total_cases[i].keys())==2):
            if ("confirmed" and "deceased") in state_total_cases[i].keys():
                state_total_cases[i]["recovered"]=0
            if("confirmed" and "migrated") in state_total_cases[i].keys():
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            if ("confirmed" and "recovered") in state_total_cases[i].keys():
                state_total_cases[i]["deceased"] = 0
            if ("confirmed" and "tested") in state_total_cases[i].keys():
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            if "deceased" and "migrated" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["recovered"]=0
            if "deceased" and "recovered" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
            if "deceased" and "tested" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["recovered"]=0
            if "migrated" and "recovered" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
            if "migrated" and "tested" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0            
            if "recovered" and "tested" in state_total_cases[i].keys():
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0

                
        if(len(state_total_cases[i].keys())==3):
            if (("confirmed" and "deceased" and "tested") in state_total_cases[i].keys() and 
                ("recovered" not in state_total_cases[i].keys())):
                state_total_cases[i]["recovered"]=0
            if (("confirmed" and "recovered" and "tested") in state_total_cases[i].keys() and 
                ("deceased" not in state_total_cases[i].keys())):
                state_total_cases[i]["deceased"]=0
            if (("confirmed" and "deceased" and "migrated") in state_total_cases[i].keys() and 
                ("recovered" not in state_total_cases[i].keys())):
                state_total_cases[i]["recovered"]=0
            if (("confirmed" and "deceased" and "tested") in state_total_cases[i].keys() and 
                ("recovered" not in state_total_cases[i].keys())):
                state_total_cases[i]["recovered"]=0
            if (("confirmed" and "migrated" and "recovered") in state_total_cases[i].keys() and 
                ("deceased" not in state_total_cases[i].keys())):
                state_total_cases[i]["deceased"]=0
            if (("confirmed" and "migrated" and "tested") in state_total_cases[i].keys() and 
                (("recovered" and "deceased") not in state_total_cases[i].keys())):
                state_total_cases[i]["deceased"]=0
                state_total_cases[i]["recovered"]=0
            if (("deceased" and "migrated" and "recovered") in state_total_cases[i].keys() and 
                ("confirmed" not in state_total_cases[i].keys())):
                state_total_cases[i]["confirmed"]=0
            if (("deceased" and "migrated" and "tested") in state_total_cases[i].keys() and 
                (("recovered" and "confirmed") not in state_total_cases[i].keys())):
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["recovered"]=0
            if (("deceased" and "recovered" and "tested") in state_total_cases[i].keys() and 
                ("confirmed" not in state_total_cases[i].keys())):
                state_total_cases[i]["confirmed"]=0
            if (("migrated" and "recovered" and "tested") in state_total_cases[i].keys() and 
                (("confirmed" and "deceased") not in state_total_cases[i].keys())):
                state_total_cases[i]["confirmed"]=0
                state_total_cases[i]["deceased"]=0
            if ("confirmed" and "recovered" and "deceased") in state_total_cases[i].keys():
                continue
            if ("confirmed" and "deceased" and "recovered") in state_total_cases[i].keys():
                continue
            
                
        if(len(state_total_cases[i].keys())==4):
            if (("confirmed" and "deceased" and "migrated" and "tested") in state_total_cases[i].keys() and 
                 ("recovered" not in state_total_cases[i].keys())):
                state_total_cases[i]["recovered"]=0
            if (("confirmed" and "migrated" and "recovered" and "tested") in state_total_cases[i].keys() and 
                  ("deceased" not in state_total_cases[i].keys())):
                state_total_cases[i]["deceased"]=0 
               
            if (("deceased" and "migrated" and "recovered" and "tested") in state_total_cases[i].keys() and 
                ("confirmed" not in state_total_cases[i].keys())):
                state_total_cases[i]["confirmed"]=0
            if ("confirmed" and "deceased" and "migrated" and "recovered") in state_total_cases[i].keys():
                continue
            if ("confirmed" and "deceased" and "recovered" and "tested") in state_total_cases[i].keys():
                continue
            if ("confirmed" and "migrated" and "deceased" and "recovered") in state_total_cases[i].keys():
                continue
            if ("confirmed" and "tested" and "deceased" and "recovered") in state_total_cases[i].keys():
                continue
            if ("migrated" and "confirmed" and "deceased" and "recovered") in state_total_cases[i].keys():
                continue
            if ("confirmed" and "deceased" and "tested" and "recovered") in state_total_cases[i].keys():
                continue
            if ("tested" and "confirmed" and "deceased" and "recovered") in state_total_cases[i].keys():
                continue
                
            
        if(len(state_total_cases[i].keys())==5):
            continue
    return df

        
url = "https://api.covid19india.org/v3/timeseries.json"
response = requests.get(url).content
df = pd.read_json(response)
df.to_csv("unprocessed_india_states_data.csv")

data = pd.read_csv("unprocessed_india_states_data.csv")
data = data.rename({'Unnamed: 0': 'Date'}, axis=1)
info_var = "{'total':{'confirmed':0,'deceased':0,'recovered':0}}"
for i in data.columns:
    data[i]=data[i].apply(lambda x: x if x == x else info_var)

data = data.sort_values((['Date']))

date_list = data.iloc[:,0]
data.drop('UN',axis=1,inplace=True)
data.reset_index(inplace = True, drop = True)
data1 = data.copy()
data1.drop(["Date"],axis=1,inplace=True)
date_list = []
state_list = []
casesinfo_list = []
for i in range(len(data)):
    for j in data1.columns:
        date_list.append(data["Date"][i])
        state_list.append(j)
        casesinfo_list.append(data[j][i])
data = {'Date':date_list, 'Statecode':state_list,'cases_info':casesinfo_list} 
df1 = pd.DataFrame(data) 
state_cases = []
state_total_cases = []
for i in range(len(df1)):
    state_cases.append(ast.literal_eval(df1["cases_info"][i]))
    state_total_cases.append(state_cases[i]['total'])

df1 = preprocessing(df1)  
#for i in range(len(df2)):
    #print(state_total_cases[i])   
   
state_confirmed_cases = []
state_deceased_cases = []
state_recovered_cases = []
state_active_cases = []

#for i in range(len(state_total_cases)):
    #print(state_total_cases[i]['deceased'])
for i in range(len(state_total_cases)):
    state_confirmed_cases.append(state_total_cases[i]["confirmed"])
    state_deceased_cases.append(state_total_cases[i]['deceased'])
    state_recovered_cases.append(state_total_cases[i]['recovered'])
    state_active_cases.append(state_total_cases[i]['confirmed']-(state_total_cases[i]['deceased']
                                                                 +state_total_cases[i]['recovered']))
dict2={'Date':date_list, 'Statecode':state_list,"confirmed":state_confirmed_cases,"deaths":state_deceased_cases,
     "recovered":state_recovered_cases,"active":state_active_cases}
covid_cases_data =  pd.DataFrame(dict2)
state_replace_func(covid_cases_data)
covid_cases_data = covid_cases_data[["Date","Statecode","confirmed","active","deaths","recovered"]]
covid_cases_data.drop(covid_cases_data.loc[covid_cases_data['Statecode']== 'Total count' ].index, inplace=True)

covid_cases_data.to_csv("states_data.csv")


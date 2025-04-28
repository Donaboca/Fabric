#!/usr/bin/env python
# coding: utf-8

# ## nb_lahti_data_loader
# 
# New notebook

# # Datan haku avoimesta rajapinnasta
# 
# UserTypes:
# 
# 1: pedestrian, 2: bicycle, 3: horse, 4: car, 5: bus, 6: minibus, 7: undefined, 8: motorcycle, 9: kayak, 13: e-scooter, 14: truck
# - Käytetään vain UserTypejä 1 ja 2.
# 
# #### Asetetaan hakutyyppi parametrisoluun
# - Oletuksena 0, viikkohaku.
# - Parametrilla kuitenkin määritellään minkätyyppinen haku on.
# 

# In[1]:


# Parametrisolu
# hakutyyppi: 0 = viikkohaku, 1 = alkuhaku, 2 = vikatilanne, keskeytys
hakutyyppi = 2


# #### Kirjastojen tuonnit, url ja headerit sekä haku

# In[2]:


# Importit, yhteysparametrit ja yhteyden muodostus
import requests
from requests.exceptions import HTTPError
import numpy as np
import pandas as pd
import datetime
import time
from notebookutils import mssparkutils

headers = {'Accept': 'application/json',
           'Authorization': 'Bearer 6ce24c435cbf1d201f83ea2e60f4da'}
url = 'https://apieco.eco-counter-tools.com/api/1.0/site'



# ## Asemat ja kulkijatyypit parquet tiedostoon/tiedostosta
# 
# Ensihaussa tehdään max kolme yritystä saada data rajapinnasta. Jos se ei onnistu, notebookin ajo päätetään ja poistutaan exit-arvon kera pipelineen.

# In[3]:


MAX_RETRIES = 3
DELAY = 5
success = False

# Jos viikkohaku, niin asemat ja kulkijatyypit haetaan ensihaussa muodostetuista tiedostoista
if hakutyyppi == 0:
    data = pd.read_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/asemat_userTypet.parquet')
# Muuten haetaan asemat ja kulkijatyypit rajapinnasta
elif hakutyyppi == 1:
    for haku in range(1, MAX_RETRIES+1):
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            
            print(f'Tiedonhaku onnistui!')
            # Viedään alkuhaussa mittausasemat ja valitut tiedot dataframeen
            data = resp.json()
            df = pd.json_normalize(data, record_path='channels', meta='name', meta_prefix='real_')

            data = df[['id', 'real_name', 'name', 'userType']].copy()
            data = data.set_index('id')
            data.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/asemat_userTypet.parquet')
            success = True
            break
        except HTTPError as http_err:
            print(f'Tiedonhaussa ilmeni HTTP-virhe: {http_err}')
        except Exception as e:
            print(f'Tiedonhaussa ilmeni virhe: {e}')

        if haku < MAX_RETRIES:
            print(f'Yritetään uudestaan {DELAY} sekunnin kuluttua.')
            time.sleep(DELAY)
    # Jos dataa ei saada kolmella yrityksellä, niin lopetetaan notebookin ajo tähän soluun ja palataan pipelineen
    # exit-valuen kera
    if not success:
        mssparkutils.notebook.exit('Fail')
    else:
        print('Jatketaan Notebookin suorittamista...')


# In[5]:


print(hakutyyppi)
data.head()


# #### Asema ID:t parquet-tiedostoon (ensihaussa) / haku parquet-tiedostosta.

# In[6]:


# Ensihaussa Asema ID:t df ja sen tallennus parquet-tiedostoksi. Hakutyyppi 1 tarkoittaa ensihakua.
if hakutyyppi == 1:
    asema_idt = data.index
    asema_idt = np.array(asema_idt)
    asema_idt_df = pd.DataFrame({'Station_id': asema_idt})
    asema_idt_df.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/asema_idt.parquet')
# Viikkohaussa voidaan käyttää tallennettua tiedostoa - data on staattista
else:
    asema_idt_df = pd.read_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/asema_idt.parquet')
    asema_idt = asema_idt_df['Station_id']


# In[7]:


asema_idt


# #### Funktiot
# - date_to_string -> muutetaan date string-tyypppiseksi sekä lisätään merkkijonon perään aikaleima
# - hakupaivat -> viikkohaussa palautetaan viime viikon ma ja su tai alkuhaussa edellisen vuoden tammikuun ensimmäinen päivä sekä viime viikon sunnuntai.

# In[8]:


def date_to_string(date, time):
    return date.strftime("%Y-%m-%d") + time
    
def hakupaivat(hakutyyppi):
    current_date = datetime.datetime.now()
    current_day_of_week = current_date.weekday()
    # Alkuhaku 1
    if hakutyyppi == 1:
        start_day = datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')
        end_day = (current_date - datetime.timedelta(days=current_day_of_week + 7)) + datetime.timedelta(days=6) 
    # Muuten viikkohaku (0)
    else:
        start_day = current_date - datetime.timedelta(days=current_day_of_week + 7)
        end_day = start_day + datetime.timedelta(days=6)

    haun_alku_str = date_to_string(start_day, 'T00:00:00')
    haun_loppu_str = date_to_string(end_day, 'T23:59:59')
    
    return haun_alku_str, haun_loppu_str


# #### Funktio kavijamaarat
# - Lisätään data df:ään kävijämäärät, joko kaikki tai viikkotason määrä riippuen hakutyypistä.
# 

# In[9]:


### Datan haku ja kävijämäärien lisääminen mittausasemadataan (df: data)
# Mittauspisteen ohittaneet, alkuarvo 0

data['user_count'] = 0

# dataframe
data_pvm = pd.DataFrame(columns=['id', 'pvm', 'viikonpäivä', 'user_type', 'määrä']) #  'user_type'
#sk = {'ma': 0, 'ti': 0, 'ke': 0, 'to': 0, 'pe': 0, 'la': 0, 'su':0}
weekdays = ['maanatai', 'tiistai', 'keskiviikko', 'torstai', 'perjantai', 'lauantai', 'sunnuntai']

def kavijamaarat():
    global data_pvm
    haun_alku_str, haun_loppu_str = hakupaivat(hakutyyppi)
    
    # Haetaan tiedot asema kerrallaan ja lisätään kulkijoiden määrät hakuajalta
    for asema_id in asema_idt:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                r = requests.get('https://apieco.eco-counter-tools.com/api/1.0/data/site/' + str(asema_id) + 
                                '?begin=' + haun_alku_str + '&end=' + haun_loppu_str + '&step=day', headers=headers, timeout=20)
                r.raise_for_status()
                asemadata = r.json()
                laskuri = 0
                for paiva in asemadata:
                    if paiva['counts'] is not None:
                        laskuri += paiva['counts']
                        # 0 = maanantai, 6 = sunnuntai
                        weekday_index = datetime.datetime.strptime(paiva['date'], '%Y-%m-%dT%H:%M:%S%z').weekday()
                        userType = data.loc[data.index==asema_id, 'userType'].values[0]
                        row = pd.Series({
                            'id': asema_id,
                            'pvm': paiva['date'].split('T')[0],
                            'viikonpäivä': weekdays[weekday_index],
                            'user_type': userType,
                            'määrä': paiva['counts']})
                        data_pvm = pd.concat([data_pvm, row.to_frame().T])
                        #sk['ma'] += paiva['counts']


                #print(sk)
                # jos laskuri > 0 ---- data df:n sarakeotsikoita muutettava täsmäämään toisen df:n kanssa?????
                if laskuri:
                    data.loc[asema_id, 'user_count'] = data.loc[asema_id, 'user_count'] + laskuri
                # Jos onnistui, siirrytään seuraavaan asema_id:hen (break while loop)
                break
            
            except HTTPError as http_err:
                print(f'Tiedonaussa ilmeni HTTP-virhe: {http_err}')
            except requests.exceptions.RequestException as re:
                print(f'Virheilmoitus: {re}')   
            except Exception as e:
                print(f'Tiedonhaussa ilmeni virhe: {e}')

            retries += 1
            if retries < MAX_RETRIES:
                print(f'\nYritetään uudestaan asema_id {asema_id} {DELAY} sekunnin kuluttua.\n')
                time.sleep(DELAY)
            else:
                #print(f'Datan haku epäonnistui asema_id:n {asema_id} kohdalla {MAX_RETRIES} yrityksen jälkeen.')
                mssparkutils.notebook.exit('Fail')

            
kavijamaarat()


# In[11]:


data.tail(20)


# In[12]:


data_pvm.head()


# #### Data ja data_pvm df:t parquet-tiedostoksi

# In[19]:


# Jos ensihaku, muodostetaan tiedostot
if hakutyyppi == 1:
    # kokonaismäärät - total_count -taulu   
    data.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/data_total.parquet')
    data_pvm.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/fact_table' + '.parquet')
# Jos viikkohaku, nimetään tiedostot aikaleiman kera
else:
    now_utc = datetime.datetime.now()
    now = now_utc + datetime.timedelta(hours=2)
    tiedoston_aika = now.strftime('%Y%m%d %H%M%S')
    data.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/data_total_' + tiedoston_aika + '.parquet')
    data_pvm.to_parquet('abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/fact_table_' + tiedoston_aika + '.parquet')


# #### userType tiedostoksi - vain ensihaussa
# 
# 1: pedestrian, 2: bicycle, 3: horse, 4: car, 5: bus, 6: minibus, 7: undefined, 8: motorcycle, 9: kayak, 13: e-scooter, 14: truck

# In[15]:


if hakutyyppi == 1:
     userTypes = pd.DataFrame({'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14],
                              'selite': ['jalankulkija', 'pyöräilijä', 'hevonen', 'auto',
                                   'linja-auto', 'minibussi', 'määrittelemätön', 
                                   'moottoripyörä', 'kajakki', 'sähköpotkulauta', 'kuorma-auto']})
     userTypes.to_parquet("abfss://Lahti@onelake.dfs.fabric.microsoft.com/LH_Lahti.Lakehouse/Files/data/user_types.parquet")


# In[ ]:


# Jos kaikki ajettu onnistuneesti, asetetaan notebookin exitValueksi Success
mssparkutils.notebook.exit('Success')


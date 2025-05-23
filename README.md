# Fabric-projekti - tiedostojen ja kuvauksen kera. Git-integraatiota ei voinut asettaa käyttöön työtilastani.
![image](https://github.com/user-attachments/assets/0e9a0e43-583d-4792-9843-8fedd8d03b54)

Dataputki datan hakuun Lahden avoimesta datasta. Datana kevyen liikenteen mittauspisteiden dataa. Datasta poimitaan kevyenliikenteen kulkijatyypit kävelijä ja pyöräilijä. Data tallennetaan Lakehouseen parquet-tiedostoiksi / luetaan parquet-tiedostoista, käsitellään tietomallinnuksen mukaiseksi (tähtimalli) ja sitten tallennetaan valmiiksi jalostettu data delta-tauluihin. Viikkoajoissa, riippuen taulusta, lisätään uudet rivit tai päivitetään olemassa olevien rivien kulkijamääriä. Taustalla on Power BI -raportointi, joka näyttää ajantasaista tietoja - deltataulut ovat nk. direct modessa ja erikseen luodut päivämäärä- ja eräs aputaulu ovat import modessa. Raportoinnissa on hyödynnetty DAX-kieltä.

1. Lookup aktiviteetti tarkistaa json-tiedostosta arvon, onko kyseessä dataputken ensiajo tai sen jälkeen toistuva viikottainen ajo. Kyseinen json-tiedosto on luotu notebookin Nb write json file.ipynb avulla ennen dataputken ensimmäistä ajoa.
2. Pipeline-muuttujan arvo asetetaan edellisen kohdan lopputuleman mukaan: @activity('Lookup first_run').output.value[0].first_run
3. Ajetaan nb_lahti_data_loader.ipynb, jolle välitetään parametrina aiempi muuttujan arvo (nb:ssa parametrisolu). Notebook tekee parametrin mukaan, joko datan alkuhaun tai vaihtoehtoisesti hakee edellisen viikon datan. Notebookin baseparametrin arvoksi: @variables('lookup_value')
4. Notebookin exitValue joko Success tai Fail ja kumpi, sitä selvitetään IF-aktiviteetissa (@equals(activity('Nb first or weekly fetch').output.result.exitValue, 'Success')). Jos Success, edetään tekemättä mitään seuraavaan aktiviteettiin ja jos Fail, niin kirjoitetaan virhelokiin merkintä Nb Writing to a log file if pipeline run has failed.ipynb avulla ja sen jälkeen päätetään dataputken ajo Fail-aktiviteetin avulla.
5. Jos dataputken ajo jatkui edellisestä aktiviteetista eteenpäin, selvitetään IF-aktiviteetin avulla mikä oli alussa olleen Lookup-aktiviteetin saama arvo (@equals(variables('lookup_value'), 1)). Jos kyseessä oli ensiajo, niin luodaan delta-taulut Nb Tables for Power BI.ipynb notebookissa ja sen jälkeen asetetaan json-tiedoston arvoksi 0 (jälleen notebookin avulla, Nb Change first_run value to 0.ipynb), joka ilmaisee viikkoajoa. Jos kyseessä oli viikkoajo, suoritetaan datan lisäys delta-tauluihin: Nb Weekly Update of Tables.ipynb.
6. Jos IF-aktiviteetissa tapahtui virhe, niin kirjoitetaan merkintä jälleen virhelokiin. Muussa tapauksessa dataputki on ajettu onnistuneesti läpi.

## Power BI
- Raportit päivittyvät automaattisesti. Data laken delta-tauluihin on yhteys DirectQuery -tyyppisesti (itse muodostettuihin tauluihin, kuten Measure Tableen Import-tyyppisesti).
- Exporttasin Excel-muotoisen raportin, josta löytyvät kaikki mittarit.
- Power BI -raportin kuvakaappaukset näkee repositoryn https://github.com/Donaboca/PowerBI read me -osiosta.


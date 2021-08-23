# Python Sciprt for DataPipline, to Azure Container !!

## DataPipeline Process:
* Get Data From the PostgresDB
* Store the Data in to Temporary File, which gets auto deleted permenatly from the PC
* Then Pushs it to the blobs of a Container, if the Blob is already present, then it appends, if it is not present then it create a files and appends
* It Appends in the Process fo Batchs, it get's the data by the query stores 500 in a temp file and then sends 100 by 100 to the Azure Container Blobs, after 500 it clears the files and then store next 500.
### Libraries to be Installed:
Instal Azure:
```shell
pip install azure
```
Install psycopg2
```shell
pip install psycopg2-binary
```





"""
Created By Shashi Preetham
"""
from azure.storage.filedatalake import DataLakeServiceClient
import datetime
import psycopg2
import csv
from csv import writer,reader

things_list = []

""" Get the Present Time Stamp """
timestamp = datetime.datetime.now()
date_time = timestamp.strftime("%m-%d-%Y")
print("Current Date and Time: ", date_time)

"""Step 1:Connect to the DB """
conn = psycopg2.connect("host=s1-d-war-pgs-01.postgres.database.azure.com "
                        "dbname=thingworx "
                        "user=shashi@s1-d-war-pgs-01"
                        "password='shashi'")
cur = conn.cursor()
conn.commit()

""" Step 2: Get the Things """
cur.execute("Select Distinct source_id from value_stream;")
records = cur.fetchall();

"""Step 3: Add A Header to the CSV FILE"""
List = ['Things']
with open('things.csv', 'a') as f_object:
    writer_object = writer(f_object)
    writer_object.writerow(List)
    f_object.close()

"""Step 4: Store the things in the CSV File """
with open('things.csv', 'a+') as f:
    writer = csv.writer(f, delimiter=',')
    for row in records:
        writer.writerow(row)
print ("Done Writing")

"""Step 5: Get the Thing Property Values From CSV File """
with open('things.csv', 'r') as read_obj:
    csv_reader = reader(read_obj)
    header = next(csv_reader)
    if header != None:
        for row in csv_reader:
            print(row[0])
            things_list.append(row[0])

"""Step 6: Get the Data from Based on the Things and Store in to the CSV"""
for x in things_list:
    query = ("Select * From public.value_stream where source_id like '" + x + "' LIMIT 1000")
    cur.execute(query)
    records = cur.fetchall();
    filename = x + '.csv'
    with open(filename, 'a+') as f:
        writer = csv.writer(f, delimiter=',')
        for row in records:
            writer.writerow(row)
conn.close()

"""----------------------------------------Data to Azure Data Lake----------------------------------"""


""" Step 1: Connect to the Azure Storage Account """
global service_client
service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", "saimiotanalytics"), credential="y/WSUo7xiM/l9XF9xyrP8XRDTe7l1S96z2zCrheX/8y9iONHZ79HEK87o4KPdP5koOD2nN9e1DmwHeHf2kRD3g==")


""" Step 2: Create A Container """
global file_system_client
file_system_client = service_client.create_file_system(file_system=date_time+"9")


""" Step 4: Upload files to a particular directory of the Container """
for x in things_list:
    filename = x + '.csv'
    file_system_client = service_client.get_file_system_client(file_system=date_time+"9")
    file_client = file_system_client.create_file(filename)
    local_file = open(filename, 'r') # Change the Path over here !!!
    file_contents = local_file.read()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))
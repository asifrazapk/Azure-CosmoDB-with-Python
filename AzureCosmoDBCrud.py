#%% 

import os
from azure.cosmos import CosmosClient, exceptions, PartitionKey
import json
import asyncio
from asyncio import exceptions

URL = "https://asifcosmodb21may.documents.azure.com:443/"
KEY = "fvbXNXGHHlb2MSgdczP5xOQpTKE7WMhyAErgi11pcqqrLBy1hVKaDeeZRORRGIJuwi6W5WlEV3iWACDbNZIGaw=="

#%%

client = CosmosClient(URL, credential=KEY)
DATABASE_NAME = 'PromptTag'
database = client.get_database_client(DATABASE_NAME)
CONTAINER_NAME = 'PromptContainer'
container = database.get_container_client(CONTAINER_NAME)

#%%
## Create a database
try:
    database = client.create_database(DATABASE_NAME)
except exceptions.CosmosResourceExistsError:
    database = client.get_database_client(DATABASE_NAME)

# %%
## Create a container
try:
    container = database.create_container(id=CONTAINER_NAME, partition_key=PartitionKey(path='/id', kind='Hash'))
except exceptions.CosmosResourceExistsError:
    container = database.get_container_client(CONTAINER_NAME)
except exceptions.CosmosHttpResponseError:
    raise

#%%

## Create an analytical store enabled container
# try:
#     container = database.create_container(id=CONTAINER_NAME, partition_key=PartitionKey(path='/id', kind='Hash'),default_ttl=-1)
# except exceptions.CosmosResourceExistsError:
#     container = database.get_container_client(CONTAINER_NAME)
# except exceptions.CosmosHttpResponseError:
#     raise

#%%

## Create records
for i in range(1, 10):
    container.upsert_item({
            'id': 'item{0}'.format(i),
            'TYkey' : '1',
            'productName': 'Widget',
            'productModel': 'Model {0}'.format(i)
        }
    )

#%%

## Query the database
for item in container.query_items(query='SELECT * FROM mycontainer r WHERE r.id="item3"',enable_cross_partition_query=True):
    print(json.dumps(item, indent=True))

#%%
## update the record

# Read the item to update
for item in container.query_items(query='SELECT * FROM mycontainer r WHERE r.id = "item1"',  enable_cross_partition_query=True):
    print(item)

    #update any value
    item['productName'] = "updated product name"
    container.upsert_item(item)

#%%

## Delete data using the product model
for item in container.query_items(query='SELECT * FROM mycontainer r WHERE r.productModel = "Model 2"', enable_cross_partition_query=True):
    print(item)
    container.delete_item(item, partition_key=item['id'])

# %%

## Delete data using the product id
for item in container.query_items(query='SELECT * FROM mycontainer r WHERE r.id="item3"', enable_cross_partition_query=True):
    print(item)
    container.delete_item(item, partition_key=item['id'])


#%%

## Using the asynchronous client
async def create_products():
    async with CosmosClient(URL, credential=KEY) as client: # the with statement will automatically initialize and close the async client
        database = client.get_database_client(DATABASE_NAME)
        container = database.get_container_client(CONTAINER_NAME)
        for i in range(20):
            await container.upsert_item({
                    'id': 'item{0}'.format(i),
                    'productName': 'Widget',
                    'productModel': 'Model {0}'.format(i)
                }
            )
async def main():
    await create_products()

# Run the event loop to execute the asynchronous function
asyncio.run(main())

#%%

## Queries with the asynchronous client
async def create_lists():
    results = container.query_items(query='SELECT * FROM products p WHERE p.productModel = "Model 2"')
    item_list = []
    async for item in results:
        item_list.append(item)
    await client.close()
    return item_list

async def main():
    items = await create_lists()
    for item in items:
        print(item)  
    await client.close()

# Run the event loop to execute the asynchronous function 
asyncio.run(main())


# %%

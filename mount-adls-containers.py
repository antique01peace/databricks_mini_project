# Databricks notebook source
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

def mount_adls(storage_account_name):
    #get secrets from key vault 
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'clientID')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'tenantID')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'clientSecret')

    storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

    dir = '/mnt/formula1dl/'
    folders = dbutils.fs.ls(dir)

    credentials = ClientSecretCredential(tenant_id = tenant_id
                                        ,client_id = client_id
                                        ,client_secret = client_secret)
    
    #set the configurations to authenticate service principal on storage acount
    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
                }
    
    blob_service_client = BlobServiceClient(storage_account_url, credentials)            
    containers = blob_service_client.list_containers()

    for container in containers:
        container_name = container.name
        source = f"abfss://{container.name}@{storage_account_name}.dfs.core.windows.net/"
        mount_point = dir + container.name
        #if folder not exists mount otherwise update mount
        if container.name + '/' in [folder.name for folder in folders]:
            dbutils.fs.updateMount(
                source = source,
                mount_point = mount_point,
                extra_configs = configs
            )
        else:
            dbutils.fs.mount(
                source = source,
                mount_point = mount_point,
                extra_configs = configs
                )

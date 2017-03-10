
# Copy Delimited Files from s3 to Azure Blob, and Ingest into Application Insights

This repo is for a specific use case, to copy *large* numbers of *large* pipe delimieted telemetory files from s3 to Azure Blob on a schedule, and trigger the files ingest into Application Insights.

## Operation ##

The Code performs the following, given the filepath prefix: 
* List all the Blobs that is already in Azure matching the prefix
* List the s3 Blobs with the same prefix
* if the file already exists in Azure blob, then skip (making this re-runable)
* if the file is not in Azure Blob
    * Stream the file from s3 into Azure Blob
    * Changing the file Delimiter from '|' to ','
    * Ingest the comma-delimited file into Application Insights

## To Run ##

Setup the following environemt variables

```
export ACCESSKEYID="<S3 Bucket ACCESS KEY>"
export SECRETACCESSKEY="<S3 Bucket ACCESS KEY SECRET>"
export BUCKET="<S3 Bucket>"
export STORAGEACC="<Azure Blob Storage Account Name>"
export CONTAINER="<Azure Blob Container Name>"
export KEY="<Azure Blob Key>"

# use <yyyy> <mm> <dd> to replace with current date values
export PREFIX="<S3 Prefix to filter the files>" 

export APPINSIGHTS_IKEY="<Application Insights Key>"
export APPINSIGHTS_SOURCENAME="<Application Insights Source>"
```

Then run:

```
node s3 <prefix>  (can use <yyyy> <dd> <mm> in the prefix)
```

# Setup Schedule with PM2

use the `pm2process.json` to schedule the app to run in PM2



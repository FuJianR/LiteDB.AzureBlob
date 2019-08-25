# LiteDB.AzureBlob
A library that stores LiteDB data to Azure Blob Storage (Page blob or block blob).
Please checkout the demo project for usage.

Why?
* Cheap and reliable with reasonable performance for your tiny websites.
* Performance is better than Azure Table Storage when using Azure Page Blob.
* LiteDB is great.

Features:
* Load pages in advance and cache them in memory.
* Merge consecutive writing operations into one batch.
* Use either Azure Page Blob (recommended) or Azure block blob as storage backend.

Status:
* Under development. Not ready for prod usage. 

# TheByteStuff.AzureTableUtilities

[![Join the chat at https://gitter.im/TheByteStuff/AzureTableUtilities](https://badges.gitter.im/TheByteStuff/AzureTableUtilities.svg)](https://gitter.im/TheByteStuff/AzureTableUtilities?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Backup, Copy, Delete and Restore options for use with Azure Tables.  An alternative to tools such as AZCopy.


Backup destination may be to a local file or to Azure storage blob.  File may be optionally compressed (zipped).  Files may also be aged based on a rention days parameter (default is 30).
Copy allows for the same or different connection spec for Source and Destination.
Restore location may be from local file or Azure storage blob.
Blob storage and Azure Table storage connection specs may point to different servers to allow for use in production backup scenarios as well as for restoration of development environments.
Restore/Copy will create the table if it does not exist, however data on an existing table will not be deleted. It may be overwritten depending on the source data.

Delete allows for full table deletion as well as for rows.

When working with Blob storage, a folder in the format {BlobRootName}--table-{TableName} will be used to store the file or retreive the file under the Blob root specified.
File created will be in the format {TableName}_Backup_{TimeStamp}.

Backup, Copy and Delete operations allow for an optional list of filters (Filter) to be provided to select a subset of the source data for the transation.

Refer to project url for code snippets and additional information.


** Note: Per underlying Azure Tables logic, the row TimeStamp value is modified whenever a row is inserted/updated.  As such, on a copy or restore operation the current Date/Time will be updated on the TimeStamp field regardless of what is on the source.
If your application requires retention of an original timestamp value, a different field will need to be used.


The Byte Stuff, LLC is not affiliated with Microsoft nor has Microsoft endorsed this project.

Personal and commercial use of this software is free; however, a donation is appreciated if you find the software useful.

Code for a command line executable along with sample Docker build is available on GitHub -- https://github.com/TheByteStuff/AzureTableBackupRestore

Suggestions or donations are both appreciated and welcome can be made by using the [Contact" tab](https://www.thebytestuff.com/Contact?utm_source=nuget&amp;utm_medium=www&amp;utm_campaign=AzureTableUtilities).

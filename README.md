# TheByteStuff.AzureTableUtilities

   ***     V6.0.0. Backup/Restore is NOT compatible with prior versions       ***   

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

Personal and commercial use of this software is free; however, a [donation](https://www.paypal.com/donate/?hosted_button_id=33CGFK895S2FN) is appreciated if you find the software useful.

Code for a command line executable along with sample Docker build is available on GitHub [AzureTableBackupRestore](https://github.com/TheByteStuff/AzureTableBackupRestore)

Suggestions or donations are both appreciated and welcome can be made by using the [Contact tab](https://www.thebytestuff.com/Contact?utm_source=nuget&amp;utm_medium=www&amp;utm_campaign=AzureTableUtilities).


Version History

+ v6.0.0 - ** Backup/Restore is NOT compatible with prior versions **   Upgrade from Microsoft.Azure.Cosmos.Table to Azure.Data.Table.   The Microsoft.Azure.Cosmos.Table library was deprecated on 3/31/2023.  The Azure.Data.Table API (TableEntity) required a change to retain proper data typing of Int32/Int64 during serlization and is not compatible with the prior version of the customized DynamicTableEntityJsonConverter/Serializer.  As a result, backup files created with prior versions are not compatible to be restored with this release.  Similarly, backups taken with this release may not be restored under prior versions.  It is possible to convert a prior file version to work with the new serialization.  At this time, no tool is being offered to do so.
+ v5.6.2-pre - beta with 4.6.2 as a code base and including Cosmos Table 2.0.0 Preview
+ v5.6.1-pre - beta with 4.6.1 as a code base and including Cosmos Table 2.0.0 Preview
+ v5.4.1-pre - beta with 4.4.1 as a code base and including Cosmos Table 2.0.0 Preview
+ v5.4.0-pre - beta with 4.4.0 as a code base and including Cosmos Table 2.0.0 Preview
+ v5.3.0-pre - beta with 4.3.0 as a code base and including Cosmos Table 2.0.0 Preview
+ v5.0.0-pre - beta including Cosmos Table 2.0.0 Preview
+ v4.6.3 - Updated to Newstonsoft.JSON  v13.0.2 per security vulnerability concern.
+ v4.6.2 - Updated to Azure.Storage.Blobs v12.13.0 per security vulnerability concern.
+ v4.6.1 - Updated to Newtonsoft.Json v13.0.1 per security vulnerability concern.
+ v4.5.1 - Added check to confirm directory exists for Backup to File or Blob as well as Restore from Blob.  A ParameterSpecException if the directory does not exist
+ v4.5.0 - Added "All" option to Copy and Backup to process all tables under a storage connection at once.
+ v4.4.1 - Updated backup to blob logic to increase stream write size as a fix to overflow exceptions for large tables.
+ v4.4.0 - Added additional methods for backup/restore to use Blob storage directly and eliminate the need for a local working directory.
+ v4.3.0 - Added additional parameter validation, code cleanup for initial github publish
+ v4.2.2 - Corrected problem with filters on backup to blob
+ v4.2.1 - Corrected Nuspec to show .Net Standard in docs
+ v4.2.0 - Change target framework from .Net Core 2.0 to .Net Standard 2.0
+ v4.1.0 - Added RowKey option to Filters, update Microsoft.Azure.Cosmos version to 3.10.1
+ v4.0.2 - updated Nuspec to minimum dependent versions
+ v4.0.1 - updated Nuspec to minimum dependent versions
+ v4.0 - Updated package dependencies to latest released versions, removed Microsoft.Extensions.Configuration; Microsoft.Extensions.Configuration.Json; Microsoft.Extensions.Configuration.FileExtensions as they are not needed by base libraries
+ v3.0 - added delete table option, refactored base exception to be AzureTableUtilitiesException, updated class/method documentation
+ v2.1 - added Filter constructor, corrected return message on successful copy.
+ v2.0 - added "Copy" option as well as Filters.
+ v1.0 - Initial release.
using Azure;
using Azure.Data.Tables;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using TheByteStuff.AzureTableUtilities.Exceptions;
//using AZBlob = Microsoft.Azure.Storage.Blob;

using AzureTables = Azure.Data.Tables;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Backup Azure Table to local file or blob storage.
    /// </summary>
    public class BackupAzureTables
    {
        private TableServiceClient tableServiceClient;
        private BlobServiceClient blobServiceClient;


        /// <summary>
        /// Constructor, sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public BackupAzureTables(string AzureConnection) : this(AzureConnection, AzureConnection)
        {
            tableServiceClient = new TableServiceClient(AzureConnection);
            blobServiceClient = new BlobServiceClient(AzureConnection);
        }

        /// <summary>
        /// Directly set the Service Clients
        /// </summary>
        /// <param name="tableServiceClient"></param>
        /// <param name="blobServiceClient"></param>
        public BackupAzureTables(TableServiceClient tableServiceClient, BlobServiceClient blobServiceClient)
        {
            this.tableServiceClient = tableServiceClient;
            this.blobServiceClient = blobServiceClient;
        }


        /// <summary>
        /// Constructor, allows a different connection spec for Azure Table and Azure Blob storage.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        /// <param name="AzureBlobConnection">Connection string for Azure Blob Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public BackupAzureTables(string AzureTableConnection, string AzureBlobConnection)
        {
            if (String.IsNullOrEmpty(AzureTableConnection) || String.IsNullOrEmpty(AzureBlobConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            tableServiceClient = new TableServiceClient(AzureTableConnection);
            blobServiceClient = new BlobServiceClient(AzureBlobConnection);
        }


        /// <summary>
        /// Constructor, accepts SecureString and allows a different connection spec for Azure Table and Azure Blob storage.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection as a SecureString</param>
        /// <param name="AzureBlobConnection">Connection string for Azure Blob Connection as a SecureString</param>
        /*
        public BackupAzureTables(SecureString AzureTableConnection, SecureString AzureBlobConnection)
        {
            if (Helper.IsSecureStringNullOrEmpty(AzureTableConnection) || Helper.IsSecureStringNullOrEmpty(AzureBlobConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            AzureTableConnectionSpec = AzureTableConnection;
            AzureBlobConnectionSpec = AzureBlobConnection;
        }
        */

        /// <summary>
        /// Create a blob file copy of the Azure Table specified.
        /// </summary>
        /// <param name="TableName">Name of Azure Table to backup.</param>
        /// <param name="BlobRoot">Name to use as blob root folder.</param>
        /// <param name="OutFileDirectory">Local directory (path) with authority to create/write a file.</param>
        /// <param name="Compress">True to compress the file.</param>
        /// <param name="Validate">True to validate the written record count matches what was queried.</param>
        /// <param name="RetentionDays">Process will age files in blob created more than x days ago.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param> 
        /// <returns>A string indicating the name of the blob file created as well as a count of how many files were aged.</returns>
        public string BackupTableToBlob(string TableName, string BlobRoot, string OutFileDirectory, bool Compress = false, bool Validate = false, int RetentionDays = 30, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            string OutFileName;
            string OutFileNamePath = "";

            if (String.IsNullOrWhiteSpace(TableName))
            {
                throw new ParameterSpecException("TableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(BlobRoot))
            {
                throw new ParameterSpecException("BlobRoot is missing.");
            }

            if (String.IsNullOrWhiteSpace(OutFileDirectory))
            {
                throw new ParameterSpecException("OutFileDirectory is missing.");
            }

            if (!Directory.Exists(OutFileDirectory))
            {
                throw new ParameterSpecException("OutFileDirectory does not exist.");
            }

            try
            {
                OutFileName = this.BackupTableToFile(TableName, OutFileDirectory, Compress, Validate, TimeoutSeconds, filters);
                OutFileNamePath = Path.Combine(OutFileDirectory, OutFileName);


                var container = blobServiceClient.GetBlobContainerClient(BlobRoot);

                container.CreateIfNotExists();
                var directory = container.GetBlobClient(BlobRoot.ToLower() + "-table-" + TableName.ToLower() + "/" + OutFileName);

                var blobClient = container.GetBlobClient(OutFileName);
                var blobUploadOptions = new BlobUploadOptions
                {
                    TransferOptions = new StorageTransferOptions
                    {
                        MaximumTransferSize = 1024 * 1024 * 32 // Set stream write size to 32MB
                    }
                };
                blobClient.Upload(OutFileNamePath, blobUploadOptions);

                DateTimeOffset OffsetTimeNow = System.DateTimeOffset.Now;
                DateTimeOffset OffsetTimeRetain = System.DateTimeOffset.Now.AddDays(-1 * RetentionDays);


                return String.Format("Table '{0}' backed up as '{1}' under blob '{2}\\{3}';", TableName, OutFileName, BlobRoot, directory.ToString());
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                throw new BackupFailedException(String.Format("Table '{0}' backup failed.", TableName), ex);
            }
            finally
            {
                if ((!String.IsNullOrEmpty(OutFileNamePath)) && (File.Exists(OutFileNamePath)))
                {
                    try
                    {
                        File.Delete(OutFileNamePath);
                    }
                    catch (Exception ex)
                    {
                        throw new AzureTableBackupException(String.Format("Error cleaning up files '{0}'.", OutFileNamePath), ex);
                    }
                }
            }
        }


        /// <summary>
        /// Create a local file copy of the specified Azure Table.
        /// </summary>
        /// <param name="TableName">Name of Azure Table to backup.</param>
        /// <param name="OutFileDirectory">Local directory (path) with authority to create/write a file.</param>
        /// <param name="Compress">True to compress the file.</param>
        /// <param name="Validate">True to validate the written record count matches what was queried.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param>
        /// <returns>A string containing the name of the file created.</returns>
        public string BackupTableToFile(string TableName, string OutFileDirectory, bool Compress = false, bool Validate = false, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            if (String.IsNullOrWhiteSpace(TableName))
            {
                throw new ParameterSpecException("TableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(OutFileDirectory))
            {
                throw new ParameterSpecException("OutFileDirectory is missing.");
            }

            if (!Directory.Exists(OutFileDirectory))
            {
                throw new ParameterSpecException("OutFileDirectory does not exist.");
            }

            string OutFileName = String.Format(TableName + "_Backup_" + System.DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");
            string OutFileNameCompressed = String.Format(TableName + "_Backup_" + System.DateTime.Now.ToString("yyyyMMddHHmmss") + ".7z");
            int RecordCount = 0;

            if (!Filter.AreFiltersValid(filters))
            {
                throw new ParameterSpecException(String.Format("One or more of the supplied filter criteria is invalid."));
            }

            string OutFileCreated = "";
            string OutFileNamePath = "";
            string OutFileNamePathCompressed = "";
            if (Path.GetFullPath(OutFileDirectory) != OutFileDirectory)
            {
                throw new ParameterSpecException(String.Format("Invalid output directory '{0}' specified.", OutFileDirectory));
            }
            else
            {
                OutFileNamePath = Path.Combine(OutFileDirectory, OutFileName);
                OutFileNamePathCompressed = Path.Combine(OutFileDirectory, OutFileNameCompressed);
            }

            try
            {
                var entitiesSerialized = new List<string>();

                DynamicTableEntityJsonSerializer serializer = new DynamicTableEntityJsonSerializer();

                TableSpec TableSpecStart = new TableSpec(TableName);

                Pageable<AzureTables.TableEntity> queryResultsFilter = tableServiceClient.GetTableClient(TableName).Query<AzureTables.TableEntity>(filter: Filter.BuildFilterSpec(filters), maxPerPage: 100);

                using (StreamWriter OutFile = new StreamWriter(OutFileNamePath))
                {
                    OutFile.WriteLine(System.Text.Json.JsonSerializer.Serialize(TableSpecStart));

                    foreach (Page<AzureTables.TableEntity> page in queryResultsFilter.AsPages())
                    {
                        List<AzureTables.TableEntity> entityList = new List<AzureTables.TableEntity>();

                        foreach (AzureTables.TableEntity qEntity in page.Values)
                        {
                            // Build a batch to insert
                            entityList.Add(qEntity);
                            //OutFile.WriteLine(JsonConvert.SerializeObject(qEntity));  // Int32 type gets lost with stock serializer
                            OutFile.WriteLine(serializer.Serialize(qEntity));
                            RecordCount++;
                        }
                    }

                    TableSpec TableSpecEnd = new TableSpec(TableName, RecordCount);
                    OutFile.WriteLine(System.Text.Json.JsonSerializer.Serialize(TableSpecEnd));

                    OutFile.Flush();
                    OutFile.Close();
                }

                if (Validate)
                {
                    int InRecords = 0;
                    // Read file/validate footer
                    using (StreamReader InFile = new StreamReader(OutFileNamePath))
                    {
                        string HeaderRec = InFile.ReadLine();
                        string FooterRec = "";
                        string DetailRec = "x";
                        do
                        {
                            DetailRec = InFile.ReadLine();
                            if (DetailRec == null)
                            {
                                InRecords--;
                            }
                            else
                            {
                                InRecords++;
                                FooterRec = DetailRec;
                            }
                        } while (DetailRec != null);
                        InFile.Close();

                        TableSpec footer = System.Text.Json.JsonSerializer.Deserialize<TableSpec>(FooterRec);

                        if ((footer.RecordCount == InRecords) && (footer.TableName.Equals(TableName)))
                        {
                            //Do nothing, in count=out count
                        }
                        else
                        {
                            throw new AzureTableBackupException("Backup file validation failed.");
                        }
                    }
                }

                // https://stackoverflow.com/questions/11153542/how-to-compress-files
                if (Compress)
                {
                    FileStream FileToCompress = File.OpenRead(OutFileNamePath);
                    byte[] buffer = new byte[FileToCompress.Length];
                    FileToCompress.Read(buffer, 0, buffer.Length);

                    FileStream CompressedFileTarget = File.Create(OutFileNamePathCompressed);
                    using (GZipStream OutFileCompressed = new GZipStream(CompressedFileTarget, CompressionMode.Compress))
                    {
                        OutFileCompressed.Write(buffer, 0, buffer.Length);
                    }
                    FileToCompress.Close();
                    CompressedFileTarget.Close();
                    if (File.Exists(OutFileNamePath))
                    {
                        try
                        {
                            File.Delete(OutFileNamePath);
                        }
                        catch (Exception ex)
                        {
                            throw new AzureTableBackupException(String.Format("Error cleaning up files '{0}'.", OutFileNamePath), ex);
                        }
                    }
                    OutFileCreated = OutFileNameCompressed;
                }
                else
                {
                    OutFileCreated = OutFileName;
                }
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Azure.Core.ConnectionString.Validate"))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }
                else
                {
                    throw new BackupFailedException(String.Format("Table '{0}' backup failed.", TableName), ex);
                }
            }
            return OutFileCreated;
        }


        /// <summary>
        /// Backup table directly to Blob.
        /// </summary>
        /// <param name="tableName">Name of Azure Table to backup.</param>
        /// <param name="blobRoot">Name to use as blob root folder.</param>
        /// <param name="compress">True to compress the file.</param>
        /// <param name="RetentionDays">Process will age files in blob created more than x days ago.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param>
        /// <returns>A string containing the name of the file created.</returns>
        public string BackupTableToBlobDirect(string tableName, string blobRoot, bool compress = false, int RetentionDays = 90, int TimeoutSeconds = 30, List<Filter> filters = default)
        {

            var fileName = tableName + "_Backup_" + System.DateTime.Now.ToString("yyyyMMddHHmmss"); //This is terrible because for multiple tables in a folder, the date will be different for each table

            var result = BackupTableToBlobDirect(tableName, blobRoot, blobRoot.ToLower() + "-table-" + tableName.ToLower(), fileName, compress, filters);

            DateTimeOffset OffsetTimeNow = System.DateTimeOffset.Now;
            DateTimeOffset OffsetTimeRetain = System.DateTimeOffset.Now.AddDays(-1 * RetentionDays);

            // DO CLEANUP AS BEFORE

            return result;
        }
        public string BackupTableToBlobDirect(string tableName, string blobRoot, string folderName, string fileName, bool compress = false, List<Filter> filters = default)
        {
            int RecordCount = 0;

            if (string.IsNullOrWhiteSpace(tableName))
                throw new ParameterSpecException("TableName is missing.");

            if (string.IsNullOrWhiteSpace(blobRoot))
                throw new ParameterSpecException("BlobRoot is missing.");

            try
            {

                var container = blobServiceClient.GetBlobContainerClient(blobRoot);
                container.CreateIfNotExists();


                var outFileName = $"{(string.IsNullOrWhiteSpace(folderName) ? "" : folderName + "/")}{fileName}.txt{(compress ? ".7z" : "")}";

                var blobClient = container.GetBlobClient(outFileName);
                var blobUploadOptions = new BlobUploadOptions
                {
                    TransferOptions = new StorageTransferOptions
                    {
                        MaximumTransferSize = 1024 * 1024 * 32 // Set stream write size to 32MB
                    }
                };

                // start upload from stream, iterate through table, possible inline compress
                try
                {

                    //TODO  Timeout set?
                    //table.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);

                    var entitiesSerialized = new List<string>();
                    var serializer = new DynamicTableEntityJsonSerializer();

                    var TableSpecStart = new TableSpec(tableName);
                    var NewLineAsBytes = Encoding.UTF8.GetBytes("\n");

                    var tempTableSpecStart = Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(TableSpecStart));
                    var bs2 = blobClient.OpenWrite(true);
                    Stream bs = bs2;

                    if (compress)
                    {
                        bs = new GZipStream(bs2, CompressionMode.Compress);
                    }
                    else
                    {
                        bs = bs2;
                    }

                    bs.Write(tempTableSpecStart, 0, tempTableSpecStart.Length);
                    bs.Flush();
                    bs.Write(NewLineAsBytes, 0, NewLineAsBytes.Length);
                    bs.Flush();

                    var queryResultsFilter = tableServiceClient.GetTableClient(tableName).Query<AzureTables.TableEntity>(filter: Filter.BuildFilterSpec(filters), maxPerPage: 100);
                    foreach (var page in queryResultsFilter.AsPages())
                    {
                        foreach (var qEntity in page.Values)
                        {
                            // var tempDTE = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(qEntity));  // Int32 type gets lost in stock serializer
                            var tempDTE = Encoding.UTF8.GetBytes(serializer.Serialize(qEntity));
                            bs.Write(tempDTE, 0, tempDTE.Length);
                            bs.Flush();
                            bs.Write(NewLineAsBytes, 0, NewLineAsBytes.Length);
                            bs.Flush();
                            RecordCount++;
                        }
                    }
                    var TableSpecEnd = new TableSpec(tableName, RecordCount);
                    var tempTableSpecEnd = Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(TableSpecEnd));
                    bs.Write(tempTableSpecEnd, 0, tempTableSpecEnd.Length);
                    bs.Flush();
                    bs.Write(NewLineAsBytes, 0, NewLineAsBytes.Length);
                    bs.Flush();
                    bs.Close();
                }
                catch (Exception ex)
                {
                    throw new BackupFailedException(String.Format("Table '{0}' backup failed.", tableName), ex);
                }


                return String.Format("Table '{0}' backed up as '{1}' under blob '{2}'.", tableName, outFileName, blobRoot);
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                throw new BackupFailedException(String.Format("Table '{0}' backup failed.", tableName), ex);
            }
            finally
            {
            }
        } // BackupTableToBlobDirect


        /// <summary>
        /// Backup all tables direct to Blob storage.
        /// </summary>
        /// <param name="BlobRoot">Name to use as blob root folder.</param>
        /// <param name="Compress">True to compress the file.</param>
        /// <param name="RetentionDays">Process will age files in blob created more than x days ago.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param>
        /// <returns>A string containing the name of the file(s) created as well as any backups aged.</returns>
        public string BackupAllTablesToBlob(string BlobRoot, bool Compress = false, int RetentionDays = 30, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            if (String.IsNullOrWhiteSpace(BlobRoot))
            {
                throw new ParameterSpecException("BlobRoot is missing.");
            }

            try
            {
                StringBuilder BackupResults = new StringBuilder();
                List<string> TableNames = Helper.GetTableNames(tableServiceClient);
                if (TableNames.Count() > 0)
                {
                    foreach (string TableName in TableNames)
                    {
                        BackupResults.Append(BackupTableToBlobDirect(TableName, BlobRoot, Compress, RetentionDays, TimeoutSeconds, filters) + "|");
                    }
                    return BackupResults.ToString();
                }
                else
                {
                    return "No Tables found.";
                }
            }
            catch (Exception ex)
            {
                throw new BackupFailedException(String.Format("Backup of all tables to blob '{0}' failed.", BlobRoot), ex);
            }
        } // BackupAllTablesToBlob



        public string BackupAllTablesToBlob(string blobRoot, string folderName, bool compress = false, List<Filter> filters = default(List<Filter>))
        {
            if (String.IsNullOrWhiteSpace(blobRoot))
            {
                throw new ParameterSpecException("BlobRoot is missing.");
            }

            try
            {
                StringBuilder BackupResults = new StringBuilder();
                List<string> TableNames = Helper.GetTableNames(tableServiceClient);
                if (TableNames.Count() > 0)
                {
                    foreach (string tableName in TableNames)
                    {
                        BackupResults.Append(BackupTableToBlobDirect(tableName, blobRoot, folderName, tableName, compress, filters) + "|");
                    }
                    return BackupResults.ToString();
                }
                else
                {
                    return "No Tables found.";
                }
            }
            catch (Exception ex)
            {
                throw new BackupFailedException(string.Format("Backup of all tables to blob '{0}' failed.", blobRoot), ex);
            }
        } // BackupAllTablesToBlob
    }
}

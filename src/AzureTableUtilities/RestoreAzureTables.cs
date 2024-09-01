using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Restore local file or blob file to Azure Table.
    /// </summary>
    public class RestoreAzureTables
    {
        private TableServiceClient tableServiceClient;
        private BlobServiceClient blobServiceClient;


        /// <summary>
        /// Constructor, sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public RestoreAzureTables(string AzureConnection) : this(AzureConnection, AzureConnection)
        {
            tableServiceClient = new TableServiceClient(AzureConnection);
            blobServiceClient = new BlobServiceClient(AzureConnection);
        }

        /// <summary>
        /// Directly set the Service Clients
        /// </summary>
        /// <param name="tableServiceClient"></param>
        /// <param name="blobServiceClient"></param>
        public RestoreAzureTables(TableServiceClient tableServiceClient, BlobServiceClient blobServiceClient)
        {
            this.tableServiceClient = tableServiceClient;
            this.blobServiceClient = blobServiceClient;
        }


        /// <summary>
        /// Constructor, allows a different connection spec for Azure Table and Azure Blob storage.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        /// <param name="AzureBlobConnection">Connection string for Azure Blob Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public RestoreAzureTables(string AzureTableConnection, string AzureBlobConnection)
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
        public RestoreAzureTables(SecureString AzureTableConnection, SecureString AzureBlobConnection)
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
        /// Restore file created in blob storage by BackupAzureTables to the destination table name specified.
        /// Blob file will be downloaded to local storage before reading.  If compressed, it will be decompressed on local storage before reading.
        /// </summary>
        /// <param name="DestinationTableName">Name of the Azure Table to restore to -  may be different than name backed up originally.</param>
        /// <param name="OriginalTableName">Name of the Azure Table originally backed (required for determining blob directory to use)</param>
        /// <param name="BlobRoot">Name to use as blob root folder.</param>
        /// <param name="WorkingDirectory">Local directory (path) with authority to create/write a file.</param>
        /// <param name="BlobFileName">Name of the blob file to restore.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <returns>A string indicating the table restored and record count.</returns>
        public string RestoreTableFromBlob(string DestinationTableName, string OriginalTableName, string BlobRoot, string WorkingDirectory, string BlobFileName, int TimeoutSeconds = 30)
        {
            string result = "Error";

            if (String.IsNullOrWhiteSpace(DestinationTableName))
            {
                throw new ParameterSpecException("DestinationTableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(OriginalTableName))
            {
                throw new ParameterSpecException("OriginalTableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(WorkingDirectory))
            {
                throw new ParameterSpecException("WorkingDirectory is missing.");
            }

            if (!Directory.Exists(WorkingDirectory))
            {
                throw new ParameterSpecException("WorkingDirectory does not exist.");
            }

            if (String.IsNullOrWhiteSpace(BlobFileName))
            {
                throw new ParameterSpecException(String.Format("Invalid BlobFileName '{0}' specified.", BlobFileName));
            }
            bool Decompress = BlobFileName.EndsWith(".7z");

            if (String.IsNullOrWhiteSpace(BlobRoot))
            {
                throw new ParameterSpecException(String.Format("Invalid BlobRoot '{0}' specified.", BlobRoot));
            }

            if (Path.GetFullPath(WorkingDirectory) != WorkingDirectory)
            {
                throw new ParameterSpecException(String.Format("Invalid WorkingDirectory '{0}' specified.", WorkingDirectory));
            }


            try
            {
                var container = blobServiceClient.GetBlobContainerClient(BlobRoot);
                container.CreateIfNotExists();
                var directory = container.GetBlobClient(BlobRoot.ToLower() + "-table-" + OriginalTableName.ToLower() + "/" + BlobFileName);

                string WorkingFileNamePath = Path.Combine(WorkingDirectory, BlobFileName);
                string WorkingFileNamePathCompressed = Path.Combine(WorkingDirectory, BlobFileName);
                /*
                 * If file is compressed, WorkingFileNamePath will be set to .txt
                 * If file is not compressed WorkingFileNamePathCompressed will be left as .txt
                 */
                if (Decompress)
                {
                    WorkingFileNamePath = WorkingFileNamePath.Replace(".7z", ".txt");
                }
                else
                {
                    //WorkingFileNamePathCompressed = WorkingFileNamePathCompressed.Replace(".txt", ".7z");
                }

                var blobClient = container.GetBlobClient(BlobFileName);
                blobClient.DownloadTo(WorkingFileNamePathCompressed);


                //https://www.tutorialspoint.com/compressing-and-decompressing-files-using-gzip-format-in-chash
                if (Decompress)
                {
                    FileStream FileToDeCompress = File.OpenRead(WorkingFileNamePathCompressed);
                    using (FileStream OutFileDecompressed = new FileStream(WorkingFileNamePath, FileMode.Create))
                    {
                        using (var zip = new GZipStream(FileToDeCompress, CompressionMode.Decompress, true))
                        {
                            byte[] buffer = new byte[FileToDeCompress.Length];
                            while (true)
                            {
                                int count = zip.Read(buffer, 0, buffer.Length);
                                if (count != 0) OutFileDecompressed.Write(buffer, 0, count);
                                if (count != buffer.Length) break;
                            }
                        }
                        FileToDeCompress.Close();
                        OutFileDecompressed.Close();
                    }
                }

                result = RestoreTableFromFile(DestinationTableName, WorkingFileNamePath, TimeoutSeconds);
                // Cleanup files
                if (File.Exists(WorkingFileNamePath))
                {
                    File.Delete(WorkingFileNamePath);
                }
                if (File.Exists(WorkingFileNamePathCompressed))
                {
                    File.Delete(WorkingFileNamePathCompressed);
                }
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Azure.Core.ConnectionString.Validate"))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }
                else
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                }
            }
            finally
            {
            }
            return result;
        }


        /// <summary>
        /// Restore file created by BackupAzureTables to the destination table name specified.
        /// </summary>
        /// <param name="DestinationTableName">Name of the Azure Table to restore to -  may be different than name backed up originally.</param>
        /// <param name="InFilePathName">Complete file name and path containing the data to be restored.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <returns>A string indicating the table restored and record count.</returns>
        public string RestoreTableFromFile(string DestinationTableName, string InFilePathName, int TimeoutSeconds = 30)
        {
            if (String.IsNullOrWhiteSpace(DestinationTableName))
            {
                throw new ParameterSpecException("DestinationTableName is missing.");
            }

            if (Path.GetFullPath(InFilePathName) != InFilePathName)
            {
                throw new ParameterSpecException(String.Format("Invalid file name/path '{0}' specified.", InFilePathName));
            }
            else
            {
                if (!File.Exists(InFilePathName))
                {
                    throw new ParameterSpecException(String.Format("File '{0}' does not exist.", InFilePathName));
                }
            }

            TableSpec footer = null;

            try
            {
                TableItem TableDest = tableServiceClient.CreateTableIfNotExists(DestinationTableName);

                DynamicTableEntityJsonSerializer serializer = new DynamicTableEntityJsonSerializer();

                bool BatchWritten = true;
                string PartitionKey = String.Empty;
                List<TableTransactionAction> Batch = new List<TableTransactionAction>();
                int BatchSize = 98;
                int BatchCount = 0;
                long TotalRecordCount = 0;

                using (StreamReader InputFileStream = new StreamReader(InFilePathName))
                {
                    string InFileLine = InputFileStream.ReadLine();
                    while (InFileLine != null)
                    {
                        if (InFileLine.Contains("ProcessingMetaData") && InFileLine.Contains("Header"))
                        {
                            System.Console.WriteLine(String.Format("Header {0}", InFileLine));
                        }
                        else if (InFileLine.Contains("ProcessingMetaData") && InFileLine.Contains("Footer"))
                        {
                            footer = System.Text.Json.JsonSerializer.Deserialize<TableSpec>(InFileLine);
                            System.Console.WriteLine(String.Format("Footer {0}", InFileLine));
                        }
                        else
                        {
                            //TableEntity dte2 = JsonConvert.DeserializeObject<TableEntity>(InFileLine);
                            TableEntity dte2 = serializer.Deserialize(InFileLine);
                            if (String.Empty.Equals(PartitionKey)) { PartitionKey = dte2.PartitionKey; }
                            if (dte2.PartitionKey == PartitionKey)
                            {
                                Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, dte2));
                                BatchCount++;
                                TotalRecordCount++;
                                BatchWritten = false;
                            }
                            else
                            {
                                try
                                {
                                    Response<IReadOnlyList<Response>> response = tableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                                    Batch = new List<TableTransactionAction>();
                                    PartitionKey = dte2.PartitionKey;
                                    Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, dte2));
                                    BatchCount = 1;
                                    TotalRecordCount++;
                                    BatchWritten = false;
                                }
                                catch (Exception ex)
                                {
                                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                                }
                            }
                            if (BatchCount >= BatchSize)
                            {
                                try
                                {
                                    Response<IReadOnlyList<Response>> response = tableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                                    PartitionKey = String.Empty;
                                    Batch = new List<TableTransactionAction>();
                                    BatchWritten = true;
                                    BatchCount = 0;
                                }
                                catch (Exception ex)
                                {
                                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                                }
                            }
                        }
                        InFileLine = InputFileStream.ReadLine();
                    }  // while (InFileLine != null)

                    //final batch
                    if (!BatchWritten)
                    {
                        try
                        {
                            //TableDest.ExecuteBatch(Batch);
                            Response<IReadOnlyList<Response>> response = tableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                            PartitionKey = String.Empty;
                        }
                        catch (Exception ex)
                        {
                            throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                        }
                    }
                } // using (StreamReader

                if (null == footer)
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed, no footer record found.", DestinationTableName));
                }
                else if (TotalRecordCount == footer.RecordCount)
                {
                    //OK, do nothing
                }
                else
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed, records read {1} does not match expected count {2} in footer record.", DestinationTableName, TotalRecordCount, footer.RecordCount));
                }

                return String.Format("Restore to table '{0}' Successful; {1} entries.", DestinationTableName, TotalRecordCount);
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (RestoreFailedException rex)
            {
                throw rex;
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Azure.Core.ConnectionString.Validate"))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }
                else
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                }
            }
            finally
            {
            }
        }


        /*
         * BlobRoot = blob root name
         * BlobDirectoryReference = "directory name"
         * BlockBlobRef = "File name"
         * 
         */
        private void DownloadFileFromBlob(string BlobRoot, string BlobDirectoryReference, string BlockBlobRef, string LocalFileName)
        {
            try
            {
                BlobContainerClient container = blobServiceClient.GetBlobContainerClient(BlobRoot);
                container.CreateIfNotExists();

                BlobClient blobClient = container.GetBlobClient($"{BlobDirectoryReference}/{BlockBlobRef}");
                blobClient.DownloadTo(LocalFileName);


            }
            catch (Exception ex)
            {
                throw new AzureTableBackupException(String.Format("Error downloading file '{0}'.", LocalFileName), ex);
            }
            finally
            {
            }
        }


        /// <summary>
        /// Restore file from blob storage by BackupAzureTables to the destination table name specified.  
        /// File will be read directly from blob storage.  If the file is compressed, it will be decompressed to blob storage and then read.
        /// </summary>
        /// <param name="DestinationTableName">Name of the Azure Table to restore to -  may be different than name backed up originally.</param>
        /// <param name="OriginalTableName">Name of the Azure Table originally backed (required for determining blob directory to use)</param>
        /// <param name="BlobRoot">Name to use as blob root folder.</param>
        /// <param name="BlobFileName">Name of the blob file to restore.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <returns>A string indicating the table restored and record count.</returns>
        public string RestoreTableFromBlobDirect(string DestinationTableName, string OriginalTableName, string BlobRoot, string BlobFileName, int TimeoutSeconds = 30)
        {
            if (String.IsNullOrWhiteSpace(DestinationTableName))
            {
                throw new ParameterSpecException("DestinationTableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(OriginalTableName))
            {
                throw new ParameterSpecException("OriginalTableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(BlobFileName))
            {
                throw new ParameterSpecException(String.Format("Invalid BlobFileName '{0}' specified.", BlobFileName));
            }
            bool Decompress = BlobFileName.EndsWith(".7z");
            string TempFileName = String.Format("{0}.temp", BlobFileName);

            if (String.IsNullOrWhiteSpace(BlobRoot))
            {
                throw new ParameterSpecException(String.Format("Invalid BlobRoot '{0}' specified.", BlobRoot));
            }

            try
            {

                BlobContainerClient container = blobServiceClient.GetBlobContainerClient(BlobRoot);
                container.CreateIfNotExists();
                BlobClient blobClient = container.GetBlobClient($"{BlobRoot.ToLower()}-table-{OriginalTableName.ToLower()}/{BlobFileName}");

                // If file is compressed, Decompress to a temp file in the blob
                if (Decompress)
                {
                    BlobClient blobClientTemp = container.GetBlobClient($"{BlobRoot.ToLower()}-table-{OriginalTableName.ToLower()}/{TempFileName}");
                    BlobClient blobClientRead = container.GetBlobClient($"{BlobRoot.ToLower()}-table-{OriginalTableName.ToLower()}/{BlobFileName}");

                    using (var decompressedStream = blobClientTemp.OpenWrite(true))
                    {
                        using (var readStream = blobClientRead.OpenRead())
                        {
                            using (var zip = new GZipStream(readStream, CompressionMode.Decompress, true))
                            {
                                zip.CopyTo(decompressedStream);
                            }
                        }
                    }
                    BlobFileName = TempFileName;
                }

                blobClient = container.GetBlobClient($"{BlobRoot.ToLower()}-table-{OriginalTableName.ToLower()}/{BlobFileName}");


                TableItem TableDest = tableServiceClient.CreateTableIfNotExists(DestinationTableName);

                using (Stream blobStream = blobClient.OpenRead())
                {
                    using (StreamReader inputFileStream = new StreamReader(blobStream))
                    {
                        string result = RestoreFromStream(inputFileStream, tableServiceClient, DestinationTableName);
                        if (Decompress)
                        {
                            BlobClient blobClientTemp = container.GetBlobClient($"{BlobRoot.ToLower()}-table-{OriginalTableName.ToLower()}/{TempFileName}");
                            blobClientTemp.DeleteIfExists();
                        }
                        return result;
                    }
                }

            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (RestoreFailedException rex)
            {
                throw rex;
            }
            catch (Exception ex)
            {
                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
            }
            finally
            {
            }
        } // RestoreTableFromBlobDirect


        private string RestoreFromStream(StreamReader InputFileStream, TableServiceClient TableDest, string DestinationTableName)
        {
            bool BatchWritten = true;
            string PartitionKey = String.Empty;
            List<TableTransactionAction> Batch = new List<TableTransactionAction>();
            int BatchSize = 100;
            int BatchCount = 0;
            long TotalRecordCount = 0;
            TableSpec footer = null;
            DynamicTableEntityJsonSerializer serializer = new DynamicTableEntityJsonSerializer();

            try
            {
                string InFileLine = InputFileStream.ReadLine();
                while (InFileLine != null)
                {
                    if (InFileLine.Contains("ProcessingMetaData") && InFileLine.Contains("Header"))
                    {
                        System.Console.WriteLine(String.Format("Header {0}", InFileLine));
                    }
                    else if (InFileLine.Contains("ProcessingMetaData") && InFileLine.Contains("Footer"))
                    {
                        footer = System.Text.Json.JsonSerializer.Deserialize<TableSpec>(InFileLine);
                        System.Console.WriteLine(String.Format("Footer {0}", InFileLine));
                    }
                    else
                    {
                        //TableEntity dte2 = JsonConvert.DeserializeObject<TableEntity>(InFileLine);
                        TableEntity dte2 = serializer.Deserialize(InFileLine);
                        if (String.Empty.Equals(PartitionKey)) { PartitionKey = dte2.PartitionKey; }
                        if (dte2.PartitionKey == PartitionKey)
                        {
                            Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, dte2));
                            BatchCount++;
                            TotalRecordCount++;
                            BatchWritten = false;
                        }
                        else
                        {
                            try
                            {
                                Response<IReadOnlyList<Response>> response = TableDest.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                                Batch = new List<TableTransactionAction>();
                                PartitionKey = dte2.PartitionKey;
                                Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, dte2));
                                BatchCount = 1;
                                TotalRecordCount++;
                                BatchWritten = false;
                            }
                            catch (Exception ex)
                            {
                                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                            }
                        }
                        if (BatchCount >= BatchSize)
                        {
                            try
                            {
                                Response<IReadOnlyList<Response>> response = TableDest.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                                PartitionKey = String.Empty;
                                Batch = new List<TableTransactionAction>();
                                BatchWritten = true;
                                BatchCount = 0;
                            }
                            catch (Exception ex)
                            {
                                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                            }
                        }
                    }
                    InFileLine = InputFileStream.ReadLine();
                }  // while (InFileLine != null)

                //final batch
                if (!BatchWritten)
                {
                    try
                    {
                        //TableDest.ExecuteBatch(Batch);
                        Response<IReadOnlyList<Response>> response = TableDest.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                        PartitionKey = String.Empty;
                    }
                    catch (Exception ex)
                    {
                        throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                    }
                }

                if (null == footer)
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed, no footer record found.", DestinationTableName));
                }
                else if (TotalRecordCount == footer.RecordCount)
                {
                    //OK, do nothing
                }
                else
                {
                    throw new RestoreFailedException(String.Format("Table '{0}' restore failed, records read {1} does not match expected count {2} in footer record.", DestinationTableName, TotalRecordCount, footer.RecordCount));
                }
            }
            catch (Exception ex)
            {
                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
            }

            return String.Format("Restore to table '{0}' Successful; {1} entries.", DestinationTableName, TotalRecordCount);
        }

        public string RestoreAllTablesFromBlob(string blobRoot, string folder)
        {
            var container = blobServiceClient.GetBlobContainerClient(blobRoot);
            if (!container.Exists())
                throw new RestoreFailedException(String.Format("Blob container '{0}' does not exist.", blobRoot));

            var restoreResults = new StringBuilder();
            foreach (BlobHierarchyItem blobItem in container.GetBlobsByHierarchy(prefix: folder, delimiter: "/"))
            {
                if (blobItem.IsBlob)
                {
                    var tableName = blobItem.Blob.Name.Split('.').First();
                    var decompress = blobItem.Blob.Name.EndsWith(".7z");
                    var tempFileName = String.Format("{0}.temp", blobItem.Blob.Name);

                    var blobClient = container.GetBlobClient(blobItem.Blob.Name);

                    // If file is compressed, Decompress to a temp file in the blob
                    if (decompress)
                    {
                        var blobClientTemp = container.GetBlobClient(tempFileName);

                        using (var decompressedStream = blobClientTemp.OpenWrite(true))
                        using (var readStream = blobClient.OpenRead())
                        using (var zip = new GZipStream(readStream, CompressionMode.Decompress, true))
                        {
                            zip.CopyTo(decompressedStream);
                        }
                        blobClient = container.GetBlobClient(tempFileName);
                    }

                    var TableDest = tableServiceClient.CreateTableIfNotExists(tableName);

                    using (Stream blobStream = blobClient.OpenRead())
                    using (StreamReader inputFileStream = new StreamReader(blobStream))
                    {
                        restoreResults.AppendLine(RestoreFromStream(inputFileStream, tableServiceClient, tableName));
                        if (decompress)
                        {
                            var blobClientTemp = container.GetBlobClient(tempFileName);
                            blobClientTemp.DeleteIfExists();
                        }
                    }
                }
            }
            return restoreResults.ToString();
        }
    }
}
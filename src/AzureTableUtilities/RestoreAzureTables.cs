using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Security;

using AZStorage = Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using AZBlob = Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Core;
using Microsoft.Azure.Storage.File;

using Cosmos = Microsoft.Azure.Cosmos;
using CosmosTable = Microsoft.Azure.Cosmos.Table;

using Newtonsoft.Json;

using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Restore local file or blob file to Azure Table.
    /// </summary>
    public class RestoreAzureTables
    {
        //private static string ThisClassName = "RestoreAzureTables";

        private SecureString AzureTableConnectionSpec = new SecureString();
        private SecureString AzureBlobConnectionSpec = new SecureString();

        /// <summary>
        /// Constructor, sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"</param>
        public RestoreAzureTables(string AzureConnection) : this(AzureConnection, AzureConnection)
        {

        }

        /// <summary>
        /// Constructor, sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection"></param>
        public RestoreAzureTables(SecureString AzureConnection) : this(AzureConnection, AzureConnection)
        {

        }

        /// <summary>
        /// Constructor, allows a different connection spec for Azure Table and Azure Blob storage.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"</param>
        /// <param name="AzureBlobConnection">Connection string for Azure Blob Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"</param>
        public RestoreAzureTables(string AzureTableConnection, string AzureBlobConnection)
        {
            if (String.IsNullOrEmpty(AzureTableConnection) || String.IsNullOrEmpty(AzureBlobConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            foreach (char c in AzureTableConnection.ToCharArray())
            {
                AzureTableConnectionSpec.AppendChar(c);
            }
            foreach (char c in AzureBlobConnection.ToCharArray())
            {
                AzureBlobConnectionSpec.AppendChar(c);
            }
        }

        /// <summary>
        /// Constructor, accepts SecureString and allows a different connection spec for Azure Table and Azure Blob storage.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection as a SecureString</param>
        /// <param name="AzureBlobConnection">Connection string for Azure Blob Connection as a SecureString</param>
        public RestoreAzureTables(SecureString AzureTableConnection, SecureString AzureBlobConnection)
        {
            if (Helper.IsSecureStringNullOrEmpty(AzureTableConnection) || Helper.IsSecureStringNullOrEmpty(AzureBlobConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            AzureTableConnectionSpec = AzureTableConnection;
            AzureBlobConnectionSpec = AzureBlobConnection;
        }


        /// <summary>
        /// Restore file created in blob storage by BackupAzureTables to the destination table name specified.
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

            if (!AZStorage.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureBlobConnectionSpec).Password, out AZStorage.CloudStorageAccount StorageAccountAZ))
            {
                throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
            }

            try
            { 
                AZBlob.CloudBlobClient ClientBlob = AZBlob.BlobAccountExtensions.CreateCloudBlobClient(StorageAccountAZ);
                var container = ClientBlob.GetContainerReference(BlobRoot);
                container.CreateIfNotExists();
                AZBlob.CloudBlobDirectory directory = container.GetDirectoryReference(BlobRoot.ToLower() + "-table-" + OriginalTableName.ToLower());

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

                AZBlob.CloudBlockBlob BlobBlock = directory.GetBlockBlobReference(BlobFileName);
                BlobBlock.DownloadToFile(WorkingFileNamePathCompressed, FileMode.Create);

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
                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
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
                if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureTableConnectionSpec).Password, out CosmosTable.CloudStorageAccount StorageAccount))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }

                CosmosTable.CloudTableClient client = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccount, new CosmosTable.TableClientConfiguration());
                CosmosTable.CloudTable TableDest = client.GetTableReference(DestinationTableName);
                TableDest.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);
                TableDest.CreateIfNotExists();

                DynamicTableEntityJsonSerializer serializer = new DynamicTableEntityJsonSerializer();

                bool BatchWritten = true;
                string PartitionKey = String.Empty;
                CosmosTable.TableBatchOperation Batch = new CosmosTable.TableBatchOperation();
                int BatchSize = 100;
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
                            footer = JsonConvert.DeserializeObject<TableSpec>(InFileLine);
                            System.Console.WriteLine(String.Format("Footer {0}", InFileLine));
                        }
                        else
                        {
                            CosmosTable.DynamicTableEntity dte2 = serializer.Deserialize(InFileLine);
                            if (String.Empty.Equals(PartitionKey)) { PartitionKey = dte2.PartitionKey; }
                            if (dte2.PartitionKey == PartitionKey)
                            {
                                Batch.InsertOrReplace(dte2);
                                BatchCount++;
                                TotalRecordCount++;
                                BatchWritten = false;
                            }
                            else
                            {
                                try
                                {
                                    TableDest.ExecuteBatch(Batch);
                                    Batch = new CosmosTable.TableBatchOperation();
                                    PartitionKey = dte2.PartitionKey;
                                    Batch.InsertOrReplace(dte2);
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
                                    TableDest.ExecuteBatch(Batch);
                                    PartitionKey = String.Empty;
                                    Batch = new CosmosTable.TableBatchOperation();
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
                            TableDest.ExecuteBatch(Batch);
                            PartitionKey = String.Empty;
                        }
                        catch (Exception ex) {
                            throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
                        }
                    }
                } // using (StreamReader

                if (null==footer)
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
                throw new RestoreFailedException(String.Format("Table '{0}' restore failed.", DestinationTableName), ex);
            }
            finally
            {
            }
        }


        /*
         * BlobRoot = blobl root name
         * BlobDirectoryReference = "directory name"
         * BlockBlobRef = "File name"
         * 
         */
        private void DownloadFileFromBlob(string BlobRoot, string BlobDirectoryReference, string BlockBlobRef, string LocalFileName)
        {
            try
            {
                if (!AZStorage.CloudStorageAccount.TryParse(ConfigurationManager.ConnectionStrings["AzureBlobStorageConfigConnection"].ConnectionString, out AZStorage.CloudStorageAccount StorageAccountAZ))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }

                AZBlob.CloudBlobClient ClientBlob = AZBlob.BlobAccountExtensions.CreateCloudBlobClient(StorageAccountAZ);

                var container = ClientBlob.GetContainerReference(BlobRoot);
                container.CreateIfNotExists();
                AZBlob.CloudBlobDirectory directory = container.GetDirectoryReference(BlobDirectoryReference);

                AZBlob.CloudBlockBlob BlobBlock = directory.GetBlockBlobReference(BlockBlobRef);

                BlobBlock.DownloadToFile(LocalFileName, FileMode.OpenOrCreate);

            }
            catch (Exception ex)
            {
                throw new AzureTableBackupException(String.Format("Error downloading file '{0}'.", LocalFileName), ex);
            }
            finally
            {
            }
        }
    }
}
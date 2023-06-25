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
using System.Net.Http;

using AZStorage = Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using AZBlob = Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Core;
using Microsoft.Azure.Storage.File;

using AzureTables = Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure;

using Newtonsoft.Json;

using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Backup Azure Table to local file or blob storage.
    /// </summary>
    public class BackupAzureTables
    {
        //private SecureString AzureTableConnectionSpec = new SecureString();
        //private SecureString AzureBlobConnectionSpec = new SecureString();
        private string AzureTableConnectionSpec = "";
        private string AzureBlobConnectionSpec = "";

        /// <summary>
        /// Constructor, sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public BackupAzureTables(string AzureConnection) : this(AzureConnection, AzureConnection)
        {
            
        }

        /// <summary>
        /// Constructor, accepts SecureString and sets same connection spec for both the Azure Tables as well as the Azure Blob storage.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections</param>
        /*
        public BackupAzureTables(SecureString AzureConnection) : this(AzureConnection, AzureConnection)
        {

        }
        */

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

            AzureTableConnectionSpec = AzureTableConnection;
            AzureBlobConnectionSpec = AzureBlobConnection;
            /*
            foreach (char c in AzureTableConnection.ToCharArray())
            {
                AzureTableConnectionSpec.AppendChar(c);
            }
            foreach (char c in AzureBlobConnection.ToCharArray())
            {
                AzureBlobConnectionSpec.AppendChar(c);
            }
            */
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
            string OutFileName = "";
            string OutFileNamePath = "";
            int BackupsAged = 0;

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

                if (!AZStorage.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureBlobConnectionSpec).Password, out AZStorage.CloudStorageAccount StorageAccountAZ))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }

                AZBlob.CloudBlobClient ClientBlob = AZBlob.BlobAccountExtensions.CreateCloudBlobClient(StorageAccountAZ);
                var container = ClientBlob.GetContainerReference(BlobRoot);
                container.CreateIfNotExists();
                AZBlob.CloudBlobDirectory directory = container.GetDirectoryReference(BlobRoot.ToLower() + "-table-" + TableName.ToLower());

                AZBlob.CloudBlockBlob BlobBlock = directory.GetBlockBlobReference(OutFileName);
                BlobBlock.StreamWriteSizeInBytes = 1024 * 1024 * 32; //Set stream write size to 32MB
                BlobBlock.UploadFromFile(OutFileNamePath);                                

                DateTimeOffset OffsetTimeNow = System.DateTimeOffset.Now;
                DateTimeOffset OffsetTimeRetain = System.DateTimeOffset.Now.AddDays(-1 * RetentionDays);

                //Cleanup old versions
                var BlobList = directory.ListBlobs().OfType<AZBlob.CloudBlockBlob>().ToList(); ;
                foreach (var blob in BlobList)
                {
                    if (blob.Properties.Created < OffsetTimeRetain)
                    {
                        try
                        {
                            blob.Delete();
                            BackupsAged++;
                        }
                        catch (Exception ex)
                        {
                            throw new AgingException(String.Format("Error aging file '{0}'.", blob.Name), ex);
                        }
                    }
                }

                return String.Format("Table '{0}' backed up as '{2}' under blob '{3}\\{4}'; {1} files aged.", TableName, BackupsAged, OutFileName, BlobRoot , directory.ToString());
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

                AzureTables.TableServiceClient clientSource = new AzureTables.TableServiceClient(AzureTableConnectionSpec.ToString());

                Pageable<AzureTables.TableEntity> queryResultsFilter = clientSource.GetTableClient(TableName).Query<AzureTables.TableEntity>(filter: Filter.BuildFilterSpec(filters), maxPerPage: 100);

                using (StreamWriter OutFile = new StreamWriter(OutFileNamePath))
                {
                    OutFile.WriteLine(JsonConvert.SerializeObject(TableSpecStart));

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
                    OutFile.WriteLine(JsonConvert.SerializeObject(TableSpecEnd));

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

                        TableSpec footer= JsonConvert.DeserializeObject<TableSpec>(FooterRec);
                        if ((footer.RecordCount==InRecords) && (footer.TableName.Equals(TableName)))
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
        /// <param name="TableName">Name of Azure Table to backup.</param>
        /// <param name="BlobRoot">Name to use as blob root folder.</param>
        /// <param name="Compress">True to compress the file.</param>
        /// <param name="RetentionDays">Process will age files in blob created more than x days ago.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param>
        /// <returns>A string containing the name of the file created.</returns>
        public string BackupTableToBlobDirect(string TableName, string BlobRoot, bool Compress = false, int RetentionDays = 30, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            string OutFileName = "";
            int RecordCount = 0;
            int BackupsAged = 0;

            if (String.IsNullOrWhiteSpace(TableName))
            {
                throw new ParameterSpecException("TableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(BlobRoot))
            {
                throw new ParameterSpecException("BlobRoot is missing.");
            }

            try
            {
                if (Compress)
                {
                    OutFileName = String.Format(TableName + "_Backup_" + System.DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt.7z");                    
                }
                else
                {
                    OutFileName = String.Format(TableName + "_Backup_" + System.DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt");
                }

                if (!AZStorage.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureBlobConnectionSpec).Password, out AZStorage.CloudStorageAccount StorageAccountAZ))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }

                AZBlob.CloudBlobClient ClientBlob = AZBlob.BlobAccountExtensions.CreateCloudBlobClient(StorageAccountAZ);
                var container = ClientBlob.GetContainerReference(BlobRoot);
                container.CreateIfNotExists();
                AZBlob.CloudBlobDirectory directory = container.GetDirectoryReference(BlobRoot.ToLower() + "-table-" + TableName.ToLower());

                AZBlob.CloudBlockBlob BlobBlock = directory.GetBlockBlobReference(OutFileName);
                BlobBlock.StreamWriteSizeInBytes = 1024 * 1024 * 32; //Set stream write size to 32MB

                // start upload from stream, iterate through table, possible inline compress
                try
                {
                    AzureTables.TableServiceClient clientSource = new AzureTables.TableServiceClient(AzureTableConnectionSpec.ToString());

                    //TODO  Timeout set?
                    //table.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);

                    var entitiesSerialized = new List<string>();
                    DynamicTableEntityJsonSerializer serializer = new DynamicTableEntityJsonSerializer();

                    TableSpec TableSpecStart = new TableSpec(TableName);
                    var NewLineAsBytes = Encoding.UTF8.GetBytes("\n");

                    var tempTableSpecStart = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(TableSpecStart));
                    AZBlob.CloudBlobStream bs2 = BlobBlock.OpenWrite();
                    Stream bs = BlobBlock.OpenWrite();

                    if (Compress)
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

                    Pageable<AzureTables.TableEntity> queryResultsFilter = clientSource.GetTableClient(TableName).Query<AzureTables.TableEntity>(filter: Filter.BuildFilterSpec(filters), maxPerPage: 100);
                    foreach (Page<AzureTables.TableEntity> page in queryResultsFilter.AsPages())
                    {
                        foreach (AzureTables.TableEntity qEntity in page.Values)
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
                    TableSpec TableSpecEnd = new TableSpec(TableName, RecordCount);
                    var tempTableSpecEnd = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(TableSpecEnd));
                    bs.Write(tempTableSpecEnd, 0, tempTableSpecEnd.Length);
                    bs.Flush();
                    bs.Write(NewLineAsBytes, 0, NewLineAsBytes.Length);
                    bs.Flush();
                    bs.Close();                    
                }
                catch (Exception ex)
                {
                    throw new BackupFailedException(String.Format("Table '{0}' backup failed.", TableName), ex);
                }

                DateTimeOffset OffsetTimeNow = System.DateTimeOffset.Now;
                DateTimeOffset OffsetTimeRetain = System.DateTimeOffset.Now.AddDays(-1 * RetentionDays);

                //Cleanup old versions
                var BlobList = directory.ListBlobs().OfType<AZBlob.CloudBlockBlob>().ToList(); ;
                foreach (var blob in BlobList)
                {
                    if (blob.Properties.Created < OffsetTimeRetain)
                    {
                        try
                        {
                            blob.Delete();
                            BackupsAged++;
                        }
                        catch (Exception ex)
                        {
                            throw new AgingException(String.Format("Error aging file '{0}'.", blob.Name), ex);
                        }
                    }
                }

                return String.Format("Table '{0}' backed up as '{2}' under blob '{3}\\{4}'; {1} files aged.", TableName, BackupsAged, OutFileName, BlobRoot, directory.ToString());
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
                List<string> TableNames = Helper.GetTableNames(AzureTableConnectionSpec);
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
    }
}

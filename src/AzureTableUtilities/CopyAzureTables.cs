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

using Cosmos = Microsoft.Azure.Cosmos;
using CosmosTable = Microsoft.Azure.Cosmos.Table;

using Newtonsoft.Json;

using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Azure Tables copy functions.
    /// </summary>
    public class CopyAzureTables
    {
        private SecureString AzureSourceTableConnection = new SecureString();
        private SecureString AzureDestinationTableConnection = new SecureString();

        /// <summary>
        /// Constructor, sets same connection spec for both the Source and Destination Azure Tables.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public CopyAzureTables(string AzureConnection) : this(AzureConnection, AzureConnection)
        {

        }

        /// <summary>
        /// Constructor, accepts SecureString and sets same connection spec for both the Source and Destination Azure Tables.
        /// </summary>
        /// <param name="AzureConnection">Connection string for Azure Table and Blob Connections</param>
        public CopyAzureTables(SecureString AzureConnection) : this(AzureConnection, AzureConnection)
        {

        }

        /// <summary>
        /// Constructor, allows a different connection spec for Source and Destination Azure Table.
        /// </summary>
        /// <param name="AzureSourceTableConnection">Connection string for Azure Table Connection; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        /// <param name="AzureDestinationTableConnection">Connection string for Azure Table Connection to be used as destination of Copy command; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;" </param>
        public CopyAzureTables(string AzureSourceTableConnection, string AzureDestinationTableConnection)
        {
            if (String.IsNullOrEmpty(AzureSourceTableConnection) || String.IsNullOrEmpty(AzureDestinationTableConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            foreach (char c in AzureSourceTableConnection.ToCharArray())
            {
                this.AzureSourceTableConnection.AppendChar(c);
            }
            foreach (char c in AzureDestinationTableConnection.ToCharArray())
            {
                this.AzureDestinationTableConnection.AppendChar(c);
            }
        }

        /// <summary>
        /// Constructor, accepts SecureString and allows a different connection spec for Source and Destination Azure Table.
        /// </summary>
        /// <param name="AzureSourceTableConnection">Connection string for Azure Table Connection as a SecureString</param>
        /// <param name="AzureDestinationTableConnection">Connection string for Azure Table Connection to be used as destination of Copy command as a SecureString</param>
        public CopyAzureTables(SecureString AzureSourceTableConnection, SecureString AzureDestinationTableConnection)
        {
            if (Helper.IsSecureStringNullOrEmpty(AzureSourceTableConnection) || Helper.IsSecureStringNullOrEmpty(AzureDestinationTableConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            this.AzureSourceTableConnection = AzureSourceTableConnection;
            this.AzureDestinationTableConnection = AzureDestinationTableConnection;
        }


        /// <summary>
        /// This method will copy entries from SourceTableName to DestinationTableName.  The destination table is NOT deleted first.
        /// </summary>
        /// <param name="SourceTableName">Name of table to copy from.</param>
        /// <param name="DestinationTableName">Name of table to copy to.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values extracted.</param> 
        /// <returns>A string indicating the result of the operation.</returns>
        public string CopyTableToTable(string SourceTableName, string DestinationTableName, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            if (String.IsNullOrWhiteSpace(SourceTableName))
            {
                throw new ParameterSpecException("SourceTableName is missing.");
            }

            if (String.IsNullOrWhiteSpace(DestinationTableName))
            {
                throw new ParameterSpecException("DestinationTableName is missing.");
            }

            if (!Filter.AreFiltersValid(filters))
            {
                throw new ParameterSpecException(String.Format("One or more of the supplied filter criteria is invalid."));
            }

            try
            {
                if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureSourceTableConnection).Password, out CosmosTable.CloudStorageAccount StorageAccountSource))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account for Source Table.  Verify connection string.");
                }

                CosmosTable.CloudTableClient clientSource = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccountSource, new CosmosTable.TableClientConfiguration());
                CosmosTable.CloudTable TableSource = clientSource.GetTableReference(SourceTableName);
                TableSource.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);

                if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureDestinationTableConnection).Password, out CosmosTable.CloudStorageAccount StorageAccountDest))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account for Destination Table.  Verify connection string.");
                }

                CosmosTable.CloudTableClient clientDestnation = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccountDest, new CosmosTable.TableClientConfiguration());
                CosmosTable.CloudTable TableDest = clientDestnation.GetTableReference(DestinationTableName);
                TableDest.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);
                TableDest.CreateIfNotExists();

                bool BatchWritten = true;
                string PartitionKey = String.Empty;
                CosmosTable.TableBatchOperation Batch = new CosmosTable.TableBatchOperation();
                int BatchSize = 100;
                int BatchCount = 0;
                long TotalRecordCountIn = 0;
                long TotalRecordCountOut = 0;
                CosmosTable.TableContinuationToken token = null;

                do
                {
                    CosmosTable.TableQuery<CosmosTable.DynamicTableEntity> tq;
                    if (default(List<Filter>) == filters)
                    {
                        tq = new CosmosTable.TableQuery<CosmosTable.DynamicTableEntity>();

                    }
                    else
                    {
                        tq = new CosmosTable.TableQuery<CosmosTable.DynamicTableEntity>().Where(Filter.BuildFilterSpec(filters));
                    }
                    var queryResult = TableSource.ExecuteQuerySegmented(tq, token);

                    foreach (CosmosTable.DynamicTableEntity dte in queryResult.Results)
                    {
                        TotalRecordCountIn++;
                        if (String.Empty.Equals(PartitionKey)) { PartitionKey = dte.PartitionKey; }
                        if (dte.PartitionKey == PartitionKey)
                        {
                            Batch.InsertOrReplace(dte);
                            BatchCount++;
                            TotalRecordCountOut++;
                            BatchWritten = false;
                        }
                        else
                        {
                            try
                            {
                                TableDest.ExecuteBatch(Batch);
                                Batch = new CosmosTable.TableBatchOperation();
                                PartitionKey = dte.PartitionKey;
                                Batch.InsertOrReplace(dte);
                                BatchCount = 1;
                                TotalRecordCountOut++;
                                BatchWritten = false;
                            }
                            catch (Exception ex)
                            {
                                throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}' failed.", SourceTableName, DestinationTableName), ex);
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
                                throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}' failed.", SourceTableName, DestinationTableName), ex);
                            }
                        }
                    }
                    token = queryResult.ContinuationToken;
                } while (token != null);

                if (!BatchWritten)
                {
                    try
                    {
                        TableDest.ExecuteBatch(Batch);
                        PartitionKey = String.Empty;
                    }
                    catch (Exception ex)
                    {
                        throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}'  failed.", SourceTableName, DestinationTableName), ex);
                    }
                }
                return String.Format("Table '{0}' copied to table '{1}', total records {2}.", SourceTableName, DestinationTableName, TotalRecordCountIn);
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}' failed.", SourceTableName, DestinationTableName), ex);
            }
            finally
            {
            }
        }
    }
}
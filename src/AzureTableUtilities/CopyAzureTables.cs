using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Azure Tables copy functions.
    /// </summary>
    public class CopyAzureTables
    {

        private TableServiceClient sourceTableServiceClient;
        private TableServiceClient destinationTableServiceClient;
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

            sourceTableServiceClient = new TableServiceClient(AzureSourceTableConnection);
            destinationTableServiceClient = new TableServiceClient(AzureDestinationTableConnection);
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

            //this.AzureSourceTableConnection = AzureSourceTableConnection;
            //this.AzureDestinationTableConnection = AzureDestinationTableConnection;
        }


        /// <summary>
        /// This method will copy entries from SourceTableName to DestinationTableName.  The destination table is NOT deleted first.
        /// </summary>
        /// <param name="SourceTableName">Name of table to copy from.</param>
        /// <param name="DestinationTableName">Name of table to copy to.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values copied.</param> 
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

            int BatchSize = 98;
            int BatchCount = 0;
            long TotalRecordCountIn = 0;
            long TotalRecordCountOut = 0;

            bool BatchWritten = true;
            string PartitionKey = String.Empty;
            //CosmosTable.TableBatchOperation Batch = new CosmosTable.TableBatchOperation();
            List<TableTransactionAction> Batch = new List<TableTransactionAction>();

            try
            {
                TableItem TableDest = destinationTableServiceClient.CreateTableIfNotExists(DestinationTableName);

                Pageable<TableEntity> queryResultsFilter = sourceTableServiceClient.GetTableClient(SourceTableName).Query<TableEntity>(filter: Filter.BuildFilterSpec(filters), maxPerPage: BatchSize);
                // Set Timeout?

                BatchCount = 0;
                foreach (Page<TableEntity> page in queryResultsFilter.AsPages())
                {
                    foreach (TableEntity qEntity in page.Values)
                    {
                        // Build a batch to insert
                        if (String.Empty.Equals(PartitionKey)) { PartitionKey = qEntity.PartitionKey; }
                        if (qEntity.PartitionKey == PartitionKey)
                        {
                            Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, qEntity));
                            BatchCount++;
                            TotalRecordCountIn++;
                            BatchWritten = false;
                        }
                        else
                        {
                            Response<IReadOnlyList<Response>> response = destinationTableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                            TotalRecordCountOut = TotalRecordCountOut + Batch.Count;
                            Batch = new List<TableTransactionAction>();

                            PartitionKey = qEntity.PartitionKey;
                            Batch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, qEntity));
                            BatchCount = 1;
                            TotalRecordCountIn++;
                            BatchWritten = false;
                        }
                        if (BatchCount >= BatchSize)
                        {
                            try
                            {
                                Response<IReadOnlyList<Response>> response = destinationTableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                                TotalRecordCountOut = TotalRecordCountOut + Batch.Count;

                                Batch = new List<TableTransactionAction>();
                                PartitionKey = String.Empty;
                                BatchWritten = true;
                                BatchCount = 0;
                            }
                            catch (Exception ex)
                            {
                                throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}' failed.", SourceTableName, DestinationTableName), ex);
                            }
                        }
                    }
                }
                if (!BatchWritten)
                {
                    Response<IReadOnlyList<Response>> response = destinationTableServiceClient.GetTableClient(DestinationTableName).SubmitTransaction(Batch);
                    TotalRecordCountOut = TotalRecordCountOut + Batch.Count;
                    Batch = new List<TableTransactionAction>();
                    PartitionKey = String.Empty;
                    BatchCount = 0;
                    BatchWritten = true;
                }
                return String.Format("Table '{0}' copied to table '{1}', total records {2}.", SourceTableName, DestinationTableName, TotalRecordCountIn);
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
                    throw new CopyFailedException(String.Format("Table '{0}' copy to '{1}' failed.", SourceTableName, DestinationTableName), ex);
                }
            }
            finally
            {
            }
        }


        /// <summary>
        /// This method will copy all tables from entries from AzureSourceTableConnection to AzureDestinationTableConnection.  The destination table is NOT deleted first.
        /// </summary>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table values copied.</param> 
        /// <returns>A string indicating the result of the operation.</returns>
        public string CopyAllTables(int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            //if (IsEqualTo(AzureSourceTableConnection.ToString(), AzureDestinationTableConnection.ToString()))
            if (sourceTableServiceClient.Uri.Equals(destinationTableServiceClient.Uri))
            {
                throw new ParameterSpecException("Source and Destination Connection specs can not match for CopyAll.");
            }

            if (!Filter.AreFiltersValid(filters))
            {
                throw new ParameterSpecException(String.Format("One or more of the supplied filter criteria is invalid."));
            }

            StringBuilder BackupResults = new StringBuilder();
            try
            {
                List<string> TableNames = Helper.GetTableNames(sourceTableServiceClient);
                if (TableNames.Count > 0)
                {
                    foreach (string TableName in TableNames)
                    {
                        BackupResults.Append(CopyTableToTable(TableName, TableName, TimeoutSeconds, filters) + "|");
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
                throw new CopyFailedException(String.Format("Operation CopyAllTables failed."), ex);
            }
        }


        /// <summary>
        /// Test if two SecureString values are equal.
        /// </summary>
        /// <param name="ss1"></param>
        /// <param name="ss2"></param>
        /// <returns>bool</returns>
        private bool IsEqualTo(SecureString ss1, SecureString ss2)
        {
            IntPtr bstr1 = IntPtr.Zero;
            IntPtr bstr2 = IntPtr.Zero;
            try
            {
                bstr1 = Marshal.SecureStringToBSTR(ss1);
                bstr2 = Marshal.SecureStringToBSTR(ss2);
                int length1 = Marshal.ReadInt32(bstr1, -4);
                int length2 = Marshal.ReadInt32(bstr2, -4);
                if (length1 == length2)
                {
                    for (int x = 0; x < length1; ++x)
                    {
                        byte b1 = Marshal.ReadByte(bstr1, x);
                        byte b2 = Marshal.ReadByte(bstr2, x);
                        if (b1 != b2) return false;
                    }
                }
                else return false;
                return true;
            }
            finally
            {
                if (bstr2 != IntPtr.Zero) Marshal.ZeroFreeBSTR(bstr2);
                if (bstr1 != IntPtr.Zero) Marshal.ZeroFreeBSTR(bstr1);
            }
        }
    }
}
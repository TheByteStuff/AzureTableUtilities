using System;
using System.Collections.Generic;
using System.Text;
using System.Security;

using CosmosTable = Microsoft.Azure.Cosmos.Table;

using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Azure Table delete functions.
    /// </summary>
    public class DeleteAzureTables
    {
        //private static string ThisClassName = "DeleteAzureTables";

        private SecureString AzureTableConnectionSpec = new SecureString();

        /// <summary>
        /// Constructor, sets the connection spec for the Azure Tables.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table; ex "AccountName=devstoreaccount1;AccountKey={xxxxxxxxxxx};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"</param>
        public DeleteAzureTables(string AzureTableConnection)
        {
            if (String.IsNullOrEmpty(AzureTableConnection))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            foreach (char c in AzureTableConnection.ToCharArray())
            {
                AzureTableConnectionSpec.AppendChar(c);
            }
        }

        /// <summary>
        /// Constructor, sets the connection spec for the Azure Tables.
        /// </summary>
        /// <param name="AzureTableConnection">Connection string for Azure Table Connection as a SecureString</param>
        public DeleteAzureTables(SecureString AzureTableConnection)
        {
            if ((null == AzureTableConnection) || (AzureTableConnection.Length<1))
            {
                throw new ConnectionException(String.Format("Connection spec must be specified."));
            }

            AzureTableConnectionSpec = AzureTableConnection;
        }

        /// <summary>
        /// This method will delete the table name specified.  The operation will be considered successful even if the table name does not exist.
        /// </summary>
        /// <param name="TableNameToDelete">Name of table to delete.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <returns>A string indicating the result of the operation.</returns>
        public string DeleteAzureTable(string TableNameToDelete, int TimeoutSeconds = 30)
        {
            if (String.IsNullOrWhiteSpace(TableNameToDelete))
            {
                throw new ParameterSpecException("TableNameToDelete is missing.");
            }
            return DeleteAzureTableHelper(TableNameToDelete, TimeoutSeconds, false);
        }

        /// <summary>
        /// This method will delete the table name specified and then recreate it.
        /// </summary>
        /// <param name="TableNameToDelete">Name of table to delete.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <returns>A string indicating the result of the operation.</returns>
        private string DeleteAndRecreatedAzureTable(string TableNameToDelete, int TimeoutSeconds = 30)
        {
            return DeleteAzureTableHelper(TableNameToDelete, TimeoutSeconds, true);
        }

        /// <summary>
        /// This method will delete rows for the table name specified corresponding to the filter criteria.  If no filter criteria is specified, all rows will be deleted.
        /// </summary>
        /// <param name="TableNameToDelete">Name of table from entries will be deleted.</param>
        /// <param name="TimeoutSeconds">Set timeout for table client.</param>
        /// <param name="filters">A list of Filter objects to be applied to table rows to delete.</param> 
        /// <returns>A string indicating the result of the operation.</returns>
        public string DeleteAzureTableRows(string TableNameToDelete, int TimeoutSeconds = 30, List<Filter> filters = default(List<Filter>))
        {
            if (String.IsNullOrWhiteSpace(TableNameToDelete))
            {
                throw new ParameterSpecException("TableNameToDelete rows from is missing.");
            }

            if (!Filter.AreFiltersValid(filters))
            {
                throw new ParameterSpecException(String.Format("One or more of the supplied filter criteria is invalid."));
            }

            try
            {
                if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureTableConnectionSpec).Password, out CosmosTable.CloudStorageAccount StorageAccountSource))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account for Source Table.  Verify connection string.");
                }

                CosmosTable.CloudTableClient clientSource = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccountSource, new CosmosTable.TableClientConfiguration());
                CosmosTable.CloudTable TableSource = clientSource.GetTableReference(TableNameToDelete);
                TableSource.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);

                //CosmosTable.CloudTable TableDest = TableSource;

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
                            Batch.Delete(dte);
                            //Batch.InsertOrReplace(dte);
                            BatchCount++;
                            TotalRecordCountOut++;
                            BatchWritten = false;
                        }
                        else
                        {
                            try
                            {
                                TableSource.ExecuteBatch(Batch);
                                Batch = new CosmosTable.TableBatchOperation();
                                PartitionKey = dte.PartitionKey;
                                Batch.Delete(dte);
                                //Batch.InsertOrReplace(dte);
                                BatchCount = 1;
                                TotalRecordCountOut++;
                                BatchWritten = false;
                            }
                            catch (Exception ex)
                            {
                                throw new DeleteFailedException(String.Format("Table '{0}' row delete failed.", TableNameToDelete), ex);
                            }
                        }
                        if (BatchCount >= BatchSize)
                        {
                            try
                            {
                                TableSource.ExecuteBatch(Batch);
                                PartitionKey = String.Empty;
                                Batch = new CosmosTable.TableBatchOperation();
                                BatchWritten = true;
                                BatchCount = 0;
                            }
                            catch (Exception ex)
                            {
                                throw new DeleteFailedException(String.Format("Table '{0}' row deletion failed.", TableNameToDelete), ex);
                            }
                        }
                    }
                    token = queryResult.ContinuationToken;
                } while (token != null);

                if (!BatchWritten)
                {
                    try
                    {
                        TableSource.ExecuteBatch(Batch);
                        PartitionKey = String.Empty;
                    }
                    catch (Exception ex)
                    {
                        throw new DeleteFailedException(String.Format("Table '{0}' row deletion failed.", TableNameToDelete), ex);
                    }
                }
                return String.Format("Table '{0}' total rows deleted {1}.", TableNameToDelete, TotalRecordCountIn);
            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                throw new DeleteFailedException(String.Format("Table '{0}' row deletion failed.", TableNameToDelete), ex);
            }
            finally
            {
            }
        }


        private string DeleteAzureTableHelper(string TableNameToDelete, int TimeoutSeconds = 30, bool Recreate = false)
        {
            try
            {
                if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureTableConnectionSpec).Password, out CosmosTable.CloudStorageAccount StorageAccount))
                {
                    throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
                }

                CosmosTable.CloudTableClient client = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccount, new CosmosTable.TableClientConfiguration());
                CosmosTable.CloudTable TableToDelete = client.GetTableReference(TableNameToDelete);
                TableToDelete.ServiceClient.DefaultRequestOptions.ServerTimeout = new TimeSpan(0, 0, TimeoutSeconds);

                bool TableExisted = TableToDelete.DeleteIfExists();
                //TableToDelete.Delete();

                if (Recreate)
                {
                    TableToDelete.CreateIfNotExists();
                }

                if (Recreate)
                {
                    //return String.Format("Table '{0}' did not exist.", TableNameToDelete);
                    return String.Format("Table '{0}' deleted and recreated.", TableNameToDelete);
                }
                else
                {
                    return String.Format("Table '{0}' deleted.", TableNameToDelete);
                }

            }
            catch (ConnectionException cex)
            {
                throw cex;
            }
            catch (Exception ex)
            {
                throw new DeleteFailedException(String.Format("Table '{0}' delete failed.", TableNameToDelete), ex);
            }
            finally
            {
            }
        }
    }
}
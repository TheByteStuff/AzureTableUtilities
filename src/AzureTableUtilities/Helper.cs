using System;
using System.Collections.Generic;
using System.Text;
using System.Security;

using Cosmos = Microsoft.Azure.Cosmos;
using CosmosTable = Microsoft.Azure.Cosmos.Table;

using TheByteStuff.AzureTableUtilities.Exceptions;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Common method for AzureTableUtilities
    /// </summary>
    public class Helper
    {
        /// <summary>
        /// Test if SecureString is null or empty.
        /// </summary>
        /// <param name="value">Value to test</param>
        /// <returns></returns>
        public static bool IsSecureStringNullOrEmpty(SecureString value)
        {
            if ((null==value) || (value.Length==0))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Return a list of table names for the given azure connection.
        /// </summary>
        /// <param name="AzureTableConnection"></param>
        /// <returns></returns>
        public static List<string> GetTableNames(SecureString AzureTableConnection)
        {
            List<string> TableNames = new List<string>();
            if (!CosmosTable.CloudStorageAccount.TryParse(new System.Net.NetworkCredential("", AzureTableConnection).Password, out CosmosTable.CloudStorageAccount StorageAccount))
            {
                throw new ConnectionException("Can not connect to CloudStorage Account.  Verify connection string.");
            }

            CosmosTable.CloudTableClient client = CosmosTable.CloudStorageAccountExtensions.CreateCloudTableClient(StorageAccount, new CosmosTable.TableClientConfiguration());
            var result = client.ListTables();
            foreach (var Table in result)
            {
                TableNames.Add(Table.Name);
            }
            return TableNames;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using System.Security;

using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Azure;

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

        public static bool IsStringNullOrEmpty(string value)
        {
            if ((null == value) || (value.Length == 0))
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
        /// <param name="client"></param>
        /// <returns></returns>
        public static List<string> GetTableNames(TableServiceClient client)
        {
            List<string> TableNames = new List<string>();

            Pageable<TableItem> result = client.Query();
            foreach (TableItem Table in result)
            {
                TableNames.Add(Table.Name);
            }
            return TableNames;
        }
    }
}

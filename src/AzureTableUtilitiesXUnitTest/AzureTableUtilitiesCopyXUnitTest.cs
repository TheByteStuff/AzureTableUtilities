using System;
using System.IO;
using System.Reflection;
using System.Configuration;
using System.Security;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Binder;
using Microsoft.Extensions.Configuration.FileExtensions;
using Microsoft.Extensions.Configuration.Json;
using Xunit;
using System.Linq;

using TheByteStuff.AzureTableUtilities;
using TheByteStuff.AzureTableUtilities.Exceptions;

namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesCopyXUnitTest
    {
        private string AzureStorageConfigConnection = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

        private string TableNameFrom = "TestSource";
        private string TableNameTo = "TestCopy";
        private string TableNameTo2 = "TestCopy2";

        private string WorkingDirectory;
        private string FileNameThatExists;
        private string FileNamePathThatExists_UserProfile;
        private string FileNamePathThatExists_SystemLogs;

        public AzureTableUtilitiesCopyXUnitTest()
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            WorkingDirectory = Path.Combine(Path.GetDirectoryName(codeBasePath), "TestFiles");
            //WorkingDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\TestFiles";
            FileNameThatExists = @"UserProfile_Backup_GoodFooter.txt";
            FileNamePathThatExists_UserProfile = WorkingDirectory + @"\" + FileNameThatExists;

            FileNameThatExists = @"SystemLogs_ForXUnit.txt";
            FileNamePathThatExists_SystemLogs = WorkingDirectory + @"\" + FileNameThatExists;

        }

        private int ExtractNextInt(string Target, string KeyValue, string EndTag=".")
        {
            try
            {
                int start = Target.IndexOf(KeyValue) + KeyValue.Length;
                int end = Target.IndexOf(EndTag, start);
                return Int32.Parse(Target.Substring(start, end - start).Trim());
            }
            catch (Exception ) { return -99999; }
        }


        [Fact]
        public void TestCopyRowKey()
        {
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection);

            string deleteInitializeResult = instanceDelete.DeleteAzureTableRows(TableNameTo);
            string deleteInitialize2Result = instanceDelete.DeleteAzureTableRows(TableNameTo2);
            string deleteInitialize3Result = instanceDelete.DeleteAzureTableRows(TableNameFrom);

            string restoreResult = instanceRestore.RestoreTableFromFile(TableNameFrom, FileNamePathThatExists_UserProfile);
            int RestoreCount = ExtractNextInt(restoreResult, "Successful;", "entries");

            List<Filter> Filters = new List<Filter>();
            Filters.Add(new Filter("RowKey", "=", "User1"));
            Filters.Add(new Filter("RowKey", "=", "User2", "OR"));

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30, Filters);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            List<Filter> Filters2 = new List<Filter>();
            Filters2.Add(new Filter("RowKey", "=", "User1"));

            string delete2Result = instanceDelete.DeleteAzureTableRows(TableNameTo, 30, Filters2);
            int RowsDeletedCount = ExtractNextInt(delete2Result, "total rows deleted");
            Assert.True(RowsDeletedCount > 0);

            string copy2Result = instanceCopy.CopyTableToTable(TableNameTo, TableNameTo2);
            int SecondRowCount = ExtractNextInt(copy2Result, "total records");
            Assert.Equal(InitialRowCount, SecondRowCount + RowsDeletedCount);

            System.Console.WriteLine("Test");
        }


        [Fact]
        public void TestCopy()
        {
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection);

            string deleteInitializeResult = instanceDelete.DeleteAzureTableRows(TableNameTo);
            string deleteInitialize2Result = instanceDelete.DeleteAzureTableRows(TableNameTo2);
            string deleteInitialize3Result = instanceDelete.DeleteAzureTableRows(TableNameFrom);

            string restoreResult = instanceRestore.RestoreTableFromFile(TableNameFrom, FileNamePathThatExists_SystemLogs);
            int RestoreCount = ExtractNextInt(restoreResult, "Successful;", "entries");

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo);
            //pull result of rows copied    "Table 'SystemLogs' copied to table 'TestCopy', total records 370."
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            List<Filter> Filters = new List<Filter>();
            Filters.Add(new Filter("PartitionKey", "=", "SecurityLog"));

            string delete2Result = instanceDelete.DeleteAzureTableRows(TableNameTo, 30, Filters);
            int RowsDeletedCount = ExtractNextInt(delete2Result, "total rows deleted");
            Assert.True(RowsDeletedCount > 0);

            string copy2Result = instanceCopy.CopyTableToTable(TableNameTo, TableNameTo2);
            int SecondRowCount = ExtractNextInt(copy2Result, "total records");
            Assert.Equal(InitialRowCount, SecondRowCount + RowsDeletedCount);

        }

        [Fact]
        public void TestCopyParameterExceptions()
        {
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);

            var exceptionFromMissing = Assert.Throws<ParameterSpecException>(() => instanceCopy.CopyTableToTable("", TableNameTo2));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromMissing));
            Assert.Contains("SourceTableName is missing.", exceptionFromMissing.ToString());

            var exceptionToMissing = Assert.Throws<ParameterSpecException>(() => instanceCopy.CopyTableToTable(TableNameTo, ""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToMissing));
            Assert.Contains("DestinationTableName is missing.", exceptionToMissing.ToString());
        }
    }
}

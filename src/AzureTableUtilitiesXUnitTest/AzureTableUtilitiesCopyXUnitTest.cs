using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using TheByteStuff.AzureTableUtilities;
using TheByteStuff.AzureTableUtilities.Exceptions;
using Xunit;

namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesCopyXUnitTest
    {
        //* See ConnectionSpecManager for configuration information
        private string AzureStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");
        private string AzureStorageConfigConnection2 = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection2", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.2:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");

        //These need to be edited to be two different sources before running CopyAll tests.
        private string AzureStorageConfigConnection_ALT1 = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection_ALT1", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");
        private string AzureStorageConfigConnection_ALT2 = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection_ALT2", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.2:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");

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


        [Fact]
        public void TestCopyAllParameterExceptions()
        {
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);

            var exceptionFromMissing = Assert.Throws<ParameterSpecException>(() => instanceCopy.CopyAllTables());
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromMissing));
            Assert.Contains("Source and Destination Connection specs can not match for CopyAll.", exceptionFromMissing.ToString());

            instanceCopy = new CopyAzureTables(AzureStorageConfigConnection, AzureStorageConfigConnection2);
            List<Filter> Filters = new List<Filter>();
            Filters.Add(new Filter("RowKey", "=", "User1", "XXXX"));
            var exceptionToMissing = Assert.Throws<ParameterSpecException>(() => instanceCopy.CopyAllTables(30, Filters));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToMissing));
            Assert.Contains("One or more of the supplied filter criteria is invalid.", exceptionToMissing.ToString());
        }


        [Fact]
        public void TestCopyAll()
        {
            DeleteAzureTables instanceDeleteSetup = new DeleteAzureTables(AzureStorageConfigConnection_ALT1);
            RestoreAzureTables instanceRestoreSetup = new RestoreAzureTables(AzureStorageConfigConnection_ALT1);
            string deleteInitializeResult = instanceDeleteSetup.DeleteAzureTableRows(TableNameTo);
            string deleteInitialize2Result = instanceDeleteSetup.DeleteAzureTableRows(TableNameTo2);

            DeleteAzureTables instanceDeleteSetup2 = new DeleteAzureTables(AzureStorageConfigConnection_ALT2);
            string deleteInitializeResult2 = instanceDeleteSetup2.DeleteAzureTableRows(TableNameTo);
            string deleteInitialize2Result2 = instanceDeleteSetup2.DeleteAzureTableRows(TableNameTo2);

            string restoreResult1 = instanceRestoreSetup.RestoreTableFromFile(TableNameTo, FileNamePathThatExists_UserProfile);
            int RestoreCount1 = ExtractNextInt(restoreResult1, "Successful;", "entries");

            string restoreResult2 = instanceRestoreSetup.RestoreTableFromFile(TableNameTo2, FileNamePathThatExists_SystemLogs);
            int RestoreCount2 = ExtractNextInt(restoreResult2, "Successful;", "entries");

            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection_ALT1, AzureStorageConfigConnection_ALT2);

            // Check for expected results on the two tables set up. Actual results may have more than these two tables depending on environment.
            string Expected1 = String.Format("Table '{0}' copied to table '{0}', total records {1}.", TableNameTo, RestoreCount1);
            string Expected2 = String.Format("Table '{0}' copied to table '{0}', total records {1}.", TableNameTo2, RestoreCount2);
            string CopyAllResults =  instanceCopy.CopyAllTables();
            Assert.Contains(Expected1, CopyAllResults.ToString());
            Assert.Contains(Expected2, CopyAllResults.ToString());
        }
    }
}

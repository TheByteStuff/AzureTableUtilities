using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using TheByteStuff.AzureTableUtilities;
using TheByteStuff.AzureTableUtilities.Exceptions;
using Xunit;

namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesDeleteXUnitTest
    {
        //* See ConnectionSpecManager for configuration information
        private string AzureStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");

        private string TableNameFrom = "TestSource";
        private string TableNameTo = "TestCopy";
        private string TableNameTo2 = "TestCopy2";

        private string WorkingDirectory;
        private string FileNameThatExists;
        private string FileNamePathThatExists_SystemLogs_LargeFile;

        public AzureTableUtilitiesDeleteXUnitTest()
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            WorkingDirectory = Path.Combine(Path.GetDirectoryName(codeBasePath), "TestFiles");

            FileNameThatExists = @"SystemLogs_LargeFile_ForXUnit.txt";
            FileNamePathThatExists_SystemLogs_LargeFile = WorkingDirectory + @"\" + FileNameThatExists;
        }

        private void InitializeTables(DeleteAzureTables instanceDelete, RestoreAzureTables instanceRestore, string FileNamePath)
        {
            instanceDelete.DeleteAzureTableRows(TableNameTo);
            instanceDelete.DeleteAzureTableRows(TableNameTo2);
            instanceDelete.DeleteAzureTableRows(TableNameFrom);

            string restoreResult = instanceRestore.RestoreTableFromFile(TableNameFrom, FileNamePath);
            int RestoreCount = ExtractNextInt(restoreResult, "Successful;", "entries");
        }

        private int ExtractNextInt(string Target, string KeyValue, string EndTag = ".")
        {
            try
            {
                int start = Target.IndexOf(KeyValue) + KeyValue.Length;
                int end = Target.IndexOf(EndTag, start);
                return Int32.Parse(Target.Substring(start, end - start).Trim());
            }
            catch (Exception) { return -99999; }
        }

        [Fact]
        public void TestDeleteParameterExceptions()
        {
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection);

            var exception = Assert.Throws<ParameterSpecException>(() => instanceDelete.DeleteAzureTable(""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exception));
            Assert.Contains("TableNameToDelete is missing.", exception.ToString());

            var exceptionRows = Assert.Throws<ParameterSpecException>(() => instanceDelete.DeleteAzureTableRows(""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionRows));
            Assert.Contains("TableNameToDelete rows from is missing.", exceptionRows.ToString());
        }


        [Fact]
        public void TestDeleteRows_LargeFile()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs_LargeFile);

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            List<Filter> DeleteFilters = new List<Filter>();
            DeleteFilters.Add(new Filter("PartitionKey", "=", "WebAppLog"));
            string DeleteResult = instanceDelete.DeleteAzureTableRows(TableNameTo, 30, DeleteFilters);
            int DeleteCount = ExtractNextInt(DeleteResult, "total rows deleted");

            string copyVerify = instanceCopy.CopyTableToTable(TableNameTo, TableNameTo2, 30);
            int RemainingRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, DeleteCount + RemainingRowCount);
        }
    }
}

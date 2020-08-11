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
    public class AzureTableUtilitiesBackupXUnitTest
    {
        private string AzureStorageConfigConnection = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
        private string AzureBlobStorageConfigConnection = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;";

        private string BlobRoot = "test";
        private string TableNameFrom = "TestSource";
        private string TableNameTo = "TestCopy";
        private string TableNameTo2 = "TestCopy2";
        private string TableNameRestoreTo = "TestRestore";

        private string WorkingDirectory;
        private string FileNameThatExists;
        private string FileNamePathThatExists_UserProfile;
        private string FileNamePathThatExists_SystemLogs;
        private string FileNamePathThatExists_SystemLogs_LargeFile;


        public AzureTableUtilitiesBackupXUnitTest()
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            WorkingDirectory = Path.Combine(Path.GetDirectoryName(codeBasePath), "TestFiles");
            //WorkingDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().CodeBase) + @"\TestFiles";

            FileNameThatExists = @"UserProfile_Backup_GoodFooter.txt";
            FileNamePathThatExists_UserProfile = WorkingDirectory + @"\" + FileNameThatExists;

            FileNameThatExists = @"SystemLogs_ForXUnit.txt";
            FileNamePathThatExists_SystemLogs = WorkingDirectory + @"\" + FileNameThatExists;

            FileNameThatExists = @"SystemLogs_LargeFile_ForXUnit.txt";
            FileNamePathThatExists_SystemLogs_LargeFile = WorkingDirectory + @"\" + FileNameThatExists;
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


        private string ExtractFileName(string Target)
        {
            try
            {
                string KeyValue = "backed up as '";
                string EndTag = "' under blob";
                int start = Target.IndexOf(KeyValue) + KeyValue.Length;
                int end = Target.IndexOf(EndTag, start);
                return Target.Substring(start, end - start).Trim();
            }
            catch (Exception) { return ""; }
        }


        private void InitializeTables(DeleteAzureTables instanceDelete, RestoreAzureTables instanceRestore, string FileNamePath)
        {
            instanceDelete.DeleteAzureTableRows(TableNameTo);
            instanceDelete.DeleteAzureTableRows(TableNameTo2);
            instanceDelete.DeleteAzureTableRows(TableNameFrom);
            instanceDelete.DeleteAzureTableRows(TableNameRestoreTo);

            string restoreResult = instanceRestore.RestoreTableFromFile(TableNameFrom, FileNamePath);
            int RestoreCount = ExtractNextInt(restoreResult, "Successful;", "entries");
        }


        [Fact]
        public void TestBackupToBlob()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs);

            List<Filter> Filters = new List<Filter>();
            Filters.Add(new Filter("RowKey", "=", "User1"));
            Filters.Add(new Filter("RowKey", "=", "User2", "OR"));

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            string backupToBlob = instanceBackup.BackupTableToBlob(TableNameFrom, BlobRoot, WorkingDirectory, true, true, 10, 10);
            //int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlob(TableNameRestoreTo, TableNameFrom, BlobRoot, WorkingDirectory, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupToBlob_LargeFile()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs_LargeFile);

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            string backupToBlob = instanceBackup.BackupTableToBlob(TableNameFrom, BlobRoot, WorkingDirectory, true, true, 10, 10);
            //int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlob(TableNameRestoreTo, TableNameFrom, BlobRoot, WorkingDirectory, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupToBlobFilters()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs);

            List<Filter> Filters2 = new List<Filter>();
            Filters2.Add(new Filter("RowKey", "=", "User1"));

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30, Filters2);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");


            string backupToBlob = instanceBackup.BackupTableToBlob(TableNameFrom, BlobRoot, WorkingDirectory, true, true, 10, 10, Filters2);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlob(TableNameRestoreTo, TableNameFrom, BlobRoot, WorkingDirectory, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }

    }
}

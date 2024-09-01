using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using TheByteStuff.AzureTableUtilities;
using Xunit;


namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesBackupXUnitTest
    {
        //* See ConnectionSpecManager for configuration information
        private string AzureStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");
        private string AzureBlobStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureBlobStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;");

        private string BlobRoot = "test";
        private string BlobDirectRoot = "testdirect";
        private string TableNameFrom = "TestSource";
        private string TableNameTo = "TestCopy";
        private string TableNameTo2 = "TestCopy2";
        private string TableNameRestoreTo = "TestRestore";

        private string WorkingDirectory;
        private string FileNameThatExists;
        private string FileNamePathThatExists_UserProfile;
        private string FileNamePathThatExists_SystemLogs;
        private string FileNamePathThatExists_SystemLogs_LargeFile;
        private string FileNameTableTypes;
        private string FileNamePathTableTypes;

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

            FileNameTableTypes = @"TestTableTypes.txt";
            FileNamePathTableTypes = WorkingDirectory + @"\" + FileNameTableTypes;
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
            string restoreResult = instanceRestore.RestoreTableFromBlob(TableNameRestoreTo, TableNameFrom, BlobRoot, WorkingDirectory,ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupToBlobDirect_LargeFile()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs_LargeFile);

            string copySetup = instanceCopy.CopyTableToTable(TableNameFrom, TableNameTo, 30);
            int InitialRowCount = ExtractNextInt(copySetup, "total records");

            string backupToBlob = instanceBackup.BackupTableToBlobDirect(TableNameFrom, BlobRoot, true, 10, 10);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlobDirect(TableNameRestoreTo, TableNameFrom, BlobRoot, ExtractFileName(backupToBlob), 30);

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


        [Fact]
        public void TestBackupToBlobTimestampFilter()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            CopyAzureTables instanceCopy = new CopyAzureTables(AzureStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs);

            List<Filter> Filters2 = new List<Filter>();
            Filters2.Add(new Filter("Timestamp", "LessThan", "2024-08-01T12:00:00"));

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


        [Fact]
        public void TestBackupToBlobDirectFilters()
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


            string backupToBlob = instanceBackup.BackupTableToBlobDirect(TableNameFrom, BlobRoot, true, 10, 10, Filters2);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlobDirect(TableNameRestoreTo, TableNameFrom, BlobRoot, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupToBlobDirect()
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

            string backupToBlob = instanceBackup.BackupTableToBlobDirect(TableNameFrom, BlobDirectRoot, false, 10, 10);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlobDirect(TableNameRestoreTo, TableNameFrom, BlobDirectRoot, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }

        //For testing compression efficiency
        [Fact]
        public void TestBackupToBlobDirectTest()
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

            string backupToBlob = instanceBackup.BackupTableToBlobDirect(TableNameFrom, BlobDirectRoot, true, 10, 10);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlobDirect(TableNameRestoreTo, TableNameFrom, BlobDirectRoot, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupToBlobDirectWithCompress()
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

            string backupToBlob = instanceBackup.BackupTableToBlobDirect(TableNameFrom, BlobDirectRoot, true, 10, 10);
            int BackupRowCount = ExtractNextInt(backupToBlob, "total records");

            //Need to restore to confirm
            string restoreResult = instanceRestore.RestoreTableFromBlobDirect(TableNameRestoreTo, TableNameFrom, BlobDirectRoot, ExtractFileName(backupToBlob), 30);

            string copyVerify = instanceCopy.CopyTableToTable(TableNameRestoreTo, TableNameTo2, 30);
            int VerifyRowCount = ExtractNextInt(copyVerify, "total records");
            Assert.Equal(InitialRowCount, VerifyRowCount);
        }


        [Fact]
        public void TestBackupAllTables()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            string restoreResult1 = instanceRestore.RestoreTableFromFile(TableNameTo, FileNamePathThatExists_UserProfile);
            int RestoreCount1 = ExtractNextInt(restoreResult1, "Successful;", "entries");

            string restoreResult2 = instanceRestore.RestoreTableFromFile(TableNameTo2, FileNamePathThatExists_SystemLogs);
            int RestoreCount2 = ExtractNextInt(restoreResult2, "Successful;", "entries");

            string Expected1 = String.Format("Table '{0}' backed up as", TableNameTo);
            string Expected2 = String.Format("Table '{0}' backed up as", TableNameTo2);
            string backupallresult = instanceBackup.BackupAllTablesToBlob(BlobDirectRoot, true, 10, 10);
            Assert.Contains(Expected1, backupallresult.ToString());
            Assert.Contains(Expected2, backupallresult.ToString());
        }


        private List<TableEntity> QueryTable(string TableName)
        {

            TableServiceClient clientSource = new TableServiceClient(AzureStorageConfigConnection.ToString());
            Pageable<TableEntity> queryResultsFilter = clientSource.GetTableClient(TableName).Query<TableEntity>(filter: "", maxPerPage: 100);
            List<TableEntity> entityList = new List<TableEntity>();

            try
            {

                foreach (Page<TableEntity> page in queryResultsFilter.AsPages())
                {
                    foreach (TableEntity qEntity in page.Values)
                    {
                        entityList.Add(qEntity);
                    }
                }
            }
            finally { }
            return entityList;
        }

        private Dictionary<string, KeyValuePair<string, bool>> CreateTypesToVerify()
        {
            Dictionary<string, KeyValuePair<string, bool>> list = new Dictionary<string, KeyValuePair<string, bool>>();

            list.Add("Int32Field", new KeyValuePair<string, bool>( "Int32", false));
            list.Add("Int64Field", new KeyValuePair<string, bool>("Int64", false));
            list.Add("StringField", new KeyValuePair<string, bool>("String", false));
            list.Add("DoubleField", new KeyValuePair<string, bool>("Double", false));
            list.Add("BooleanField1", new KeyValuePair<string, bool>("Boolean", false));
            list.Add("GUIDField", new KeyValuePair<string, bool>("Guid", false));
            list.Add("DateTimeField", new KeyValuePair<string, bool>("DateTimeOffset", false));

            return list;
        }

        private bool DoesTypeMatch(string EntityTypeString, object data)
        {
            bool match = false;
            EntityProperty.EntityPropertyType ept = EntityProperty.StringToType(EntityTypeString);

            switch (ept)
            {
                case EntityProperty.EntityPropertyType.String:
                    match = (data.GetType() == typeof(string));
                    break;
                case EntityProperty.EntityPropertyType.Boolean:
                    match = (data.GetType() == typeof(bool));
                    break;
                case EntityProperty.EntityPropertyType.Double:
                    match = (data.GetType() == typeof(double));
                    break;
                case EntityProperty.EntityPropertyType.Int32:
                    match = (data.GetType() == typeof(int));
                    break;
                case EntityProperty.EntityPropertyType.Int64:
                    match = (data.GetType() == typeof(long));
                    break;
                case EntityProperty.EntityPropertyType.GUID:
                    match = (data.GetType() == typeof(Guid));
                    break;
                case EntityProperty.EntityPropertyType.DateTime:
                    match = (data.GetType() == typeof(DateTimeOffset));
                    break;
            }

            return match;
        }


        /// <summary>
        /// Validates that the field data types in Azue are properly restored.  
        /// </summary>
        [Fact]
        public void TestBackupTableTypes()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);

            InitializeTables(instanceDelete, instanceRestore, FileNamePathThatExists_SystemLogs);

            string restoreResult1 = instanceRestore.RestoreTableFromFile(TableNameRestoreTo, FileNamePathTableTypes);
            int RestoreCount1 = ExtractNextInt(restoreResult1, "Successful;", "entries");
            Assert.Equal(2, RestoreCount1);

            List<TableEntity> tableQuery = QueryTable(TableNameRestoreTo);

            Dictionary<string, KeyValuePair<string, bool>> ValidationSource = CreateTypesToVerify();

            List<KeyValuePair<string, bool>> ValidationResults = new List<KeyValuePair<string, bool>>();

            List<KeyValuePair<string, object>> properties = tableQuery.ElementAt(0).ToList();
            foreach (KeyValuePair<string, object>field in properties)
            {
                KeyValuePair<string, bool> field2;
                if (ValidationSource.TryGetValue(field.Key, out field2))
                {
                    ValidationResults.Add(new KeyValuePair<string, bool>(field2.Key, DoesTypeMatch(field2.Key, field.Value)));
                }
            }
                
            foreach (KeyValuePair<string, bool>item in ValidationResults)
            {
                Assert.True(item.Value, $"Table Entity Type of {item.Key} failed.");
            }
        }
    }
}

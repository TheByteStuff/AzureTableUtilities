using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using TheByteStuff.AzureTableUtilities;
using TheByteStuff.AzureTableUtilities.Exceptions;
using Xunit;

namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesXUnitTest
    {
        private string WorkingDirectory;
        private string FileNameThatExists;
        private string FileNamePathThatExists;
        private string DirectoryDoesNotExist = @"c:\baddir\";

        private string DefaultBlobRoot = "backups";
        private string DefaultTableName = "TestSource";


        private string GoodWorkingDirectory = "";

        //* See ConnectionSpecManager for configuration information
        private string AzureStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;TableEndpoint=http://127.0.0.1:11002/devstoreaccount1;");
        private string AzureBlobStorageConfigConnection = ConnectionSpecManager.GetConnectionSpec("AzureBlobStorageConfigConnection", "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:11001/devstoreaccount1;");

            /*
        private SecureString AzureStorageConfigConnectionSecureStringNull = null;

        private SecureString AzureStorageConfigConnectionSecureStringEmpty = new SecureString();

        private SecureString AzureStorageConfigConnectionSecureStringNotNull = new SecureString();
        private SecureString AzureBlobStorageConfigConnectionSecureStringNotNull = new SecureString();
        */ 
        private string AzureStorageConfigConnectionSecureStringNull = null;

        private string AzureStorageConfigConnectionSecureStringEmpty = "";

        private string AzureStorageConfigConnectionSecureStringNotNull = "";
        private string AzureBlobStorageConfigConnectionSecureStringNotNull = "";


        private string StringNull = null;

        private string BadConnectionSpec = "AccountNamex=devstoreaccount1;AccountKeyx=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.222:11000/devstoreaccount1;QueueEndpoint=http://127.0.0.222:11001/devstoreaccount1;;TableEndpoint=http://127.0.0.222:10002/devstoreaccount1;";
        private string ConnectionSpecNoServer = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://192.168.91.222:11000/devstoreaccount1;QueueEndpoint=http://192.168.91.222:11001/devstoreaccount1;TableEndpoint=http://192.168.91.222:10002/devstoreaccount1;";

        private string DefaultTableNameRestore = "TestSourceTest";
        private string DefaultTableNameRestoreOriginal = "TestSource";
        private string TableNameDoesNotExist = "BogusTableName";

        public AzureTableUtilitiesXUnitTest()
        {
            //var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
            //var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);

            WorkingDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + @"\TestFiles";
            FileNameThatExists = @"UserProfile_Backup_GoodFooter.txt";
            FileNamePathThatExists = WorkingDirectory + @"\" + FileNameThatExists;
            GoodWorkingDirectory = WorkingDirectory; // + @"\workingdir";

            /*
            foreach (char c in AzureStorageConfigConnection.ToCharArray())
            {
                this.AzureStorageConfigConnectionSecureStringNotNull.AppendChar(c);
            }
            */
            AzureStorageConfigConnectionSecureStringNotNull = AzureStorageConfigConnection;
        }

        [Fact]
        public void TestBackupBadDir()
        {
            //WorkingDirectory is not formatted properly
            BackupAzureTables bu = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            Assert.Throws<ParameterSpecException>(() => bu.BackupTableToFile("TestTable", "baddir", false));

            Assert.Throws<ParameterSpecException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, "baddir", false, false, 30));
            //var exception = Assert.Throws<BackupFailedException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, "baddir", false, false, 30));
            //Assert.Contains("ParameterSpecException", exception.InnerException.ToString());
        }


        [Fact]
        public void TestBackupOutfileDirectoryDoesNotExist()
        {
            BackupAzureTables bu = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            var exception1 = Assert.Throws<ParameterSpecException>(() => bu.BackupTableToFile("TestTable", DirectoryDoesNotExist, false));
            Assert.Contains("OutFileDirectory does not exist", exception1.ToString());
             
            var exception2 = Assert.Throws<ParameterSpecException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, DirectoryDoesNotExist, false, false, 30));
            Assert.Contains("OutFileDirectory does not exist", exception2.ToString());
        }

        [Fact]
        public void TestBackupEmptyConnSpec()
        {
            Assert.Throws<ConnectionException>(() => new BackupAzureTables(StringNull));

            Assert.Throws<ConnectionException>(() => new BackupAzureTables(null, AzureBlobStorageConfigConnection));
            Assert.Throws<ConnectionException>(() => new BackupAzureTables("", AzureBlobStorageConfigConnection));

            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnection, null));
            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnection, ""));


            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnectionSecureStringNull));
            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringNull));
            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnectionSecureStringNull, AzureStorageConfigConnectionSecureStringNotNull));

            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringEmpty));
            Assert.Throws<ConnectionException>(() => new BackupAzureTables(AzureStorageConfigConnectionSecureStringEmpty, AzureStorageConfigConnectionSecureStringNotNull));
        }

        [Fact]
        public void TestBackupBadConnSpec()
        {
            BackupAzureTables bu = new BackupAzureTables(BadConnectionSpec, AzureBlobStorageConfigConnection);
            Assert.Throws<ConnectionException>(() => bu.BackupTableToFile(DefaultTableName, WorkingDirectory, false, false, 5));

            bu = new BackupAzureTables(AzureStorageConfigConnection, BadConnectionSpec);
            //Assert.Throws<ConnectionException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, WorkingDirectory, false, false, 5, 5));
            //Assert.Throws<ConnectionException>(() => bu.BackupTableToBlobDirect(DefaultTableName, DefaultBlobRoot, false, 5, 5));
            Assert.Throws<ConnectionException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, WorkingDirectory, false, false, 5, 5));
            Assert.Throws<ConnectionException>(() => bu.BackupTableToBlobDirect(DefaultTableName, DefaultBlobRoot, false, 5, 5));

            bu = new BackupAzureTables(BadConnectionSpec);
            Assert.Throws<ConnectionException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, WorkingDirectory, false, false, 5, 5));
            Assert.Throws<ConnectionException>(() => bu.BackupTableToBlobDirect(DefaultTableName, DefaultBlobRoot, false, 5, 5));
        }

        [Fact]
        public void TestBackupServerNotRunning()
        {
            BackupAzureTables bu = new BackupAzureTables(ConnectionSpecNoServer, AzureBlobStorageConfigConnection);
            Assert.Throws<BackupFailedException>(() => bu.BackupTableToFile(DefaultTableName, WorkingDirectory, false, false, 5));

            bu = new BackupAzureTables(AzureStorageConfigConnection, ConnectionSpecNoServer);
            Assert.Throws<BackupFailedException>(() => bu.BackupTableToBlob(DefaultTableName, DefaultBlobRoot, WorkingDirectory, false, false, 30, 5));

            bu = new BackupAzureTables(AzureStorageConfigConnection, ConnectionSpecNoServer);
            Assert.Throws<BackupFailedException>(() => bu.BackupTableToBlobDirect(DefaultTableName, DefaultBlobRoot, false, 30, 5));
        }


        [Fact]
        public void TestCopyEmptyConnSpec()
        {
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(StringNull));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(StringNull, AzureBlobStorageConfigConnection));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables("", AzureBlobStorageConfigConnection));

            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnection, StringNull));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnection, ""));           

            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringNull));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringNull, AzureBlobStorageConfigConnectionSecureStringNotNull));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringNull));

            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringEmpty));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringEmpty, AzureBlobStorageConfigConnectionSecureStringNotNull));
            Assert.Throws<ConnectionException>(() => new CopyAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringEmpty));
        }


        [Fact]
        public void TestCopyBadConnSpec()
        {
            CopyAzureTables instance = new CopyAzureTables(BadConnectionSpec, AzureBlobStorageConfigConnection);
            Assert.Throws<ConnectionException>(() => instance.CopyTableToTable(DefaultTableName, DefaultTableName, 5));

            instance = new CopyAzureTables(AzureStorageConfigConnection, BadConnectionSpec);
            Assert.Throws<ConnectionException>(() => instance.CopyTableToTable(DefaultTableName, DefaultTableName, 5));
        }

        [Fact]
        public void TestCopyServerNotRunning()
        {
            CopyAzureTables instance = new CopyAzureTables(ConnectionSpecNoServer, AzureStorageConfigConnection);
            Assert.Throws<CopyFailedException>(() => instance.CopyTableToTable(DefaultTableName, DefaultTableName, 5));

            instance = new CopyAzureTables(AzureStorageConfigConnection, ConnectionSpecNoServer);
            Assert.Throws<CopyFailedException>(() => instance.CopyTableToTable(DefaultTableName, DefaultTableName, 5));
        }


        [Fact]
        public void TestDeleteEmptyConnSpec()
        {
            Assert.Throws<ConnectionException>(() => new DeleteAzureTables(StringNull));
            Assert.Throws<ConnectionException>(() => new DeleteAzureTables(""));

            Assert.Throws<ConnectionException>(() => new DeleteAzureTables(AzureStorageConfigConnectionSecureStringNull));
            Assert.Throws<ConnectionException>(() => new DeleteAzureTables(AzureStorageConfigConnectionSecureStringEmpty));
        }

        [Fact]
        public void TestDeleteBadConnSpec()
        {
            DeleteAzureTables instance = new DeleteAzureTables(BadConnectionSpec);
            Assert.Throws<ConnectionException>(() => instance.DeleteAzureTable(TableNameDoesNotExist, 5));
            Assert.Throws<ConnectionException>(() => instance.DeleteAzureTableRows(TableNameDoesNotExist, 5));
        }

        [Fact]
        public void TestDeleteServerNotRunning()
        {
            DeleteAzureTables instance = new DeleteAzureTables(ConnectionSpecNoServer);
            Assert.Throws<DeleteFailedException>(() => instance.DeleteAzureTable(TableNameDoesNotExist, 5));
            Assert.Throws<DeleteFailedException>(() => instance.DeleteAzureTableRows(TableNameDoesNotExist, 5));
        }


        [Fact]
        public void TestRestoreEmptyConnSpec()
        {
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(null, AzureBlobStorageConfigConnection));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables("", AzureBlobStorageConfigConnection));

            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnection, null));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnection, ""));

            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringNull));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringNull, AzureBlobStorageConfigConnectionSecureStringNotNull));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringNull));

            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringEmpty));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringEmpty, AzureBlobStorageConfigConnectionSecureStringNotNull));
            Assert.Throws<ConnectionException>(() => new RestoreAzureTables(AzureStorageConfigConnectionSecureStringNotNull, AzureStorageConfigConnectionSecureStringEmpty));
        }


        [Fact]
        public void TestRestoreBadConnSpec()
        {
            RestoreAzureTables rs = new RestoreAzureTables(BadConnectionSpec, AzureBlobStorageConfigConnection);
            Assert.Throws<ConnectionException>(() => rs.RestoreTableFromFile(DefaultTableNameRestore, FileNamePathThatExists, 5));

            rs = new RestoreAzureTables(AzureStorageConfigConnection, BadConnectionSpec);
            Assert.Throws<ConnectionException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, WorkingDirectory, FileNameThatExists, 5));
            Assert.Throws<ConnectionException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, FileNameThatExists, 5));
        }


        [Fact]
        public void TestRestoreServerNotRunning()
        {
            RestoreAzureTables rs = new RestoreAzureTables(ConnectionSpecNoServer, AzureBlobStorageConfigConnection);
            Assert.Throws<RestoreFailedException>(() => rs.RestoreTableFromFile(DefaultTableNameRestore, FileNamePathThatExists, 5));

            rs = new RestoreAzureTables(AzureStorageConfigConnection, ConnectionSpecNoServer);
            Assert.Throws<RestoreFailedException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, GoodWorkingDirectory, FileNameThatExists, 5));
            Assert.Throws<RestoreFailedException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, "UserProfile_Backup_20200505173139.txt", 5));
        }


        [Fact]
        public void TestRestoreFileExceptions()
        {
            RestoreAzureTables rs = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            var exceptionBadDir = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromFile(DefaultTableNameRestore, "baddir"));
            Assert.Contains("Invalid file name/path", exceptionBadDir.Message);

            var exceptionBadFile = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromFile(DefaultTableNameRestore, @"d:\temp\test\UserProfile_Backup_NOPE.txt"));
            Assert.Contains("does not exist", exceptionBadFile.Message);
        }


        [Fact]
        public void TestRestoreBlobExceptions()
        {
            RestoreAzureTables rs = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            var exceptionBlobFileNameNull = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, GoodWorkingDirectory, null));
            Assert.Contains("Invalid BlobFileName", exceptionBlobFileNameNull.Message);

            var exceptionBlobFileNameEmpty = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, GoodWorkingDirectory, ""));
            Assert.Contains("Invalid BlobFileName", exceptionBlobFileNameEmpty.Message);

            var exceptionBlobRootNull = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, null, GoodWorkingDirectory, "blobfile"));
            Assert.Contains("Invalid BlobRoot", exceptionBlobRootNull.Message);

            var exceptionBlobRootEmpty = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, "", GoodWorkingDirectory, "blobfile"));
            Assert.Contains("Invalid BlobRoot", exceptionBlobRootEmpty.Message);


            //Blob Direct tests
            var exceptionBlobDirectFileNameNull = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, null));
            Assert.Contains("Invalid BlobFileName", exceptionBlobDirectFileNameNull.Message);

            var exceptionBlobDirectFileNameEmpty = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, ""));
            Assert.Contains("Invalid BlobFileName", exceptionBlobDirectFileNameEmpty.Message);

            var exceptionBlobDirectRootNull = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, null, "blobfile"));
            Assert.Contains("Invalid BlobRoot", exceptionBlobDirectRootNull.Message);

            var exceptionBlobDirectRootEmpty = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlobDirect(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, "", "blobfile"));
            Assert.Contains("Invalid BlobRoot", exceptionBlobDirectRootEmpty.Message);
        }

        [Fact]
        public void TestRestoreBlobWorkingDirectoryDoesNotExistExceptions()
        {
            RestoreAzureTables rs = new RestoreAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);

            var exceptionBlobFileNameNull = Assert.Throws<ParameterSpecException>(() => rs.RestoreTableFromBlob(DefaultTableNameRestore, DefaultTableNameRestoreOriginal, DefaultBlobRoot, DirectoryDoesNotExist, "blobfile"));
            Assert.Contains("WorkingDirectory does not exist", exceptionBlobFileNameNull.Message);
        }

        [Fact]
        public void TestFiltersFromFile()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(WorkingDirectory)
                .AddJsonFile("appsettings.json");

            IConfiguration config = builder.Build();

            Assert.Equal("backup", config["Command"]);
            Assert.Equal("RestoreTest", config["DestinationTableName"]);

            var sectionFilters = config.GetSection("Filters");
            List<Filter> filters = sectionFilters.Get<List<Filter>>();

            Assert.True(filters.Count == 2);
            Assert.True(Filter.AreFiltersValid(filters));

            Assert.Equal("true", config["Validate"]);


            var builderNoFilters = new ConfigurationBuilder()
                   .SetBasePath(WorkingDirectory)
                   .AddJsonFile("appsettings_nofilters.json");

            IConfiguration configNoFilters = builder.Build();
            var sectionFiltersNoFilters = config.GetSection("Filters");
            List<Filter> filtersNoFilters = sectionFilters.Get<List<Filter>>();
        }

        [Fact]
        public void TestFilterExceptions()
        {
            Filter f1 = new Filter("PartitionKey", "Bad", "Test", "");
            Assert.False(Filter.IsValidFilter(f1));

            List<Filter> filters = new List<Filter>();
            filters.Add(f1);

            BackupAzureTables bu = new BackupAzureTables(AzureStorageConfigConnection, AzureBlobStorageConfigConnection);
            var exception = Assert.Throws<ParameterSpecException>(() => bu.BackupTableToFile("TestTable", GoodWorkingDirectory, false, false, 30, filters));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exception));
            Assert.Contains("One or more of the supplied filter criteria is invalid.", exception.ToString());

            var exceptionBlob = Assert.Throws<BackupFailedException>(() => bu.BackupTableToBlob("TestTable", "blobroot", GoodWorkingDirectory, false, false, 30, 30, filters));
            Assert.Contains("One or more of the supplied filter criteria is invalid.", exceptionBlob.ToString());

            DeleteAzureTables instanceDelete = new DeleteAzureTables(AzureStorageConfigConnection);

            var exceptionDeleteRows = Assert.Throws<ParameterSpecException>(() => instanceDelete.DeleteAzureTableRows("XX", 30, filters));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionDeleteRows));
            Assert.Contains("One or more of the supplied filter criteria is invalid.", exceptionDeleteRows.ToString());
        }


        [Fact]
        public void TestBackupParameterExceptions()
        {
            BackupAzureTables instanceBackup = new BackupAzureTables(AzureStorageConfigConnection);

            var exceptionToFileTableMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToFile("", "baddir"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToFileTableMissing));
            Assert.Contains("TableName is missing.", exceptionToFileTableMissing.ToString());

            var exceptionToFileOutDirMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToFile("XX", ""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToFileOutDirMissing));
            Assert.Contains("OutFileDirectory is missing.", exceptionToFileOutDirMissing.ToString());

            var exceptionToBlobTableMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToBlob("", "badroot", "baddir"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToBlobTableMissing));
            Assert.Contains("TableName is missing.", exceptionToBlobTableMissing.ToString());

            var exceptionToBlobBlobRootMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToBlob("XX", "", "baddir"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToBlobBlobRootMissing));
            Assert.Contains("BlobRoot is missing.", exceptionToBlobBlobRootMissing.ToString());

            var exceptionToBlobOutDirMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToBlob("XX", "XX", ""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToBlobOutDirMissing));
            Assert.Contains("OutFileDirectory is missing.", exceptionToBlobOutDirMissing.ToString());

            var exceptionToBlobDirectTableMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToBlobDirect("", "badroot"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToBlobDirectTableMissing));
            Assert.Contains("TableName is missing.", exceptionToBlobDirectTableMissing.ToString());

            var exceptionToBlobDirectBlobRootMissing = Assert.Throws<ParameterSpecException>(() => instanceBackup.BackupTableToBlobDirect("XX", ""));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionToBlobDirectBlobRootMissing));
            Assert.Contains("BlobRoot is missing.", exceptionToBlobDirectBlobRootMissing.ToString());
        }


        [Fact]
        public void TestRestorefromFileParameterExceptions()
        {
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection);

            var exceptionFromFileDestinationTableName = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromFile("", "xxxx"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromFileDestinationTableName));
            Assert.Contains("DestinationTableName is missing.", exceptionFromFileDestinationTableName.ToString());

            var exceptionFromFileInvalidFileNamePath = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromFile(DefaultTableName, "invalid path"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromFileInvalidFileNamePath));
            Assert.Contains("Invalid file name/path 'invalid path' specified.", exceptionFromFileInvalidFileNamePath.ToString());

            var exceptionFromFileFileDoesNotExist = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromFile(DefaultTableName, @"c:\xxxx.txt"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromFileFileDoesNotExist));
            Assert.Contains(@"File 'c:\xxxx.txt' does not exist.", exceptionFromFileFileDoesNotExist.ToString());


            var exceptionFromBlobDestinationTableName = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("", "xx", "BlobRoot", "workingdir", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDestinationTableName));
            Assert.Contains("DestinationTableName is missing.", exceptionFromBlobDestinationTableName.ToString());

            var exceptionFromBlobOriginalTableNameMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("XX", "", "BlobRoot", "workingdir", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobOriginalTableNameMissing));
            Assert.Contains("OriginalTableName is missing.", exceptionFromBlobOriginalTableNameMissing.ToString());
        }

        [Fact]
        public void TestRestoreFromBlobParameterExceptions()
        {
            RestoreAzureTables instanceRestore = new RestoreAzureTables(AzureStorageConfigConnection);

            var exceptionFromBlobDestinationTableName = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("", "xx", "BlobRoot", GoodWorkingDirectory, "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDestinationTableName));
            Assert.Contains("DestinationTableName is missing.", exceptionFromBlobDestinationTableName.ToString());

            var exceptionFromBlobOriginalTableNameMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("XX", "", "BlobRoot", GoodWorkingDirectory, "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobOriginalTableNameMissing));
            Assert.Contains("OriginalTableName is missing.", exceptionFromBlobOriginalTableNameMissing.ToString());

            var exceptionFromBlobBlobRootMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("XX", "XX", "", "xx", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobBlobRootMissing));
            //Assert.Contains("Invalid BlobRoot '' specified.", exceptionFromBlobBlobRootMissing.ToString());
            Assert.Contains("WorkingDirectory does not exist.", exceptionFromBlobBlobRootMissing.ToString());

            var exceptionFromBlobBlobRootMissing2 = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("XX", "XX", null, GoodWorkingDirectory, "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobBlobRootMissing2));
            Assert.Contains("Invalid BlobRoot '' specified.", exceptionFromBlobBlobRootMissing2.ToString());

            var exceptionFromBlobWorkingDirectoryMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlob("XX", "XX", "BlobRoot", "", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobWorkingDirectoryMissing));
            Assert.Contains("WorkingDirectory is missing.", exceptionFromBlobWorkingDirectoryMissing.ToString());

            //Blob Direct
            var exceptionFromBlobDirectDestinationTableName = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlobDirect("", "xx", "BlobRoot", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDirectDestinationTableName));
            Assert.Contains("DestinationTableName is missing.", exceptionFromBlobDirectDestinationTableName.ToString());

            var exceptionFromBlobDirectOriginalTableNameMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlobDirect("XX", "", "BlobRoot", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDirectOriginalTableNameMissing));
            Assert.Contains("OriginalTableName is missing.", exceptionFromBlobDirectOriginalTableNameMissing.ToString());

            var exceptionFromBlobDirectBlobRootMissing = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlobDirect("XX", "XX", "", "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDirectBlobRootMissing));
            Assert.Contains("Invalid BlobRoot '' specified.", exceptionFromBlobDirectBlobRootMissing.ToString());

            var exceptionFromBlobDirectBlobRootMissing2 = Assert.Throws<ParameterSpecException>(() => instanceRestore.RestoreTableFromBlobDirect("XX", "XX", null, "blobfilename"));
            Assert.True(typeof(ParameterSpecException).IsInstanceOfType(exceptionFromBlobDirectBlobRootMissing2));
            Assert.Contains("Invalid BlobRoot '' specified.", exceptionFromBlobDirectBlobRootMissing2.ToString());
        }

    }
}

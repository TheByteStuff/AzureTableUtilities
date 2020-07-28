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
    public class AzureTableUtilitiesDeleteXUnitTest
    {
        private string AzureStorageConfigConnection = "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

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
    }
}

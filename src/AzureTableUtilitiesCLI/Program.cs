// See https://aka.ms/new-console-template for more information
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using TheByteStuff.AzureTableUtilities;


string source;
string destination;
string name;
string folder;
string operation;

if (args.Length > 0)
{
    operation = args[0];
     if (operation == "restore" || operation == "backup")
    {
        if (args.Length == 5)
        {
            source = args[1];
            destination = args[2];
            name = args[3];
            folder = args[4];
        }
        else
        {
            Console.WriteLine("Invalid number of arguments");
            return;
        }
    }
    else
    {
        Console.WriteLine("Invalid operation");
        return;
    }   

}
else
{
    Console.WriteLine("No arguments");
    return;
}

var credentials = new Azure.Identity.DefaultAzureCredential();

var tableClientService = new TableServiceClient(new Uri(source), credentials);
var blobClientService = new BlobServiceClient(new Uri(destination), credentials);


if (operation == "backup")
{
    //var backupfolder = DateTime.Now.ToString("yyyyMMddTHHmm");

    var backup = new BackupAzureTables(tableClientService, blobClientService);
    backup.BackupAllTablesToBlob(name, folder, false);
}
else if (operation == "restore")
{
    var restore = new RestoreAzureTables(tableClientService, blobClientService);
    restore.RestoreAllTablesFromBlob(name, folder);
}
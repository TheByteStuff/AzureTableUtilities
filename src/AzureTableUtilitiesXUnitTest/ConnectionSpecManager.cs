using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace AzureTableUtilitiesXUnitTest
{
    /// <summary>
    /// Helper for XUnit tests.  
    /// Set environvment variable "AzureUtilitiesTestConfigFile" to be the full name/path for xml file containing Azure connection specs.
    /// 
    /// Config file format:
    /// <code><?xml version="1.0" encoding="utf-8" ?>
    /// <Map>
    /// <add ConnectionName = "AzureStorageConfigConnection" ConnectionValue="..."/>
    ///   <add ConnectionName = "AzureStorageConfigConnection2" ConnectionValue="..."/>  
    ///   <add ConnectionName = "AzureStorageConfigConnection_ALT1" ConnectionValue="..."/>  
    ///   <add ConnectionName = "AzureStorageConfigConnection_ALT2" ConnectionValue="..."/>  
    ///   <add ConnectionName = "AzureBlobStorageConfigConnection" ConnectionValue="..."/>  
    /// </Map></code>
    /// 
    /// </summary>
    public class ConnectionSpecManager
    {
        private static Dictionary<string, string> ConnectionSpecMap = null;
        private static object _sync = new object(); // single lock for both fields

        private static Dictionary<string, string> GetConnectionSpecMap()
        {
            lock (_sync)
            {
                if (ConnectionSpecMap == null)
                {
                    string AzureUtilitiesTestConfigFile = System.Environment.GetEnvironmentVariable("AzureUtilitiesTestConfigFile");

                    var xdoc = System.Xml.Linq.XDocument.Load(AzureUtilitiesTestConfigFile);
                    ConnectionSpecMap = xdoc.Root.Elements()
                                       .ToDictionary(a => (string)a.Attribute("ConnectionName"),
                                                     a => (string)a.Attribute("ConnectionValue"));
                }
            }
            return ConnectionSpecMap;
        }

        public static string GetConnectionSpec(string ConnectionName)
        {
            return GetConnectionSpecMap().GetValueOrDefault(ConnectionName, null);
        }

        public static string GetConnectionSpec(string ConnectionName, string DefaultConnectionString)
        {
            return GetConnectionSpecMap().GetValueOrDefault(ConnectionName, DefaultConnectionString);
        }

    }
}

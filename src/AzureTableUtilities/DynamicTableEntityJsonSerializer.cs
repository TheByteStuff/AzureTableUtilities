using System;

//using Microsoft.Azure.Cosmos.Table;
using Azure.Data.Tables;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Based on classes from https://www.nuget.org/packages/DynamicTableEntityJsonSerializer/1.0.0
    /// </summary>
    class DynamicTableEntityJsonSerializer
    {
        private readonly DynamicTableEntityJsonConverter jsonConverter;

        public DynamicTableEntityJsonSerializer(List<string> excludedProperties = null)
        {
            this.jsonConverter = new DynamicTableEntityJsonConverter(excludedProperties);
        }

        public string Serialize(TableEntity entity)
        {
            string str;
            if (entity != null)
                str = JsonConvert.SerializeObject((object)entity, new JsonConverter[1]
                {
          (JsonConverter) this.jsonConverter
                });
            else
                str = (string)null;
            return str;
        }

        public TableEntity Deserialize(string serializedEntity)
        {
            TableEntity local;
            if (serializedEntity != null)
                local = JsonConvert.DeserializeObject<TableEntity>(serializedEntity, new JsonConverter[1] { (JsonConverter)this.jsonConverter });
            else
                local = null;
            return (TableEntity)local;
        }
    }
}
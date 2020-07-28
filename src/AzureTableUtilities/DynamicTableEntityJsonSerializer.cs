using System;

using Microsoft.Azure.Cosmos.Table;
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

        public string Serialize(DynamicTableEntity entity)
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

        public DynamicTableEntity Deserialize(string serializedEntity)
        {
            DynamicTableEntity local;
            if (serializedEntity != null)
                local = JsonConvert.DeserializeObject<DynamicTableEntity>(serializedEntity, new JsonConverter[1] {(JsonConverter) this.jsonConverter});
            else
                local = null;
            return (DynamicTableEntity)local;
        }
    }
}
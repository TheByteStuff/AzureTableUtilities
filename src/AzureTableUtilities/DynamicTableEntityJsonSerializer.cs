using System;
using System.Collections.Generic;
using System.Text.Json;
using Azure.Data.Tables;

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
            if (entity == null)
                return null;

            var options = new JsonSerializerOptions
            {
                Converters = { jsonConverter }
            };

            return JsonSerializer.Serialize(entity, options);
        }

        public TableEntity Deserialize(string serializedEntity)
        {
            if (serializedEntity == null)
                return null;

            var options = new JsonSerializerOptions
            {
                Converters = { jsonConverter }
            };

            return JsonSerializer.Deserialize<TableEntity>(serializedEntity, options);
        }
    }
}

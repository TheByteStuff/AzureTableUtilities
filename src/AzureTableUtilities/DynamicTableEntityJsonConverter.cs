using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Data.Tables;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    ///  Based on classes from https://www.nuget.org/packages/DynamicTableEntityJsonSerializer/1.0.0
    /// </summary>
    class DynamicTableEntityJsonConverter : JsonConverter<TableEntity>
    {
        private const int EntityPropertyIndex = 0;
        private const int EntityPropertyEdmTypeIndex = 1;
        private readonly List<string> excludedProperties;

        private static List<string> excludedKeys = new List<string> { "PartitionKey", "RowKey", "Timestamp", "ETag" };

        public DynamicTableEntityJsonConverter(List<string> excludedProperties = null)
        {
            this.excludedProperties = excludedProperties;
        }

        public override void Write(Utf8JsonWriter writer, TableEntity value, JsonSerializerOptions options)
        {
            if (value == null)
                return;
            writer.WriteStartObject();
            WriteJsonProperties(writer, value, this.excludedProperties);
            writer.WriteEndObject();
        }

        public override TableEntity Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            TableEntity dynamicTableEntity = new TableEntity();

            using (JsonDocument document = JsonDocument.ParseValue(ref reader))
            {
                foreach (JsonProperty property in document.RootElement.EnumerateObject())
                {
                    if (string.Equals(property.Name, "PartitionKey", StringComparison.Ordinal))
                        dynamicTableEntity.PartitionKey = property.Value.GetString();
                    else if (string.Equals(property.Name, "RowKey", StringComparison.Ordinal))
                        dynamicTableEntity.RowKey = property.Value.GetString();
                    else if (string.Equals(property.Name, "Timestamp", StringComparison.Ordinal))
                        dynamicTableEntity.Timestamp = property.Value.GetDateTimeOffset();
                    else if (string.Equals(property.Name, "ETag", StringComparison.Ordinal))
                    {
                        dynamicTableEntity.ETag = new Azure.ETag(property.Value.GetString());
                    }
                    else
                    {
                        KeyValuePair<string, object> data = CreateKeyValue(property);
                        dynamicTableEntity.Add(data.Key, data.Value);
                    }
                }
            }
            return dynamicTableEntity;
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(TableEntity).IsAssignableFrom(objectType);
        }

        private static void WriteJsonProperties(
          Utf8JsonWriter writer,
          TableEntity entity,
          List<string> excludedProperties = null)
        {
            if (entity == null)
                return;
            writer.WriteString("PartitionKey", entity.PartitionKey);
            writer.WriteString("RowKey", entity.RowKey);
            writer.WriteString("Timestamp", entity.Timestamp?.ToString("o"));

            for (int j = 0; j < entity.Count; j++)
            {
                string valueType = entity.ElementAt(j).Value.GetType().Name;

                if (excludedKeys.Contains(entity.ElementAt(j).Key))
                {
                    continue;
                }
                else
                {
                    EntityProperty ep = new EntityProperty(entity.ElementAt(j), EntityProperty.StringToType(valueType));
                    WriteJsonProperty(writer, entity.ElementAt(j), EntityProperty.StringToType(valueType));
                }
            }
        }

        private static void WriteJsonProperty(
          Utf8JsonWriter writer,
          KeyValuePair<string, object> property,
          EntityProperty.EntityPropertyType type)
        {
            if (string.IsNullOrWhiteSpace(property.Key) || property.Value == null)
                return;

            switch ((int)type)
            {
                case 0:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.String);
                    break;
                case 1:
                    throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during serialization.", type));
                case 2:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.Boolean);
                    break;
                case 3:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.DateTime);
                    break;
                case 4:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.Double);
                    break;
                case 5:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.GUID);
                    break;
                case 6:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.Int32);
                    break;
                case 7:
                    WriteJsonPropertyWithEdmType(writer, property.Key, property.Value, EntityProperty.EntityPropertyType.Int64);
                    break;
                default:
                    throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during serialization.", type));
            }
        }

        private static void WriteJsonPropertyWithEdmType(
          Utf8JsonWriter writer,
          string key,
          object value,
          EntityProperty.EntityPropertyType tableEntityType)
        {
            writer.WritePropertyName(key);
            writer.WriteStartObject();
            writer.WritePropertyName(key);
            writer.WriteStringValue(value.ToString());
            writer.WritePropertyName("EdmType");
            writer.WriteStringValue(tableEntityType.ToString());
            writer.WriteEndObject();
        }

        private static KeyValuePair<string, object> CreateKeyValue(JsonProperty property)
        {
            if (property.Value.ValueKind == JsonValueKind.Null)
                return new KeyValuePair<string, object>();

            JsonElement element = property.Value;
            EntityProperty.EntityPropertyType edmType = (EntityProperty.EntityPropertyType)Enum.Parse(typeof(EntityProperty.EntityPropertyType), element.GetProperty("EdmType").GetString(), true);

            KeyValuePair<string, object> kvp = new KeyValuePair<string, object>();
            switch ((int)edmType)
            {
                case 0:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetString());
                    break;
                case 1:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetBytesFromBase64());
                    break;
                case 2:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetBoolean());
                    break;
                case 3:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetDateTimeOffset());
                    break;
                case 4:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetDouble());
                    break;
                case 5:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetGuid());
                    break;
                case 6:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetInt32());
                    break;
                case 7:
                    kvp = new KeyValuePair<string, object>(property.Name, element.GetProperty(property.Name).GetInt64());
                    break;
                default:
                    throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during deserialization.", edmType));
            }
            return kvp;
        }
    }
}

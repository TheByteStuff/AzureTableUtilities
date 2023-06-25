using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Azure.Data.Tables;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    ///  Based on classes from https://www.nuget.org/packages/DynamicTableEntityJsonSerializer/1.0.0
    /// </summary>
    class DynamicTableEntityJsonConverter : JsonConverter
    {
        private const int EntityPropertyIndex = 0;
        private const int EntityPropertyEdmTypeIndex = 1;
        private readonly List<string> excludedProperties;

        private static List<string> excludedKeys = new List<string> { "PartitionKey", "RowKey", "Timestamp", "ETag" };

        public DynamicTableEntityJsonConverter(List<string> excludedProperties = null)
        {
            this.excludedProperties = excludedProperties;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
                return;
            writer.WriteStartObject();
            DynamicTableEntityJsonConverter.WriteJsonProperties(writer, (TableEntity)value, this.excludedProperties);
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return (object)null;
            TableEntity dynamicTableEntity = new TableEntity();

            using (List<JProperty>.Enumerator enumerator = JObject.Load(reader).Properties().ToList<JProperty>().GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    JProperty current = enumerator.Current;
                    if (string.Equals(current.Name, "PartitionKey", StringComparison.Ordinal))
                        dynamicTableEntity.PartitionKey = ((object)current.Value).ToString();
                    else if (string.Equals(current.Name, "RowKey", StringComparison.Ordinal))
                        dynamicTableEntity.RowKey = ((object)current.Value).ToString();
                    else if (string.Equals(current.Name, "Timestamp", StringComparison.Ordinal))
                        dynamicTableEntity.Timestamp = (DateTimeOffset)current.Value.ToObject<DateTimeOffset>(serializer);
                    else if (string.Equals(current.Name, "ETag", StringComparison.Ordinal))
                    {
                        dynamicTableEntity.ETag = new Azure.ETag(current.Value.ToString());
                    }
                    else
                    {
                        KeyValuePair<string, object> data = DynamicTableEntityJsonConverter.CreateKeyValue(serializer, current);
                        dynamicTableEntity.Add(data.Key, data.Value);
                    }
                }
            }
            return (object)dynamicTableEntity;
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(TableEntity).IsAssignableFrom(objectType);
        }

        private static void WriteJsonProperties(
          JsonWriter writer,
          TableEntity entity,
          List<string> excludedProperties = null)
        {
            if (entity == null)
                return;
            writer.WritePropertyName("PartitionKey");
            writer.WriteValue(entity.PartitionKey);
            writer.WritePropertyName("RowKey");
            writer.WriteValue(entity.RowKey);
            writer.WritePropertyName("Timestamp");
            writer.WriteValue(entity.Timestamp);
            //writer.WritePropertyName("ETag");
            //writer.WriteValue(entity.ETag);
            //int i= 0;
            for (int j = 0; j < entity.Count; j++)
            {
                string ValueType = entity.ElementAt(j).Value.GetType().Name;


                if (excludedKeys.Contains(entity.ElementAt(j).Key))
                {

                }
                else
                {
                    EntityProperty ep = new EntityProperty(entity.ElementAt(j), EntityProperty.StringToType(ValueType)); //  EntityProperty.EntityPropertyType.String);
                    DynamicTableEntityJsonConverter.WriteJsonProperty(writer, entity.ElementAt(j), EntityProperty.StringToType(ValueType));
                }
            }

        }

        private static void WriteJsonProperty(
          JsonWriter writer,
            KeyValuePair<string, object> property,
            EntityProperty.EntityPropertyType type)
        {
            //https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_JsonToken.htm
            if (string.IsNullOrWhiteSpace(property.Key) || property.Value == null)
                return;
            switch ((int)type)
            {
                case 0:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.String);
                    break;
                case 1:
                    throw new NotSupportedException(string.Format((IFormatProvider)CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during serialization.", type));
                case 2:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.Boolean);
                    break;
                case 3:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.DateTime);
                    break;
                case 4:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.Double);
                    break;
                case 5:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.GUID);
                    break;
                case 6:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.Int32);
                    break;
                case 7:
                    DynamicTableEntityJsonConverter.WriteJsonPropertyWithEdmType(writer, property.Key, (object)property.Value, EntityProperty.EntityPropertyType.Int64);
                    break;
                default:
                    throw new NotSupportedException(string.Format((IFormatProvider)CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during serialization.", type));
            }
        }

        private static void WriteJsonPropertyWithEdmType(
          JsonWriter writer,
          string key,
          object value,
          EntityProperty.EntityPropertyType TableEntityType)
        {
            writer.WritePropertyName(key);
            writer.WriteStartObject();
            writer.WritePropertyName(key);
            writer.WriteValue(value);
            writer.WritePropertyName("EdmType");
            writer.WriteValue(TableEntityType.ToString());
            writer.WriteEndObject();
        }

        private static KeyValuePair<string, object> CreateKeyValue(
          JsonSerializer serializer,
          JProperty property)
        {
            if (property == null)
                return new KeyValuePair<string, object>();
            List<JProperty> list = JObject.Parse(((object)property.Value).ToString()).Properties().ToList<JProperty>();
            EntityProperty.EntityPropertyType edmType = (EntityProperty.EntityPropertyType)Enum.Parse(typeof(EntityProperty.EntityPropertyType), ((object)list[1].Value).ToString(), true);
            //EntityProperty entityProperty = new EntityProperty(new KeyValuePair<string, object>("test", 123), edmType);
            KeyValuePair<string, object> KVP = new KeyValuePair<string, object>();
            switch ((int)edmType)
            {
                case 0:
                    KVP = new KeyValuePair<string, object>(list[0].Name, (string)list[0].Value.ToObject<string>(serializer));
                    break;
                case 1:
                    KVP = new KeyValuePair<string, object>(list[0].Name, (byte[])list[0].Value.ToObject<byte[]>(serializer));
                    //entityProperty = EntityProperty.GeneratePropertyForByteArray((byte[])list[0].Value.ToObject<byte[]>(serializer));
                    break;
                case 2:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new bool?((bool)list[0].Value.ToObject<bool>(serializer)));
                    break;
                case 3:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new DateTimeOffset?((DateTimeOffset)list[0].Value.ToObject<DateTimeOffset>(serializer)));
                    break;
                case 4:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new double?((double)list[0].Value.ToObject<double>(serializer)));
                    break;
                case 5:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new Guid?((Guid)list[0].Value.ToObject<Guid>(serializer)));
                    break;
                case 6:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new int?((int)list[0].Value.ToObject<int>(serializer)));
                    break;
                case 7:
                    KVP = new KeyValuePair<string, object>(list[0].Name, new long?((long)list[0].Value.ToObject<long>(serializer)));
                    break;
                default:
                    throw new NotSupportedException(string.Format((IFormatProvider)CultureInfo.InvariantCulture, "Unsupported EntityProperty.PropertyType:{0} detected during deserialization.", (object)edmType));
            }
            return KVP;
        }
    }
}
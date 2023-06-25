using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Helper class for serializing Data to retain the Azure types
    /// </summary>
    public class EntityProperty
    {
        /// <summary>
        /// Helper class for serializing Data to retain the Azure types
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        public EntityProperty(KeyValuePair<string, object> value, EntityPropertyType type)
        {
            Value = value;
            PropertyType = type;
        }

        public KeyValuePair<string, object> Value;

        /// <summary>
        /// Azure.Data.Table types.  BinaryData does not appear to be valid but was retained per old model.
        /// </summary>
        public enum EntityPropertyType { String = 0, BinaryData = 1, Boolean = 2, DateTime = 3, Double = 4, GUID = 5, Int32 = 6, Int64 = 7 };


        /// <summary>
        /// Return EntityPropertyType for corresponding string value
        /// </summary>
        /// <param name="TypeAsString"></param>
        /// <returns></returns>
        public static EntityPropertyType StringToType(string TypeAsString)
        {
            if (TypeAsString.Equals("String"))
            {
                return EntityPropertyType.String;
            }
            else if (TypeAsString.Equals("Int32"))
            {
                return EntityPropertyType.Int32;
            }
            else if (TypeAsString.Equals("Int64"))
            {
                return EntityPropertyType.Int64;
            }
            else if (TypeAsString.Equals("Double"))
            {
                return EntityPropertyType.Double;
            }
            else if (TypeAsString.Equals("Boolean"))
            {
                return EntityPropertyType.Boolean;
            }
            else if (TypeAsString.Equals("Guid"))
            {
                return EntityPropertyType.GUID;
            }
            else if (TypeAsString.Equals("DateTimeOffset"))
            {
                return EntityPropertyType.DateTime;
            }
            else if (TypeAsString.Equals("BinaryData"))
            {
                return EntityPropertyType.BinaryData;
            }
            else
            {
                return EntityPropertyType.String;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public EntityPropertyType PropertyType;

    }
}
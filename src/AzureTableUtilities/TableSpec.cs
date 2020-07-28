using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities
{
    class TableSpec
    {
        public string ProcessingMetaData = "ProcessingMetaData";
        public string TableName { get; set; }
        public string RecordType { get; set; }
        public long RecordCount { get; set; } = 0;
        public DateTime ProcessingTimeStamp { get; set; }

        public TableSpec()
        {
        }

        public TableSpec(string TableName)
        {
            this.TableName = TableName;
            this.RecordType = "Header";
            this.ProcessingTimeStamp = System.DateTime.Now;
        }

        public TableSpec(string TableName, long RecordCount) : this(TableName)
        {
            this.RecordType = "Footer";
            this.RecordCount = RecordCount;
        }

        public TableSpec(string TableName, string RecordType, long RecordCount) : this(TableName, RecordCount)
        {
            this.RecordType = RecordType;
        }

    }
}

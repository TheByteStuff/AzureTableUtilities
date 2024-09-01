using System.Collections.Generic;
using TheByteStuff.AzureTableUtilities;
using Xunit;

namespace AzureTableUtilitiesXUnitTest
{
    public class AzureTableUtilitiesFiltersBaseXUnitTest
    {
        private string OptionPartitionKey = "PartitionKey";
        private string Value = "Test";
        private string JoinAnd = "and";
        private string Comparison = "!=";

        private void SetFilterToGood(Filter f1)
        {
            f1.Join = "AND";
            f1.Comparison = "!=";
            f1.Option = "PartitionKey";
            f1.Value = "Test";
        }

        [Fact]
        public void TestFilterComparison()
        {
            Filter f1 = new Filter();
            SetFilterToGood(f1);
            Assert.True(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Comparison = "=";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "==";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "Equal";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "<>";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "!=";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "NotEqual";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = ">";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "GreaterThan";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = ">=";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "GreaterThanOrEqual";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "<";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "LessThan";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "<=";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Comparison = "LessThanOrEqual";
            Assert.True(Filter.IsValidFilter(f1));

            f1.Comparison = null;
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "NE";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "GE";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "LE";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "GT";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "LT";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Comparison = "EQ";
            Assert.False(Filter.IsValidFilter(f1));
        }


        [Fact]
        public void TestFilterJoin()
        {
            Filter f1 = new Filter();
            SetFilterToGood(f1);

            SetFilterToGood(f1);
            f1.Join = "";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Join = "OR";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Join = "AND";
            Assert.True(Filter.IsValidFilter(f1));

            f1.Join = "NOT";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Join = null;
            Assert.False(Filter.IsValidFilter(f1));
        }

        [Fact]
        public void TestFilterValue()
        {
            Filter f1 = new Filter();
            SetFilterToGood(f1);

            f1.Option = "PartitionKey";
            f1.Value = Value;
            Assert.True(Filter.IsValidFilter(f1));
            f1.Value = "";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Value = null;
            Assert.False(Filter.IsValidFilter(f1));

            f1.Option = "RowKey";
            f1.Value = "Value";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Value = "";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Value = null;
            Assert.False(Filter.IsValidFilter(f1));

            f1.Option = "Timestamp";
            f1.Value = "2020-01-01";
            Assert.True(Filter.IsValidFilter(f1));
            f1.Value = "2020-01-01 14:00:00";
            Assert.True(Filter.IsValidTimestamp(f1.Value));
            Assert.True(Filter.IsValidFilter(f1));
            f1.Value = "20020x";
            Assert.False(Filter.IsValidFilter(f1));
            Assert.False(Filter.IsValidTimestamp(f1.Value));
            f1.Value = "";
            Assert.False(Filter.IsValidFilter(f1));
            f1.Value = null;
            Assert.False(Filter.IsValidFilter(f1));
        }


        [Fact]
        public void TestFilterScenarios()
        {
            Filter f1 = new Filter();
            SetFilterToGood(f1);

            Assert.True(Filter.IsValidFilter(f1));
            f1.Option = "Timestamp";
            f1.Value = "2020-01-01 11:44:00";
            Assert.True(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Value = "20200101";
            f1.Option = "Timestamp";    // Value is invalid Timestamp, expect fail
            Assert.False(Filter.IsValidTimestamp(f1.Value));
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Comparison = "nope";
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Option = "InvalidValue";
            Assert.False(Filter.IsValidOption(f1.Option));
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Comparison = "InvalidValue";
            Assert.False(Filter.IsValidFilter(f1));

            Filter f2 = new Filter();
            f2.Join = "AND";
            f2.Comparison = "!=";
            f2.Option = "PartitionKey";
            f2.Value = "Test";

            Filter f3 = new Filter();
            f3.Join = "OR";
            f3.Comparison = "<";
            f3.Option = "Timestamp";
            f3.Value = "2020-01-01 11:44:00";

            Filter f4 = new Filter();
            f4.Join = "";
            f4.Comparison = "!=";
            f4.Option = "RowKey";
            f4.Value = "Test";

            Assert.True(Filter.IsValidFilter(f2));
            Assert.True(Filter.IsValidFilter(f3));
            Assert.True(Filter.IsValidFilter(f4));

            List<Filter> ValidList1 = new List<Filter>();
            Assert.True(Filter.AreFiltersValid(ValidList1));    //Test Empty List
            ValidList1.Add(f4);
            ValidList1.Add(f2);
            ValidList1.Add(f3);
            Assert.True(Filter.AreFiltersValid(ValidList1));    //Valid list
            string FilterSpec = Filter.BuildFilterSpec(ValidList1);
            Assert.NotNull(FilterSpec);
            Assert.NotEmpty(FilterSpec);
            string ExpectedFilterSpec = "RowKey ne 'Test'  and  PartitionKey ne 'Test'  or  Timestamp lt DateTime'2020-01-01 11:44:00'";
            //Assert.Equal("((RowKey ne 'Test') and (PartitionKey ne 'Test')) or (Timestamp lt datetime'2020-01-01T16:44:00.0000000Z')", FilterSpec);
            Assert.Equal(ExpectedFilterSpec, FilterSpec);
            // "RowKey 'ne' 'Test'  and  PartitionKey 'ne' 'Test'  or  Timestamp 'lt' datetime'1/1/2020 11:44:00 AM -05:00'"

            List<Filter> InvalidJoin1 = new List<Filter>();
            ValidList1.Add(f4);
            ValidList1.Add(f4);
            Assert.False(Filter.AreFiltersValid(ValidList1)); // Empty Join second in list

            List<Filter> InvalidJoin2 = new List<Filter>();
            ValidList1.Add(f2);
            ValidList1.Add(f3);
            Assert.False(Filter.AreFiltersValid(ValidList1)); // Populated Join first in list
        }

        [Fact]
        public void TestFilterConstructor()
        {
            Filter f1 = new Filter(OptionPartitionKey, Comparison, "Test", JoinAnd);

            Assert.Equal(OptionPartitionKey, f1.Option);
            Assert.Equal(Comparison, f1.Comparison);
            Assert.Equal("Test", f1.Value);
            Assert.Equal(JoinAnd, f1.Join);
            Assert.True(Filter.IsValidFilter(f1));

            f1.Option = "Timestamp";
            f1.Value = "2020-01-01 11:44:00";
            Assert.True(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Value = "20200101";
            f1.Option = "Timestamp";    // Value is invalid Timestamp, expect fail
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Comparison = "nope";
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Option = "InvalidValue";
            Assert.False(Filter.IsValidFilter(f1));

            SetFilterToGood(f1);
            f1.Comparison = "InvalidValue";
            Assert.False(Filter.IsValidFilter(f1));
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Container class for filter parameters to be used on Backup, Copy or Delete operations to remove records from the destination per filter values.
    /// </summary>
    public class Filter
    {

        /// <summary>
        /// Create an empty Filter object.
        /// </summary>
        public Filter()
        {

        }

        /// <summary>
        /// Constructor to create a Filter object with attributes set.
        /// </summary>
        /// <param name="Option">The option.</param>
        /// <param name="Comparison">The Comparison value.</param>
        /// <param name="Value">The Value.</param>
        /// <param name="Join">The Join definition (optional)</param>
        public Filter(string Option, string Comparison, string Value, string Join = "")
        {
            this.Option = Option;
            this.Comparison = Comparison;
            this.Value = Value;
            this._Join = Join.ToLower().Trim();
        }

        /// <summary>
        /// The value to use to join the current Filter with the prior Filter
        /// </summary>
        public string Join {
            get { return this._Join; }
            set { this._Join = (value==null ? null : value.ToLower().Trim()); }
        }

        private string _Join = "";

        /// <summary>
        /// The Option for this filter, ex: PartitionKey
        /// </summary>
        public string Option { get; set; } = "";

        /// <summary>
        /// The comparison to use for the value specified.
        /// </summary>
        public string Comparison { get; set; } = "";

        /// <summary>
        /// The value of the Option.
        /// </summary>
        public string Value { get; set; } = "";

        private const string PartitionKey = "PartitionKey";
        private const string RowKey = "RowKey";
        private const string Timestamp = "Timestamp";

        /// <summary>
        /// Tests whether the list of Filter objects passed are valid filters.
        /// Contextual checks (ex: join must be an empty string on first in list, and populated on others) are made.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns>bool</returns>
        public static bool AreFiltersValid(List<Filter> filters)
        {
            bool IsValid = true;
            bool EmptyJoin = true;
            if (filters != null)
            {
                foreach (Filter filter in filters)
                {
                    IsValid = IsValid && IsValidFilter(filter) && CheckJoin(filter.Join, EmptyJoin);
                    EmptyJoin = false;
                }
            }
            return IsValid;
        }

        private static bool CheckJoin(string value, bool EmptyExpected)
        {
            return (String.IsNullOrEmpty(value) && EmptyExpected) || (!String.IsNullOrEmpty(value) && !EmptyExpected);
        }

        /// <summary>
        /// Tests whether the passed Filter object is valid.  Note that a filter may be valid for use alone, but not work in a list (ex: Join value must match context of placement in list)
        /// </summary>
        /// <param name="filter"></param>
        /// <returns>bool</returns>
        public static bool IsValidFilter(Filter filter)
        {
            bool valid = false;
            if (IsValidComparision(filter.Comparison) && IsValidJoin(filter.Join))
            {
                if (IsValidOption(filter.Option))
                {
                    if ((PartitionKey.Equals(filter.Option)) || (RowKey.Equals(filter.Option)))
                    {
                        if (!String.IsNullOrWhiteSpace(filter.Value))
                            valid = true;
                    }
                    else
                    {
                        // Validate Timestamp
                        valid = IsValidTimestamp(filter.Value);
                    }
                }
            }
            return valid;
        }

        /// <summary>
        /// Tests if the string passed is a valid filter option.
        /// </summary>
        /// <param name="Option">Valid values are PartitionKey, RowKey and Timestamp.</param>
        /// <returns>bool</returns>
        public static bool IsValidOption(string Option)
        {
            if ((!String.IsNullOrEmpty(Option)) && (Option.Equals(PartitionKey) || Option.Equals(RowKey) || Option.Equals(Timestamp)))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Tests if the string passed is a valid comparison operator for the filter.
        /// </summary>
        /// <param name="comparison">Valid values are =, ==, Equal, !=, &lt;&gt;, NotEqual, &gt;, GreaterThan, &gt;=, GreaterThanOrEqual, &lt;, LessThan, &lt;=, LessThanOrEqual</param>
        /// <returns>bool</returns>
        public static bool IsValidComparision(string comparison)
        {
            return (ConvertComparisonToOperator(comparison).Length > 0);
        }

        /// <summary>
        /// Tests if the string passed is a valid Timestamp; .Net DateTimeOffset is used for validation.
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns>bool</returns>
        public static bool IsValidTimestamp(string timestamp)
        {
            try
            {
                DateTimeOffset dto = DateTimeOffset.Parse(timestamp);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Tests if the string value passed is a valid Join operator for the filters.
        /// </summary>
        /// <param name="join">Valid values are empty string, and, or</param>
        /// <returns>bool</returns>
        public static bool IsValidJoin(string join)
        {
            if ((join!=null) && (join.Equals("") || join.Equals("and") || join.Equals("or")))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Converts a string containing a Filter compartison value into the value used for the Azure where clause.
        /// </summary>
        /// <param name="comparison"></param>
        /// <returns>string</returns>
        public static string ConvertComparisonToOperator(string comparison)
        {
            switch (comparison)
            {
                case "=":
                case "==":
                case "Equal": return "eq"; 
                case "<>":
                case "!=":
                case "NotEqual": return "ne"; 
                case ">":
                case "GreaterThan": return "gt"; 
                case ">=":
                case "GreaterThanOrEqual": return "ge";
                case "<":
                case "LessThan": return "lt";
                case "<=":
                case "LessThanOrEqual": return "le";
                default: return "";
            }
        }

        /// <summary>
        /// Build the "Where" clause for the query call of the table being extracted.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public static string BuildFilterSpec(List<Filter> filters)
        {
            if (null == filters)
            {
                return String.Empty;
            }

            string filterout = "";
            bool concat = false;
            foreach (Filter filter in filters)
            {
                string filtertemp = "";
                switch (filter.Option)
                {
                    case PartitionKey:
                        filtertemp = $"PartitionKey {Filter.ConvertComparisonToOperator(filter.Comparison)} '{filter.Value}'";
                        break;
                    case RowKey:
                        filtertemp = $"RowKey {Filter.ConvertComparisonToOperator(filter.Comparison)} '{filter.Value}'"; // CosmosTable.TableQuery.GenerateFilterCondition(RowKey, Filter.ConvertComparisonToOperator(filter.Comparison), filter.Value);
                        break;
                    case Timestamp:
                        filtertemp = $"Timestamp {Filter.ConvertComparisonToOperator(filter.Comparison)} datetime'{DateTimeOffset.Parse(filter.Value)}'";
                        break;
                    default: throw new Exception(String.Format("Unknown filter option {0}", filter.Option));
                }
                if (concat)
                {
                    string joinValue = " and ";
                    if ("OR".Equals(filter.Join.ToUpper()))
                    {
                        joinValue = " or ";
                    }
                    filterout = $"{filterout} {joinValue} {filtertemp}";
                }
                else
                {
                    concat = true;
                    filterout = filtertemp;
                }
            }
            return filterout;
        }
    }
}
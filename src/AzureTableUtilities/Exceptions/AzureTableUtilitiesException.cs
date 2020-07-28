using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Base exception class for this package.
    /// </summary>
    public class AzureTableUtilitiesException : Exception
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableUtilitiesException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableUtilitiesException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableUtilitiesException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
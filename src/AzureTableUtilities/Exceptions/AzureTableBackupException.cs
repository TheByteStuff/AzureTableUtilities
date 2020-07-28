using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Base exception class for this package.
    /// </summary>
    public class AzureTableBackupException : Exception
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableBackupException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableBackupException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AzureTableBackupException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Exception for the backup class.
    /// </summary>
    public class BackupFailedException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public BackupFailedException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public BackupFailedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public BackupFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

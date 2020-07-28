using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Ckass for restore operation exceptions.
    /// </summary>
    public class RestoreFailedException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public RestoreFailedException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public RestoreFailedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public RestoreFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

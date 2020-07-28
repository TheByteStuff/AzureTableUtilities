using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Exception class for the delete operations.
    /// </summary>
    public class DeleteFailedException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public DeleteFailedException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public DeleteFailedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public DeleteFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

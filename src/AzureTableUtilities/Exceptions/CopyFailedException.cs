using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Class for copy operation exceptions.
    /// </summary>
    public class CopyFailedException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CopyFailedException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public CopyFailedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public CopyFailedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

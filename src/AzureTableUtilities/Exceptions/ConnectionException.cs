using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Class for connection related exceptions.
    /// </summary>
    public class ConnectionException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ConnectionException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

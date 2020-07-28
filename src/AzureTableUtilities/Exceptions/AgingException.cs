using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Class for aging exceptions.
    /// </summary>
    public class AgingException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public AgingException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AgingException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public AgingException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

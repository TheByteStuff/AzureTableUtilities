using System;
using System.Collections.Generic;
using System.Text;

namespace TheByteStuff.AzureTableUtilities.Exceptions
{
    /// <summary>
    /// Class for Parameter spec exceptions.
    /// </summary>
    public class ParameterSpecException : AzureTableUtilitiesException
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public ParameterSpecException()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ParameterSpecException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ParameterSpecException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

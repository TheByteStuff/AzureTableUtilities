using System;
using System.Collections.Generic;
using System.Text;
using System.Security;

namespace TheByteStuff.AzureTableUtilities
{
    /// <summary>
    /// Common method for AzureTableUtilities
    /// </summary>
    public class Helper
    {
        /// <summary>
        /// Test if SecureString is null or empty.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool IsSecureStringNullOrEmpty(SecureString value)
        {
            if ((null==value) || (value.Length==0))
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Exceptions
{
    public class PropertyNotListableException : Exception
    {
        public PropertyNotListableException(string propertyName) 
            : base(string.Format("Property [{0}] manually informed is not marked as listable.", propertyName))
        {
        }
    }
}

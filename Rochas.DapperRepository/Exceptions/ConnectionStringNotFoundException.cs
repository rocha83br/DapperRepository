using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Exceptions
{
    public sealed class ConnectionStringNotFoundException : Exception
    {
        public ConnectionStringNotFoundException()
            : base("ConnectionString not found.")
        {
        }
    }
}

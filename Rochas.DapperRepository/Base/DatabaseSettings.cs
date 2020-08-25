using System;

namespace Rochas.DapperRepository.Base
{
    public abstract class DataBaseSettings
    {
        #region Declarations

        protected string _connString;
        protected string[] _replicaConnStrings;
        protected string _logPath;

        #endregion

        #region Public Properties

        protected bool replicationEnabled
        {
            get
            {
                return ((_replicaConnStrings != null)
                    && (_replicaConnStrings.Length > 0));
            }
        }

        #endregion

        #region Constructors

        protected DataBaseSettings(string connectionString, string logPath, params string[] replicaConnStrings)
        {
            _connString = connectionString;

            if (replicaConnStrings != null)
                _replicaConnStrings = replicaConnStrings;

            _logPath = logPath;
        }

        #endregion
    }
}

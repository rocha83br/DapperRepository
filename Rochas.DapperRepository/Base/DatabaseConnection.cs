using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using MySqlConnector;
using Rochas.DapperRepository.Helpers.SQL;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Enums;

namespace Rochas.DapperRepository.Base
{
    public class DataBaseConnection : DataBaseSettings, IDisposable
    {
        #region Declarations

        private DatabaseEngine engine;
        protected bool keepConnection = false;
        protected IDbConnection connection;
        protected IDbTransaction transactionControl;
        
        #endregion
        
        #region Constructors

        public DataBaseConnection(DatabaseEngine databaseEngine, string connectionString, string logPath = null, bool keepConnected = false, params string[] replicaConnStrings) : base(connectionString, logPath, replicaConnStrings)
        {
            engine = databaseEngine;

            keepConnection = keepConnected;
            if (keepConnection) Connect();
        }

        #endregion

        #region Public Methods

        public void StartTransaction()
        {
            if (connection.State != ConnectionState.Open)
                Connect();

            this.transactionControl = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if ((connection.State == ConnectionState.Open)
                && (transactionControl != null))
                transactionControl.Commit();
        }

        public void CancelTransaction()
        {
            if ((connection.State == ConnectionState.Open)
                && (transactionControl != null))
                transactionControl.Rollback();
        }

        public void Dispose()
        {
            connection.Dispose();

            if (transactionControl != null)
                transactionControl.Dispose();

            GC.ReRegisterForFinalize(this);
        }

        #endregion

        #region Helper Methods

        protected bool Connect(string optionalConnConfig = "")
        {
            if (!string.IsNullOrEmpty(_connString) || !string.IsNullOrEmpty(optionalConnConfig))
            {
                if (engine == DatabaseEngine.SQLServer)
                    connection = new SqlConnection();
                else
                    connection = new MySqlConnection();

                if ((connection.State != ConnectionState.Open) && (connection.State != ConnectionState.Connecting))
                {
                    if (!string.IsNullOrEmpty(optionalConnConfig))
                        connection.ConnectionString = optionalConnConfig;
                    else
                        connection.ConnectionString = _connString;

                    connection.Open();
                }
            }
            else
                throw new ConnectionStringNotFoundException();

            return (connection.State == ConnectionState.Open);
        }

        protected bool Disconnect()
        {
            if (connection.State == ConnectionState.Open)
                connection.Close();

            return (connection.State == ConnectionState.Closed);
        }

        protected IEnumerable<object> ExecuteQuery(Type entityType, string sqlInstruction)
        {
            IEnumerable<object> result = null;

            if (connection.State != ConnectionState.Open)
                Connect();

            result = connection.Query(entityType, sqlInstruction);

            return result;
        }

        protected async Task<IEnumerable<object>> ExecuteQueryAsync(Type entityType, string sqlInstruction)
        {
            IEnumerable<object> result = null;

            if (connection.State != ConnectionState.Open)
                Connect();

            result = await connection.QueryAsync(entityType, sqlInstruction);

            return result;
        }

        protected int ExecuteCommand(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            IDbCommand sqlCommand;
            string insertCommand = SQLStatements.SQL_ReservedWord_INSERT;

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                int insertedKey = 0;
                if (sqlCommand.CommandText.Contains(insertCommand))
                {
                    sqlCommand.ExecuteNonQuery();
                    sqlCommand.CommandText = SQLStatements.SQL_Action_GetLastId;
                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out insertedKey);
                    executionReturn = insertedKey;
                }
                else
                    executionReturn = sqlCommand.ExecuteNonQuery();

                sqlCommand = null;
            }

            return executionReturn;
        }

        protected async Task<int> ExecuteCommandAsync(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            IDbCommand sqlCommand;
            string insertCommand = SQLStatements.SQL_ReservedWord_INSERT;

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                int insertedKey = 0;
                if (sqlCommand.CommandText.Contains(insertCommand))
                {
                    sqlCommand.ExecuteNonQuery();
                    var command = SQLStatements.SQL_Action_GetLastId;
                    var lastId = await connection.ExecuteScalarAsync(command);
                    int.TryParse(lastId.ToString(), out insertedKey);
                    executionReturn = insertedKey;
                }
                else
                    executionReturn = await connection.ExecuteAsync(sqlInstruction);

                sqlCommand = null;
            }

            return executionReturn;
        }

        private IDbCommand CompositeCommand(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInstruction;

            if ((transactionControl != null)
                    && (transactionControl.Connection != null))
                sqlCommand.Transaction = transactionControl;

            if (parameters != null)
            {
                sqlCommand.Parameters.Clear();
                foreach (var param in parameters)
                {
                    SqlParameter newSqlParameter = new SqlParameter(param.Key.ToString(), param.Value);
                    sqlCommand.Parameters.Add(newSqlParameter);
                }
            }

            if (transactionControl != null)
                sqlCommand.Transaction = transactionControl;

            return sqlCommand;
        }

        #endregion
    }
}

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
        private readonly string insertCommand = SQLStatements.SQL_ReservedWord_INSERT;
        private readonly string countCommand = SQLStatements.SQL_ReservedWord_COUNT;

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
            if ((connection == null) 
                || (connection.State != ConnectionState.Open))
                keepConnection = Connect();

            this.transactionControl = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if ((connection != null) && (connection.State == ConnectionState.Open)
                && (transactionControl != null))
            {
                transactionControl.Commit();
                keepConnection = false;
            }
        }

        public void CancelTransaction()
        {
            if ((connection != null) && (connection.State == ConnectionState.Open)
                && (transactionControl != null))
            {
                transactionControl.Rollback();
                keepConnection = false;
            }
        }

        public void Dispose()
        {
            if (connection != null)
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

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                if (sqlCommand.CommandText.StartsWith(insertCommand)
                    || sqlCommand.CommandText.Contains(countCommand))
                {
                    if (sqlCommand.CommandText.StartsWith(insertCommand))
                    {
                        sqlCommand.ExecuteNonQuery();
                        sqlCommand.CommandText = SQLStatements.SQL_Action_GetLastId;
                    }

                    int scalarReturn;
                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out scalarReturn);
                    executionReturn = scalarReturn;
                }
                else
                    executionReturn = sqlCommand.ExecuteNonQuery();
            }

            return executionReturn;
        }

        protected async Task<int> ExecuteCommandAsync(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            IDbCommand sqlCommand;

            int executionReturn = 0;

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                if (sqlCommand.CommandText.StartsWith(insertCommand)
                    || sqlCommand.CommandText.Contains(countCommand))
                {
                    if (sqlCommand.CommandText.StartsWith(insertCommand))
                    {
                        sqlCommand.ExecuteNonQuery();
                        sqlCommand.CommandText = SQLStatements.SQL_Action_GetLastId;
                    }

                    int scalarReturn;
                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out scalarReturn);
                    executionReturn = scalarReturn;
                }
                else
                    executionReturn = await connection.ExecuteAsync(sqlInstruction);
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

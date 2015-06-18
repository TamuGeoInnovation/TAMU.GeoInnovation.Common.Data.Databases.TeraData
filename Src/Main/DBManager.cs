using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace USC.GISResearchLab.Common.Utils.Databases
{
    public sealed class DBManager : IDBManager, IDisposable
    {
        #region Properties
        private IDbConnection _IDbConnection;
        private IDataReader _IDataReader;
        private IDbCommand _IDbCommand;
        private DataProvider _ProviderType;
        private IDbTransaction _IDbTransaction = null;
        private IDbDataParameter[] _IDbParameters = null;
        private string _ConnectionString;

        public IDbConnection Connection
        {
            get
            {
                return _IDbConnection;
            }
            set
            {
                _IDbConnection = value;
            }
        }

        public IDataReader DataReader
        {
            get
            {
                return _IDataReader;
            }
            set
            {
                _IDataReader = value;
            }
        }

        public DataProvider ProviderType
        {
            get
            {
                return _ProviderType;
            }
            set
            {
                _ProviderType = value;
            }
        }

        public string ConnectionString
        {
            get
            {
                return _ConnectionString;
            }
            set
            {
                _ConnectionString = value;
            }
        }

        public IDbCommand Command
        {
            get
            {
                return _IDbCommand;
            }
            set
            {
                _IDbCommand = value;
            }
        }

        public IDbTransaction Transaction
        {
            get
            {
                return _IDbTransaction;
            }
            set
            {
                Transaction = value;
            }
        }

        public IDbDataParameter[] Parameters
        {
            get
            {
                return _IDbParameters;
            }
            set
            {
                _IDbParameters = value;
            }
        }
        #endregion

        public DBManager()
        {

        }

        public DBManager(DataProvider providerType)
        {
            ProviderType = providerType;
        }

        public DBManager(DataProvider providerType, string connectionString)
        {
            ProviderType = providerType;
            ConnectionString = connectionString;
        }

        public void Open()
        {
            Connection = DBManagerFactory.GetConnection(ProviderType);
            Connection.ConnectionString = ConnectionString;

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }

            Command = DBManagerFactory.GetCommand(ProviderType);
        }

        public void Close()
        {
            if (Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Close();
            Command = null;
            Transaction = null;
            Connection = null;
        }

        public void CreateParameters(int paramsCount)
        {
            Parameters = new IDbDataParameter[paramsCount];
            Parameters = DBManagerFactory.GetParameters(ProviderType, paramsCount);
        }

        public void AddParameters(int index, string paramName, object objValue)
        {
            if (index < Parameters.Length)
            {
                Parameters[index].ParameterName = paramName;
                Parameters[index].Value = objValue;
            }
        }

        public void BeginTransaction()
        {
            if (Transaction == null)
            {
                Transaction = DBManagerFactory.GetTransaction(ProviderType);
            }
            Command.Transaction = _IDbTransaction;
        }

        public void CommitTransaction()
        {
            if (Transaction != null)
            {
                Transaction.Commit();
            }
            Transaction = null;
        }

        public IDataReader ExecuteReader(CommandType commandType, string commandText)
        {
            Command = DBManagerFactory.GetCommand(ProviderType);
            Command.Connection = Connection;
            PrepareCommand(Command, Connection, Transaction, commandType, commandText, Parameters);
            DataReader = Command.ExecuteReader();
            Command.Parameters.Clear();
            return DataReader;
        }

        public void CloseReader()
        {
            if (DataReader != null)
            {
                DataReader.Close();
            }
        }

        private void AttachParameters(IDbCommand command, IDbDataParameter[] commandParameters)
        {
            foreach (IDbDataParameter idbParameter in commandParameters)
            {
                if ((idbParameter.Direction == ParameterDirection.InputOutput) && (idbParameter.Value == null))
                {
                    idbParameter.Value = DBNull.Value;
                }
                command.Parameters.Add(idbParameter);
            }
        }

        private void PrepareCommand(IDbCommand command, IDbConnection connection, IDbTransaction transaction, CommandType commandType, string commandText, IDbDataParameter[] commandParameters)
        {
            command.Connection = connection;
            command.CommandText = commandText;
            command.CommandType = commandType;

            if (transaction != null)
            {
                command.Transaction = transaction;
            }

            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
        }

        public int ExecuteNonQuery(CommandType commandType, string commandText)
        {
            Command = DBManagerFactory.GetCommand(ProviderType);
            PrepareCommand(Command, Connection, Transaction, commandType, commandText, Parameters);
            int returnValue = Command.ExecuteNonQuery();
            Command.Parameters.Clear();
            return returnValue;
        }

        public object ExecuteScalar(CommandType commandType, string commandText)
        {
            Command = DBManagerFactory.GetCommand(ProviderType);
            PrepareCommand(Command, Connection, Transaction, commandType, commandText, Parameters);
            object returnValue = Command.ExecuteScalar();
            Command.Parameters.Clear();
            return returnValue;
        }

        public DataSet ExecuteDataSet(CommandType commandType, string commandText)
        {
            Command = DBManagerFactory.GetCommand(ProviderType);
            PrepareCommand(Command, Connection, Transaction, commandType, commandText, Parameters);
            IDbDataAdapter dataAdapter = DBManagerFactory.GetDataAdapter(ProviderType);
            dataAdapter.SelectCommand = Command;
            DataSet dataSet = new DataSet();
            dataAdapter.Fill(dataSet);
            Command.Parameters.Clear();
            return dataSet;
        }

        public DataTable ExecuteDataTable(CommandType commandType, string commandText)
        {
            DataSet dataSet = ExecuteDataSet(commandType, commandText);
            return dataSet.Tables[0];
        }
    }
}

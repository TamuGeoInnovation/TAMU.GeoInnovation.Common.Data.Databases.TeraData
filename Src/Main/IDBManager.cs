using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace USC.GISResearchLab.Common.Utils.Databases
{
    // from http://www.dotnetjohn.com/articles.aspx?articleid=244

    public enum DataProvider
    {
        Oracle, SqlServer, OleDb, Odbc, MySql, Shapefile
    }

    public interface IDBManager
    {
        #region Properties
        DataProvider ProviderType
        {
            get;
            set;
        }

        string ConnectionString
        {
            get;
            set;
        }

        IDbConnection Connection
        {
            get;
        }
        IDbTransaction Transaction
        {
            get;
        }

        IDataReader DataReader
        {
            get;
        }
        IDbCommand Command
        {
            get;
        }

        IDbDataParameter[] Parameters
        {
            get;
        }
        #endregion

        void Open();
        void BeginTransaction();
        void CommitTransaction();
        void CreateParameters(int paramsCount);
        void AddParameters(int index, string paramName, object objValue);
        IDataReader ExecuteReader(CommandType commandType, string commandText);
        DataSet ExecuteDataSet(CommandType commandType, string commandText);
        object ExecuteScalar(CommandType commandType, string commandText);
        int ExecuteNonQuery(CommandType commandType, string commandText);
        void CloseReader();
        void Close();
        void Dispose();
    }
}

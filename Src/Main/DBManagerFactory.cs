using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data.Odbc;

namespace USC.GISResearchLab.Common.Utils.Databases
{
    // from http://www.dotnetjohn.com/articles.aspx?articleid=244
    public sealed class DBManagerFactory
    {
        private DBManagerFactory() { }
        public static IDbConnection GetConnection(DataProvider providerType)
        {
            IDbConnection iDbConnection = null;
            switch (providerType)
            {
                case DataProvider.SqlServer:
                    iDbConnection = new SqlConnection();
                    break;
                case DataProvider.OleDb:
                    iDbConnection = new OleDbConnection();
                    break;
                case DataProvider.Odbc:
                    iDbConnection = new OdbcConnection();
                    break;
                case DataProvider.Oracle:
                    //iDbConnection = new OracleConnection();
                    break;
                default:
                    return null;
            }
            return iDbConnection;
        }

        public static IDbCommand GetCommand(DataProvider providerType)
        {
            switch (providerType)
            {
                case DataProvider.SqlServer:
                    return new SqlCommand();
                case DataProvider.OleDb:
                    return new OleDbCommand();
                case DataProvider.Odbc:
                    return new OdbcCommand();
                case DataProvider.Oracle:
                    //return new OracleCommand();
                default:
                    return null;
            }
        }

        public static IDbDataAdapter GetDataAdapter(DataProvider providerType)
        {
            switch (providerType)
            {
                case DataProvider.SqlServer:
                    return new SqlDataAdapter();
                case DataProvider.OleDb:
                    return new OleDbDataAdapter();
                case DataProvider.Odbc:
                    return new OdbcDataAdapter();
                case DataProvider.Oracle:
                    //return new OracleDataAdapter();
                default:
                    return null;
            }
        }

        public static IDbTransaction GetTransaction(DataProvider providerType)
        {
            IDbConnection iDbConnection = GetConnection(providerType);
            IDbTransaction iDbTransaction = iDbConnection.BeginTransaction();
            return iDbTransaction;
        }

        public static IDataParameter GetParameter(DataProvider providerType)
        {
            IDataParameter iDataParameter = null;
            switch (providerType)
            {
                case DataProvider.SqlServer:
                    iDataParameter = new SqlParameter();
                    break;
                case DataProvider.OleDb:
                    iDataParameter = new OleDbParameter();
                    break;
                case DataProvider.Odbc:
                    iDataParameter = new OdbcParameter();
                    break;
                case DataProvider.Oracle:
                    //iDataParameter = newOracleParameter();
                    break;

            }
            return iDataParameter;
        }

        public static IDbDataParameter[] GetParameters(DataProvider providerType, int paramsCount)
        {
            IDbDataParameter[] idbParams = new IDbDataParameter[paramsCount];

            switch (providerType)
            {
                case DataProvider.SqlServer:
                    for (int i = 0; i < paramsCount; ++i)
                    {
                        idbParams[i] = new SqlParameter();
                    }
                    break;
                case DataProvider.OleDb:
                    for (int i = 0; i < paramsCount; ++i)
                    {
                        idbParams[i] = new OleDbParameter();
                    }
                    break;
                case DataProvider.Odbc:
                    for (int i = 0; i < paramsCount; ++i)
                    {
                        idbParams[i] = new OdbcParameter();
                    }
                    break;
                case DataProvider.Oracle:
                    for (int i = 0; i < paramsCount; ++i)
                    {
                        //idbParams[i] = new OracleParameter();
                    }
                    break;
                default:
                    idbParams = null;
                    break;
            }
            return idbParams;
        }
    }
}

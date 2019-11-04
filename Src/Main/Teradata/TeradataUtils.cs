using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace USC.GISResearchLab.Common.Databases.Teradata
{
    public class TeradataUtils
    {

        public static string MAX_LENGTH_STRING = "(MAX)";
        public static int MAX_LENGTH_INT = 99999999;

        public static bool StoredProcedureExists(SqlConnection conn, string storedProcedureName, bool shouldOpenAndClose)
        {
            bool ret = false;
            try
            {
                string database = conn.Database;
                string sql = "SELECT 1 FROM sysobjects WHERE NAME = '" + storedProcedureName + "' AND Type = 'p'";

                object o = DoSelectScalar(conn, new SqlCommand(sql), shouldOpenAndClose);
                if (o != null && o != DBNull.Value)
                {
                    ret = true;
                }

            }
            catch (Exception ex)
            {
                string msg = "Error testing stored procedure: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public static void CreateStoredProcedure(SqlConnection conn, string storedProcedureName, string select, string from, string where, string[] parameters, bool shouldOpenAndClose)
        {
            try
            {

                string sql = "";
                sql += "CREATE PROCEDURE [" + storedProcedureName + "]";
                if (parameters != null)
                {
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (i > 0)
                        {
                            sql += ", ";
                        }

                        sql += "@" + parameters[i] + " varchar(255)";
                    }
                }
                sql += " AS ";
                sql += select + " " + from + " " + where;

                DoNonQuery(conn, new SqlCommand(sql), shouldOpenAndClose);

            }
            catch (Exception ex)
            {
                string msg = "Error creating table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }


        public static void CreateTable(SqlConnection conn, string tableName, string createTableSql, bool dropIfExists, bool shouldOpenAndClose)
        {
            try
            {
                if (dropIfExists)
                {
                    string dropSql = " IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "') DROP TABLE [" + tableName + "]; ";
                    createTableSql = dropSql + createTableSql;
                }

                DoNonQuery(conn, new SqlCommand(createTableSql), shouldOpenAndClose);

            }
            catch (Exception ex)
            {
                string msg = "Error creating table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public static int DoSelectMaxId(SqlConnection conn, string table, bool shouldOpenAndClose)
        {
            return DoSelectMaxId(conn, null, table, shouldOpenAndClose);
        }

        public static int DoSelectMaxId(SqlConnection conn, SqlTransaction tx, string table, bool shouldOpenAndClose)
        {
            int ret = -1;
            try
            {
                SqlCommand cmd = new SqlCommand("SELECT MAX(ID) FROM " + table);
                if (tx != null)
                {
                    cmd.Transaction = tx;
                }
                ret = Convert.ToInt32(DoSelectScalar(conn, cmd, shouldOpenAndClose));
            }
            catch (Exception ex)
            {
                string msg = "Error getting max: " + ex.Message;
                throw new Exception(msg, ex);
            }

            return ret;
        }

        public static object DoSelectScalar(SqlConnection conn, SqlCommand cmd, bool shouldOpenAndClose)
        {
            object ret = null;
            try
            {

                if (shouldOpenAndClose)
                {
                    conn.Open();
                }
                cmd.Connection = conn;
                ret = cmd.ExecuteScalar();

            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }

                string msg = "Error getting scalar: " + ex.Message;
                throw new Exception(msg, ex);
            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }
            }
            return ret;
        }

        public static DataTable DoSelectRows(SqlConnection conn, SqlCommand cmd, bool shouldOpenAndClose)
        {
            DataTable ret;
            SqlDataAdapter dataAdapter;

            try
            {

                if (shouldOpenAndClose)
                {
                    conn.Open();
                }
                cmd.Connection = conn;

                ret = new DataTable();
                dataAdapter = new SqlDataAdapter();
                dataAdapter.SelectCommand = cmd;
                dataAdapter.Fill(ret);


            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }

                string msg = "Error getting rows as datatable: " + ex.Message;
                throw new Exception(msg, ex);
            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }
            }
            return ret;
        }

        public static void DoNonQuery(SqlConnection conn, SqlCommand cmd, bool shouldOpenAndClose)
        {
            try
            {
                if (shouldOpenAndClose)
                {
                    conn.Open();
                }
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }

                string msg = "Error doing nonquery: " + ex.Message;
                throw new Exception(msg, ex);
            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }
            }
        }

        public static SqlConnection GetConnection(string dataSource, string catalog, string userName, string password)
        {
            SqlConnection ret = null;
            try
            {
                string connectionString = "Data Source=" + dataSource + "; Initial Catalog=" + catalog + ";uid=" + userName + ";pwd=" + password;
                ret = new SqlConnection(connectionString);
            }
            catch (Exception ex)
            {

                string msg = "Error getting sql server database connection: " + ex.Message;
            }
            return ret;
        }

        public static SqlConnection GetConnection(string connectionString)
        {
            SqlConnection ret = null;
            try
            {
                ret = new SqlConnection(connectionString);
            }
            catch (Exception ex)
            {
                string msg = "Error getting sql server database connection: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }



        public static string[] GetPrimaryKeyField(SqlConnection conn, string tableName, bool shouldOpenAndClose)
        {

            string[] ret = null;

            try
            {
                if (shouldOpenAndClose)
                {
                    conn.Open();
                }

                DataTable dataTable = conn.GetSchema(SqlClientMetaDataCollectionNames.IndexColumns, new string[] { null, null, tableName });

                if (dataTable != null)
                {

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        // row values from http://msdn2.microsoft.com/en-us/library/ms254969.aspx
                        DataRow dataRow = dataTable.Rows[i];
                        string columnName = Convert.ToString(dataRow["column_name"]);
                        string indexName = dataRow["index_name"].ToString();
                        bool isPrimaryKey = dataRow["constraint_name"].ToString().StartsWith("PK");
                        if (isPrimaryKey)
                        {
                            ret = new string[2];
                            ret[0] = columnName;
                            ret[1] = indexName;
                            break;
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    conn.Close();
                }

                string msg = "Error getting primary key field: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    conn.Close();
                }
            }

            return ret;
        }

        public static bool TableExists(SqlConnection conn, string catalog, string table, bool shouldOpenAndClose)
        {
            bool ret = false;
            try
            {


                if (table == null || table.Equals(""))
                {
                    throw new Exception("table parameter is null or empty");
                }

                string sql = "";
                sql += "IF EXISTS (SELECT 1 FROM [" + catalog + "].INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME='" + table + "')";
                sql += " SELECT 'true' ELSE SELECT 'false' ";

                ret = Convert.ToBoolean(DoSelectScalar(conn, new SqlCommand(sql), shouldOpenAndClose));

            }
            catch (Exception ex)
            {
                string msg = "Error testing table exists: " + ex.Message;
                throw new Exception(msg, ex);
            }


            return ret;
        }


        public static ArrayList GetTablesAsArrayList(SqlConnection conn, bool shouldOpenAndClose)
        {
            return GetTables(conn, shouldOpenAndClose);
        }

        public static ArrayList GetTables(SqlConnection conn, bool shouldOpenAndClose)
        {
            ArrayList ret = null;

            try
            {
                if (shouldOpenAndClose)
                {
                    conn.Open();
                }

                DataTable schemaTable = GetTables(conn);
                if (schemaTable != null)
                {
                    ret = new ArrayList();
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        ret.Add(schemaTable.Rows[i].ItemArray[2].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    conn.Close();
                }
                string msg = "Error getting table names: " + ex.Message;
                throw new Exception(msg, ex);
            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    conn.Close();
                }
            }
            return ret;
        }

        public static DataTable GetTables(SqlConnection conn)
        {
            DataTable ret = null;
            try
            {
                //string sql = "select * from sysobjects where type='u'";
                //SqlCommand cmd = new SqlCommand(sql);
                //ret = DoSelectRows(conn, cmd, false);

                SqlDataAdapter da = new SqlDataAdapter("SELECT * FROM Information_Schema.Tables where Table_Type = 'BASE TABLE'", conn);
                ret = new DataTable();
                da.Fill(ret);

            }
            catch (Exception ex)
            {
                if (conn != null)
                {
                    conn.Close();
                }

                string msg = "Error getting table schema: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (conn != null)
                {
                    conn.Close();
                }
            }
            return ret;
        }


    }
}

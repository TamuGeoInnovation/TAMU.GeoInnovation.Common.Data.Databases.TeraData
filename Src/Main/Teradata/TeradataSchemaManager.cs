using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Teradata.Client.Provider;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Core.Utils.Arrays;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.SqlServer;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Databases.TypeConverters.DataProviderTypeConverters;
using USC.GISResearchLab.Common.Utils.Databases;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;

namespace USC.GISResearchLab.Common.Databases.Teradata
{
   

    public class TeradataSchemaManager : AbstractSchemaManager
    {
        public static string MAX_LENGTH_STRING = "(MAX)";
        public static int MAX_LENGTH_INT = 99999999;

        public TeradataSchemaManager()
        {
            DataProviderType = DataProviderType.Teradata;
            DatabaseType = DatabaseType.Teradata;
            QueryManager = new QueryManager(DataProviderType, DatabaseType);
        }

        public TeradataSchemaManager(string connectionString)
        {
            DataProviderType = DataProviderType.Teradata;
            DatabaseType = DatabaseType.Teradata;
            ConnectionString = connectionString;
            QueryManager = new QueryManager(DataProviderType, DatabaseType, ConnectionString);
        }

        public override void CreateDatabase()
        {
            throw new NotImplementedException();
        }

        public override TableDefinition GetTableDefinition(string table)
        {
            TableDefinition ret = null;
            try
            {
                TableColumn[] tableColumns = GetColumns(table);
                if (tableColumns != null)
                {
                    if (ret == null)
                    {
                        ret = new TableDefinition(DataProviderType, table);
                    }
                    ret.TableColumns = tableColumns;
                }
            }
            catch (Exception ex)
            {

                string msg = "Error getting table definition: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public override void RemoveTableFromDatabase(string tableName)
        {
            try
            {
                string sql = "";
                sql += " use " + ((TdConnection)Connection).Database + "; ";
                sql += " IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "') DROP TABLE [" + tableName + "]; ";
                QueryManager.ExecuteNonQuery(CommandType.Text, sql);

            }
            catch (Exception ex)
            {
                string msg = "Error removing table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void RemoveIndexFromTable(string tableName, string indexName)
        {
            try
            {
                string sql = "";
                sql += " use " + ((TdConnection)Connection).Database + "; ";
                sql += " IF EXISTS (SELECT * FROM sys.indexes WHERE OBJECT_ID = OBJECT_ID('" + tableName + "') AND name = '" + indexName + "' ) DROP INDEX [dbo].[" + tableName + "].[" + indexName + "]; ";
                QueryManager.ExecuteNonQuery(CommandType.Text, sql);

            }
            catch (Exception ex)
            {
                string msg = "Error RemoveIndexFromTable: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void RemoveSpatialIndexFromTable(string tableName, string indexName)
        {
            try
            {
                string sql = "";
                sql += " use " + ((TdConnection)Connection).Database + "; ";
                sql += " IF EXISTS (SELECT * FROM sys.indexes WHERE OBJECT_ID = OBJECT_ID('" + tableName + "') AND name = '" + indexName + "' ) DROP INDEX [" + indexName + "] ON [dbo].[" + tableName + "]; ";
                QueryManager.ExecuteNonQuery(CommandType.Text, sql);

            }
            catch (Exception ex)
            {
                string msg = "Error RemoveIndexFromTable: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void RemoveConstraintFromTable(string tableName, string constraintName)
        {
            try
            {
                string sql = "";
                sql += " use " + ((TdConnection)Connection).Database + "; ";
                sql += " IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='dbo' AND TABLE_NAME='" + tableName + "' AND CONSTRAINT_NAME='" + constraintName + "'  ) ALTER TABLE [dbo].[" + tableName + "] DROP CONSTRAINT [" + constraintName + "]; ";
                QueryManager.ExecuteNonQuery(CommandType.Text, sql);

            }
            catch (Exception ex)
            {
                string msg = "Error RemoveConstraintFromTable: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

		public override TableColumn[] GetColumns(string table)
		{
            if (Connection.State == ConnectionState.Open)
            {
                return GetColumns(table, false);
            }
            else
            {
                return GetColumns(table, true);
            }
		}

        public override TableColumn[] GetColumns(string databaseName, string table)
        {
            if (Connection.State == ConnectionState.Open)
            {
                return GetColumns(databaseName, table, false);
            }
            else
            {
                return GetColumns(databaseName, table, true);
            }
        }

        public override TableColumn[] GetColumns(string table, bool shouldOpenAndClose)
        {

            //return GetColumnsWithSQLQuery(table, shouldOpenAndClose);
            //return GetColumnsWithGetSchema(table, shouldOpenAndClose);
            return GetColumnsWithQuery(null, table, shouldOpenAndClose);

        }

        public override TableColumn[] GetColumns(string datbaseName, string table, bool shouldOpenAndClose)
        {
            //return GetColumnsWithSQLQuery(datbaseName, table, shouldOpenAndClose);
            //return GetColumnsWithGetSchema(datbaseName, table, shouldOpenAndClose);
            return GetColumnsWithQuery(datbaseName, table, shouldOpenAndClose);
        }

        public TableColumn[] GetColumnsWithGetSchema(string table, bool shouldOpenAndClose)
        {
            TableColumn[] ret = null;
            TdDataReader reader = null;
            try
            {
                table = table.Trim();

                // table name must not be surrounded with brackets
                if (table.StartsWith("["))
                {
                    table = table.Substring(1);
                }

                if (table.EndsWith("]"))
                {
                    table = table.Substring(0, table.Length - 1);
                }

                table = table.Trim();

                string[] primaryKey = GetPrimaryKeyField(table, shouldOpenAndClose);

                string[] restrictions = new string[3];
                restrictions[0] = Connection.Database;
                restrictions[1] = "dbo";
                restrictions[2] = table;

                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                DataTable schemaTable = ((TdConnection)Connection).GetSchema(SqlClientMetaDataCollectionNames.Columns, restrictions);

                if (schemaTable != null)
                {
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        // row values from http://msdn2.microsoft.com/en-us/library/ms254969.aspx
                        DataRow columnRow = schemaTable.Rows[i];
                        string name = Convert.ToString(columnRow["column_name"]);
                        object defaultValue = columnRow["column_default"];
                        string isNullable = Convert.ToString(columnRow["is_nullable"]);
                        string dbType = Convert.ToString((columnRow["data_type"]));
                        string maxCharLength = Convert.ToString(columnRow["character_maximum_length"]);

                        int maxOctetLength = -1;
                        if (columnRow["character_octet_length"] != DBNull.Value)
                        {
                            maxOctetLength = Convert.ToInt32(columnRow["character_octet_length"]);
                        }

                        int numericPrecision = -1;
                        if (columnRow["numeric_precision"] != DBNull.Value)
                        {
                            numericPrecision = Convert.ToInt32(columnRow["numeric_precision"]);
                        }


                        IDataProviderTypeConverterManager typeConverter = DataProviderTypeConverterManagerFactory.GetDataProviderTypeConverterManager(DataProviderType);
                        DatabaseSuperDataType databaseSuperDataType = typeConverter.ToSuperTypeFromdbTypeString(dbType);
                        TableColumn column = new TableColumn(name, databaseSuperDataType, defaultValue);

                        if (primaryKey != null)
                        {
                            if (String.Compare(primaryKey[0], name, true) == 0)
                            {
                                column.IsPrimaryKey = true;
                            }
                        }

                        if (dbType.IndexOf("char") > -1)
                        {
                            if (maxCharLength != null && maxCharLength.ToString() != String.Empty)
                            {
                                if (maxCharLength == "-1")
                                {
                                    column.Length = MAX_LENGTH_INT;
                                }
                                else if (Convert.ToInt32(maxCharLength) > 8000)
                                {
                                    column.Length = MAX_LENGTH_INT;
                                }
                                else
                                {
                                    column.Length = Convert.ToInt32(maxCharLength);
                                }
                            }
                        }
                        else if (dbType.IndexOf("decimal") > -1)
                        {
                            if (numericPrecision > 0)
                            {
                                column.Precision = numericPrecision;
                            }

                            if (maxOctetLength > 0)
                            {
                                column.MaxOctetLength = maxOctetLength;
                            }
                        }

                        if (String.Compare(isNullable, "no", true) == 0)
                        {
                            column.IsNullable = false;
                        }

                        if (ret == null)
                        {
                            ret = new TableColumn[1];
                            ret[0] = column;
                        }
                        else
                        {
                            TableColumn[] temp = new TableColumn[ret.Length + 1];
                            Array.Copy(ret, temp, ret.Length);
                            temp[temp.Length - 1] = column;
                            ret = temp;
                        }
                    }
                }
            }

            catch (Exception ex)
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting columns: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        public TableColumn[] GetColumnsWithGetSchema(string databaseName, string table, bool shouldOpenAndClose)
        {
            TableColumn[] ret = null;
            TdDataReader reader = null;
            try
            {
                table = table.Trim();

                // table name must not be surrounded with brackets
                if (table.StartsWith("["))
                {
                    table = table.Substring(1);
                }

                if (table.EndsWith("]"))
                {
                    table = table.Substring(0, table.Length - 1);
                }

                table = table.Trim();

                //string[] primaryKey = GetPrimaryKeyField(databaseName, table, shouldOpenAndClose);

                string[] restrictions = new string[3];
                restrictions[0] = databaseName;
                restrictions[1] = table;
                restrictions[2] = null;

                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                //DataTable schemaTable = ((TdConnection)Connection).GetSchema("Columns");
                DataTable schemaTable = ((TdConnection)Connection).GetSchema("Columns", restrictions);


                string tableString = DisplayDataTable(schemaTable);

                if (schemaTable != null)
                {
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        DataRow columnRow = schemaTable.Rows[i];

                        string columnDatabaseName = Convert.ToString(columnRow["TABLE_SCHEMA"]);
                        string columnTableName = Convert.ToString(columnRow["TABLE_NAME"]);

                        if (String.Compare(columnDatabaseName, databaseName, true) == 0 && String.Compare(columnTableName, table, true) == 0)
                        {

                            string name = Convert.ToString(columnRow["column_name"]);
                            object defaultValue = columnRow["column_default"];
                            string isNullable = Convert.ToString(columnRow["is_nullable"]);
                            string dbType = Convert.ToString((columnRow["COLUMN_TYPE"]));
                            string maxCharLength = Convert.ToString(columnRow["character_maximum_length"]);

                            int maxOctetLength = -1;
                            if (columnRow["character_octet_length"] != DBNull.Value)
                            {
                                maxOctetLength = Convert.ToInt32(columnRow["character_octet_length"]);
                            }

                            int numericPrecision = -1;
                            if (columnRow["numeric_precision"] != DBNull.Value)
                            {
                                numericPrecision = Convert.ToInt32(columnRow["numeric_precision"]);
                            }


                            IDataProviderTypeConverterManager typeConverter = DataProviderTypeConverterManagerFactory.GetDataProviderTypeConverterManager(DataProviderType);
                            DatabaseSuperDataType databaseSuperDataType = typeConverter.ToSuperTypeFromdbTypeString(dbType);
                            TableColumn column = new TableColumn(name, databaseSuperDataType, defaultValue);

                            //if (primaryKey != null)
                            //{
                            //    if (String.Compare(primaryKey[0], name, true) == 0)
                            //    {
                            //        column.IsPrimaryKey = true;
                            //    }
                            //}

                            if (dbType.IndexOf("char") > -1)
                            {
                                if (maxCharLength != null && maxCharLength.ToString() != String.Empty)
                                {
                                    if (maxCharLength == "-1")
                                    {
                                        column.Length = MAX_LENGTH_INT;
                                    }
                                    else if (Convert.ToInt32(maxCharLength) > 8000)
                                    {
                                        column.Length = MAX_LENGTH_INT;
                                    }
                                    else
                                    {
                                        column.Length = Convert.ToInt32(maxCharLength);
                                    }
                                }
                            }
                            else if (dbType.IndexOf("decimal") > -1)
                            {
                                if (numericPrecision > 0)
                                {
                                    column.Precision = numericPrecision;
                                }

                                if (maxOctetLength > 0)
                                {
                                    column.MaxOctetLength = maxOctetLength;
                                }
                            }

                            if (String.Compare(isNullable, "no", true) == 0)
                            {
                                column.IsNullable = false;
                            }

                            if (ret == null)
                            {
                                ret = new TableColumn[1];
                                ret[0] = column;
                            }
                            else
                            {
                                TableColumn[] temp = new TableColumn[ret.Length + 1];
                                Array.Copy(ret, temp, ret.Length);
                                temp[temp.Length - 1] = column;
                                ret = temp;
                            }
                        }
                    }
                }
            }

            catch (Exception ex)
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting columns: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        public TableColumn[] GetColumnsWithQuery(string databaseName, string table, bool shouldOpenAndClose)
        {
            TableColumn[] ret = null;
            TdDataReader reader = null;
            try
            {
                table = table.Trim();

                // table name must not be surrounded with brackets
                if (table.StartsWith("["))
                {
                    table = table.Substring(1);
                }

                if (table.EndsWith("]"))
                {
                    table = table.Substring(0, table.Length - 1);
                }

                table = table.Trim();

                //string[] primaryKey = GetPrimaryKeyField(databaseName, table, shouldOpenAndClose);

                string[] restrictions = new string[3];
                restrictions[0] = databaseName;
                restrictions[1] = table;
                restrictions[2] = null;

                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }


                // using the schema method does not work 100% of the time, actually only like 10% of the time.
                //DataTable schemaTable = ((TdConnection)Connection).GetSchema("Columns");
                //DataTable schemaTable = ((TdConnection)Connection).GetSchema("Columns", restrictions);


                string sql = "select top 1 * from " + table;
                DataTable queryResult = QueryManager.ExecuteDataTable(CommandType.Text, sql);


                DataColumnCollection columns = queryResult.Columns;
                for (int i = 0; i < columns.Count; i++)
                {
                    DataColumn dataColumn = columns[i];


                    string name = dataColumn.ColumnName;
                    object defaultValue = dataColumn.DefaultValue;
                    string isNullable = dataColumn.AllowDBNull.ToString();
                    Type dbType = dataColumn.DataType;
                    string maxCharLength = dataColumn.MaxLength.ToString();

                    // make sure to use the data table type converter rather than the teradata
                    IDataProviderTypeConverterManager typeConverter = DataProviderTypeConverterManagerFactory.GetDataProviderTypeConverterManager(DataProviderType.DataTable);
                    DatabaseSuperDataType databaseSuperDataType = typeConverter.ToSuperType(dbType);
                    TableColumn tableColumn = new TableColumn(name, databaseSuperDataType, defaultValue);


                    if (dbType == typeof(char))
                    {
                        if (maxCharLength != null && maxCharLength.ToString() != String.Empty)
                        {
                            if (maxCharLength == "-1")
                            {
                                tableColumn.Length = MAX_LENGTH_INT;
                            }
                            else if (Convert.ToInt32(maxCharLength) > 8000)
                            {
                                tableColumn.Length = MAX_LENGTH_INT;
                            }
                            else
                            {
                                tableColumn.Length = Convert.ToInt32(maxCharLength);
                            }
                        }
                    }
                    else if (dbType == typeof(decimal))
                    {
                        //if (numericPrecision > 0)
                        //{
                        //    tableColumn.Precision = numericPrecision;
                        //}

                        //if (maxOctetLength > 0)
                        //{
                        //    tableColumn.MaxOctetLength = maxOctetLength;
                        //}
                    }

                    if (String.Compare(isNullable, "false", true) == 0)
                    {
                        tableColumn.IsNullable = false;
                    }

                    if (ret == null)
                    {
                        ret = new TableColumn[1];
                        ret[0] = tableColumn;
                    }
                    else
                    {
                        TableColumn[] temp = new TableColumn[ret.Length + 1];
                        Array.Copy(ret, temp, ret.Length);
                        temp[temp.Length - 1] = tableColumn;
                        ret = temp;
                    }

                }
            }

            catch (Exception ex)
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting columns: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (reader != null)
                {
                    reader.Close();
                }

                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        public string BuildCreateTableStatementFromDataTable(DataTable dataTable, string tableName)
        {
            dataTable.TableName = tableName;
            TableDefinition tableDefinition  = TableDefinition.FromDataTable(dataTable, false);
            return BuildCreateTableStatement(tableDefinition);
        }

        public override string BuildCreateTableStatement(TableDefinition tableDefinition)
        {
            string ret = "";
            if (tableDefinition != null)
            {

                TeradataTypeConverter typeConverter = new TeradataTypeConverter();

                if (!String.IsNullOrEmpty(tableDefinition.Name))
                {
                    string outputTableName = DatabaseUtils.AsDbTableName(tableDefinition.Name.Trim(), true, false, false);
                    string outputTableNameNoBrackets = DatabaseUtils.AsDbTableName(tableDefinition.Name.Trim(), false, false, false);


                    ret += " IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + outputTableNameNoBrackets + "') DROP TABLE " + outputTableName + " ; ";


                    ret += "CREATE TABLE [dbo]." + outputTableName + " (";

                    for (int i = 0; i < tableDefinition.TableColumns.Length; i++)
                    {

                        TableColumn column = tableDefinition.TableColumns[i];
                        if (i > 0)
                        {
                            ret += ", ";
                        }

                        ret += DatabaseUtils.AsDbTableName(column.Name, true, false, false);
                        ret += "[" + DatabaseTypeConverter.GetTypeAsString(column.DatabaseSuperDataType) + "] ";

                        if (column.IsPrimaryKey)
                        {
                            ret += " IDENTITY (1,1) ";
                        }

                        if (column.DatabaseSuperDataType == DatabaseSuperDataType.Decimal || column.DatabaseSuperDataType == DatabaseSuperDataType.Money || column.DatabaseSuperDataType == DatabaseSuperDataType.SmallMoney)
                        {
                            ret += "(";

                            if (column.Length >= 0)
                            {
                                ret += column.Length;
                            }
                            else
                            {
                                ret += typeConverter.GetDefaultLength(column.DatabaseSuperDataType);
                            }

                            ret += ",";

                            if (column.Precision >= 0)
                            {
                                ret += column.Precision;
                            }
                            else
                            {
                                ret += typeConverter.GetDefaultPrecision(column.DatabaseSuperDataType);
                            }

                            ret += ") ";
                        }
                        else if (column.DatabaseSuperDataType == DatabaseSuperDataType.VarChar || column.DatabaseSuperDataType == DatabaseSuperDataType.String)
                        {
                            ret += "(";

                            if (column.Length >= 0)
                            {
                                ret += column.Length;
                            }
                            else
                            {
                                ret += typeConverter.GetDefaultLength(column.DatabaseSuperDataType);
                            }

                            ret += ") ";
                        }
                        else if (column.DatabaseSuperDataType == DatabaseSuperDataType.BigInt || column.DatabaseSuperDataType == DatabaseSuperDataType.Int32 || column.DatabaseSuperDataType == DatabaseSuperDataType.SmallInt || column.DatabaseSuperDataType == DatabaseSuperDataType.Double || column.DatabaseSuperDataType == DatabaseSuperDataType.DateTime)
                        {
                            // do nothing
                        }
                        else if (column.DatabaseSuperDataType == DatabaseSuperDataType.Geography || column.DatabaseSuperDataType == DatabaseSuperDataType.Geometry)
                        {
                            // do nothing
                        }
                        else
                        {
                            if (column.Length == MAX_LENGTH_INT)
                            {
                                ret += MAX_LENGTH_STRING + " ";
                            }
                            else
                            {
                                if (column.Length >= 0)
                                {
                                    ret += "(" + column.Length + ") ";
                                }
                            }
                        }

                        if (!column.IsNullable)
                        {
                            ret += " NOT NULL ";
                        }
                    }
                    ret += ") ;";
                }
            }
            else
            {
                throw new Exception("Table name cannot be null");
            }
            return ret;
        }

        public string GetCreateTableStatement(string tableName)
        {

            string ret = "";

            try
            {
                string[] primaryKey = GetPrimaryKeyField(tableName);

                string[] restrictions = new string[3];
                restrictions[0] = Connection.Database;
                restrictions[1] = "dbo";
                restrictions[2] = tableName;

                DataTable schemaTable = ((TdConnection)Connection).GetSchema(SqlClientMetaDataCollectionNames.Columns, restrictions);

                if (schemaTable != null)
                {
                    string outputTableName = tableName.Trim();
                    outputTableName = DatabaseUtils.AsDbTableName(outputTableName, true, false);


                    ret += "CREATE TABLE [dbo]." + outputTableName + " (";


                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        // row values from http://msdn2.microsoft.com/en-us/library/ms254969.aspx
                        DataRow columnRow = schemaTable.Rows[i];
                        string columnName = Convert.ToString(columnRow["column_name"]);
                        string columnDefault = Convert.ToString(columnRow["column_default"]);
                        string columnIsNullable = Convert.ToString(columnRow["is_nullable"]);
                        string columnDataType = Convert.ToString(columnRow["data_type"]);
                        string columnMaxCharLength = Convert.ToString(columnRow["character_maximum_length"]);

                        string columnMaxOctetLength = Convert.ToString(columnRow["character_octet_length"]);
                        string columnNumbericPrecision = Convert.ToString(columnRow["numeric_precision"]);
                        //string columnNumbericPrecisionRadix = columnRow["numeric_precision_radix"];
                        //string columnNumbericScale = columnRow["numeric_scale"];
                        //string columnDateTimePrecision = columnRow["datetime_precision"];

                        if (i > 0)
                        {
                            ret += ", ";
                        }

                        ret += "[" + columnName + "] ";
                        ret += "[" + columnDataType + "] ";

                        if (primaryKey != null)
                        {
                            if (String.Compare(primaryKey[0], columnName, true) == 0)
                            {
                                ret += " IDENTITY (1,1) ";
                            }
                        }

                        if (columnDataType.IndexOf("char") > -1)
                        {
                            if (columnMaxCharLength != null && columnMaxCharLength.ToString() != String.Empty)
                            {
                                if (columnMaxCharLength == "-1")
                                {
                                    ret += "(max) ";
                                }
                                else if (Convert.ToInt32(columnMaxCharLength) > 8000)
                                {
                                    ret += "(max) ";
                                }
                                else
                                {
                                    ret += "(" + columnMaxCharLength + ") ";
                                }
                            }
                        }
                        else if (columnDataType.IndexOf("decimal") > -1)
                        {
                            ret += "(";
                            if (columnNumbericPrecision != "")
                            {
                                ret += columnNumbericPrecision;
                            }
                            else
                            {
                                ret += "0";
                            }

                            ret += ",";

                            if (columnMaxOctetLength != "")
                            {
                                ret += columnMaxOctetLength;
                            }
                            else
                            {
                                ret += "0";
                            }

                            ret += ") ";
                        }

                        if (String.Compare(columnIsNullable, "no", true) == 0)
                        {
                            ret += " NOT NULL ";
                        }
                    }

                    ret += ")";
                }

            }
            catch (Exception ex)
            {
                string msg = "Error getting field names: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public ArrayList GetFieldNames(string tableName)
        {
            ArrayList ret = new ArrayList();

            try
            {

                DataTable schemaTable = ((TdConnection)Connection).GetSchema(SqlClientMetaDataCollectionNames.Columns, new string[] { null, null, tableName });
                for (int i = 0; i < schemaTable.Rows.Count; i++)
                {
                    ret.Add(schemaTable.Rows[i].ItemArray[3].ToString().ToUpper());
                }
            }
            catch (Exception ex)
            {


                string msg = "Error getting field names: " + ex.Message;
                throw new Exception(msg, ex);

            }


            return ret;
        }

        public void CreateTable(string tableName, string createTableSql, bool dropIfExists, bool shouldOpenAndClose)
        {
            try
            {
                if (dropIfExists)
                {
                    string dropSql = " IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "') DROP TABLE [" + tableName + "]; ";
                    createTableSql = dropSql + createTableSql;
                }

                QueryManager.ExecuteNonQuery(CommandType.Text, createTableSql);
            }
            catch (Exception ex)
            {
                string msg = "Error creating table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void AddColumnsToTable(string tableName, string[] columnNames, DatabaseSuperDataType[] dataTypes)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                AddColumnToTable(tableName, columnNames[i], dataTypes[i]);
            }
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType)
        {
            TeradataTypeConverter typeConverter = new TeradataTypeConverter();
            int defaultLength = typeConverter.GetDefaultLength(dataType);
            int defaultPrecision = typeConverter.GetDefaultPrecision(dataType);

            AddColumnToTable(tableName, columnName, dataType, true, defaultLength, defaultPrecision);
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType, bool nullable, int maxLength, int precision)
        {
            try
            {

                TeradataTypeConverter typeConverter = new TeradataTypeConverter();
                string sql = "";
                sql += " ALTER TABLE " + tableName;
                sql += " ADD ";
                sql += " " + DatabaseUtils.AsDbColumnName(columnName);
                sql += " " + typeConverter.GetTypeAsString(dataType);
                if (maxLength > 0)
                {
                    sql += " ( ";

                    if (maxLength == Int32.MaxValue)
                    {
                        sql += " MAX " ;
                    }
                    else
                    {
                        sql += " " + maxLength;
                    }

                    if (precision > 0)
                    {
                        sql += ", " + precision;
                    }

                    sql += " ) ";
                }

                if (nullable)
                {
                    sql += " NULL ";
                }
                else
                {
                    sql += " NOT NULL ";
                }

                QueryManager.ExecuteNonQuery(CommandType.Text, sql, true);
            }
            catch (Exception ex)
            {
                string msg = "Error AddColumnToTable: " + ex.Message + " - tableName: " + tableName + ", columnName: " + columnName + ", dataType: " + dataType.ToString();
                throw new Exception(msg, ex);

            }
        }

        public string BuildConnectionString(string dataSource, string catalog, string userName, string password)
        {
            return "Data Source=" + dataSource + "; Initial Catalog=" + catalog + ";uid=" + userName + ";pwd=" + password;
        }

        public string[] GetPrimaryKeyField(string table)
        {
            return GetPrimaryKeyField(table, true);
        }

        public string[] GetPrimaryKeyField(string databaseName, string table)
        {
            return GetPrimaryKeyField(databaseName, table, true);
        }

        public string[] GetPrimaryKeyField(string table, bool shouldOpenAndClose)
        {

            string[] ret = null;

            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                table = table.Trim();

                // table name must not be surrounded with brackets
                if (table.StartsWith("["))
                {
                    table = table.Substring(1);
                }

                if (table.EndsWith("]"))
                {
                    table = table.Substring(0, table.Length - 1);
                }

                table = table.Trim();

                string[] restrictions = new string[3];
                restrictions[0] = Connection.Database;
                restrictions[1] = "dbo";
                restrictions[2] = table;

                DataTable dataTable = ((TdConnection)Connection).GetSchema(SqlClientMetaDataCollectionNames.IndexColumns, restrictions);

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
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting primary key field: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        public string[] GetPrimaryKeyField(string databaseName, string table, bool shouldOpenAndClose)
        {

            string[] ret = null;

            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                table = table.Trim();

                // table name must not be surrounded with brackets
                if (table.StartsWith("["))
                {
                    table = table.Substring(1);
                }

                if (table.EndsWith("]"))
                {
                    table = table.Substring(0, table.Length - 1);
                }

                table = table.Trim();

                string[] restrictions = new string[2];
                restrictions[0] = databaseName;
                restrictions[1] = table;

                DataTable dataTable = ((TdConnection)Connection).GetSchema("Indexes", restrictions);

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
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting primary key field: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        //public override bool TableExists(string table)
        //{
        //    bool ret = false;
        //    try
        //    {


        //        if (table == null || table.Equals(""))
        //        {
        //            throw new Exception("table parameter is null or empty");
        //        }

        //        string sql = "";
        //        sql += "IF EXISTS (SELECT 1 FROM [" + ((SqlConnection)Connection).Database + "].INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME='" + table + "')";
        //        sql += " SELECT 'true' ELSE SELECT 'false' ";

        //        ret = Convert.ToBoolean(QueryManager.ExecuteScalar(CommandType.Text, sql));

        //    }
        //    catch (Exception ex)
        //    {
        //        string msg = "Error testing table exists: " + ex.Message;
        //        throw new Exception(msg, ex);
        //    }


        //    return ret;
        //}


        public ArrayList GetTablesAsArrayList()
        {
            ArrayList ret = null;

            try
            {
                DataTable schemaTable = GetTablesAsDataTable();
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
                string msg = "Error GetTablesAsArrayList: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

		public override string[] GetTables()
		{
            if (Connection.State == ConnectionState.Open)
            {
                return GetTables(false);
            }
            else
            {
                return GetTables(true);
            }
		}

        public override string[] GetTables(string database)
        {
            if (Connection.State == ConnectionState.Open)
            {
                return GetTables(database, false);
            }
            else
            {
                return GetTables(database, true);
            }
        }

        public override string[] GetTables(bool shouldOpenAndClose)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetTablesAsDataTable(shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string table = Convert.ToString(dataTable.Rows[i]["TABLE_NAME"]);
                        if (table != String.Empty)
                        {
                            ret = (string[])ArrayUtils.Add(ret, table);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error GetTables: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public override string[] GetTables(string database, bool shouldOpenAndClose)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetTablesAsDataTable(database, shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string table = Convert.ToString(dataTable.Rows[i]["TABLE_NAME"]);
                        if (table != String.Empty)
                        {
                            ret = (string[])ArrayUtils.Add(ret, table);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error GetTables: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public override DataTable GetTablesAsDataTable()
        {
            return GetTablesAsDataTable(true);
        }

        public override DataTable GetTablesAsDataTable(string database)
        {
            return GetTablesAsDataTable(database, true);
        }

        public override DataTable GetTablesAsDataTable(bool shouldOpenAndClose)
        {
            DataTable ret = null;
            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                //TdDataAdapter da = new TdDataAdapter("SELECT * FROM Information_Schema.Tables where Table_Type = 'BASE TABLE' ORDER BY TABLE_NAME", (TdConnection)Connection);
                //ret = new DataTable();
                //da.Fill(ret);

                ret = ((TdConnection)Connection).GetSchema("Tables");
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error GetTablesAsDataTable: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

            }
            return ret;
        }


        public override DataTable GetTablesAsDataTable(string database, bool shouldOpenAndClose)
        {
            DataTable ret = null;
            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                //TdDataAdapter da = new TdDataAdapter("SELECT * FROM Information_Schema.Tables where Table_Type = 'BASE TABLE' ORDER BY TABLE_NAME", (TdConnection)Connection);
                //ret = new DataTable();
                //da.Fill(ret);

                string[] restrictions = new string[3];
                restrictions[0] = database;
                restrictions[1] = null;
                restrictions[2] = "T";

                //ret = ((TdConnection)Connection).GetSchema("Tables", restrictions);
                ret = ((TdConnection)Connection).GetSchema("Tables");
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error GetTablesAsDataTable: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

            }
            return ret;
        }

        public override string[] GetDatabases()
        {
            return GetDatabases(true, DatabaseNameListingOptions.OnlyAsDBOwner);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose)
        {
            return GetDatabases(shouldOpenAndClose, DatabaseNameListingOptions.OnlyAsDBOwner);
        }

        public override string[] GetDatabases(DatabaseNameListingOptions opt)
        {
            return GetDatabases(true, opt);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetDatabasesAsDataTable(shouldOpenAndClose, opt);
                if (dataTable != null)
                {
                    string tableString = DisplayDataTable(dataTable);

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        // string table = Convert.ToString(dataTable.Rows[i]["DATABASE_NAME"]);
                        string table = Convert.ToString(dataTable.Rows[i][1]);
                        if (!string.IsNullOrEmpty(table))
                        {
                            ret = (string[])ArrayUtils.Add(ret, table);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting databases: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        static string DisplayDataTable(DataTable t)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("\n Number of columns in Data Table: {0}", t.Columns.Count);
            sb.AppendFormat("\n Number of rows in Data Table: {0}", t.Rows.Count);
            sb.AppendFormat("\n ");
            foreach (DataColumn c in t.Columns)
            {
                sb.AppendFormat(c.ColumnName + " : ");
            }

            sb.AppendFormat("\n ===============================================================================");

            foreach (DataRow r in t.Rows)
            {
                sb.AppendFormat("\n");
                foreach (Object i in r.ItemArray)
                {
                    sb.AppendFormat(i + " : ");
                }
                sb.AppendFormat("\n");
            }

            return sb.ToString();
        }

        public override DataTable GetDatabasesAsDataTable()
        {
            return GetDatabasesAsDataTable(true, DatabaseNameListingOptions.OnlyAsDBOwner);
        }

        public override  DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose)
        {
            return GetDatabasesAsDataTable(shouldOpenAndClose, DatabaseNameListingOptions.OnlyAsDBOwner);
        }

        public override DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {
            DataTable ret = null;
            
            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                //if (opt == DatabaseNameListingOptions.OnlyAsDBOwner)
                //{
                //    query = "EXEC sp_databases";
                //}
                //else
                //{
                //    query = "CREATE TABLE #DBNames([DATABASE_NAME] varchar(255),[DATABASE_SIZE] INT,[REMARKS] INT)" + Environment.NewLine + "EXEC sp_msForEachDB 'INSERT INTO #DBNames VALUES (''?'', 0, 0)'" + Environment.NewLine + "select * from #DBNames" + Environment.NewLine + "drop table #DBNames";
                //}

                //da = new TdDataAdapter(query, (TdConnection)Connection);
                //ret = new DataTable();
                //da.Fill(ret);

                ret = ((TdConnection)Connection).GetSchema("Schemata");
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting databases: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

            }
            return ret;
        }

        public override string GetTableClusteredIndex(string tableName)
        {
            return GetTableClusteredIndex(tableName, true);
        }

        public override string GetTableClusteredIndex(string tableName, bool shouldOpenAndClose)
        {
            string ret = null;
            try
            {
                DataTable dataTable = GetTableIndexesAsDataTable(tableName, shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string index = Convert.ToString(dataTable.Rows[i]["name"]);
                        string description = Convert.ToString(dataTable.Rows[i]["type_desc"]);
                        if (index != String.Empty)
                        {
                            if (String.Compare(description, ("clustered"), true) == 0)
                            {
                                ret = index;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting GetTableIndexes: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public override string[] GetTableSpatialIndexes(string tableName)
        {
            return GetTableSpatialIndexes(tableName, true);
        }

        public override string[] GetTableSpatialIndexes(string tableName, bool shouldOpenAndClose)
        {
            List<string> ret = new List<string>();
            try
            {
                DataTable dataTable = GetTableIndexesAsDataTable(tableName, shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string index = Convert.ToString(dataTable.Rows[i]["name"]);
                        string description = Convert.ToString(dataTable.Rows[i]["type_desc"]);
                        if (index != String.Empty)
                        {
                            if (String.Compare(description, ("spatial"), true) == 0)
                            {
                                ret.Add(index);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting GetTableSpatialIndexes: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret.ToArray();
        }


        public override string[] GetTableIndexes(string tableName)
        {
            return GetTableIndexes(tableName, true);
        }

        public override string[] GetTableIndexes(string tableName, bool shouldOpenAndClose)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetTableIndexesAsDataTable(tableName, shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string table = Convert.ToString(dataTable.Rows[i]["name"]);
                        if (table != String.Empty)
                        {
                            ret = (string[])ArrayUtils.Add(ret, table);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting GetTableIndexes: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public DataTable GetTableIndexesAsDataTable(string tableName)
        {
            return GetTableIndexesAsDataTable(tableName, true);
        }

        public DataTable GetTableIndexesAsDataTable(string tableName, bool shouldOpenAndClose)
        {
            DataTable ret = null;
            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                string sql = "select * from sys.indexes where object_id = OBJECT_ID('" + tableName + "')";

                TdDataAdapter da = new TdDataAdapter(sql, (TdConnection)Connection);
                ret = new DataTable();
                da.Fill(ret);
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting GetTableIndexesAsDataTable: " + ex.Message;
                throw new Exception(msg, ex);

            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

            }
            return ret;
        }

        public override void AddGeogIndexToDatabase(string tableName)
        {
            throw new NotImplementedException();
        }

        public override void AddGeogIndexToDatabase(string tableName, bool shouldOpenCloseConnection)
        {
            throw new NotImplementedException();
        }

    }
}

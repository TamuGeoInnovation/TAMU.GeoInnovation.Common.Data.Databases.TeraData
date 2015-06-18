using System;
using System.Data;
using System.Data.SqlClient;
using USC.GISResearchLab.Common.Core.Databases.BulkCopys;
using USC.GISResearchLab.Common.Utils.Databases;

namespace USC.GISResearchLab.Common.Core.Databases.Teradata
{
    public class TeradataBulkCopy : AbstractBulkCopy
	{
		//public new event SqlRowsCopiedEventHandler SqlRowsCopied;

		#region Properties
		private SqlBulkCopy _SqlBulkCopy;
		public SqlBulkCopy SqlBulkCopy
		{
			get { return _SqlBulkCopy; }
			set { _SqlBulkCopy = value; }
		}
		#endregion

		public TeradataBulkCopy(DatabaseType databaseType, SqlConnection conn)
		{
			DatabaseType = databaseType;
			Connection = conn;
			SqlBulkCopy = new SqlBulkCopy(conn);
		}

		public override void Close()
		{
			SqlBulkCopy.Close();
		}

        public override void GenerateColumnMappings()
        {
            GenerateColumnMappings(null);
        }

		public override void GenerateColumnMappings(string [] excludeColumns)
		{
			try
			{
				Connection.Open();
				SqlCommand cmd1 = new SqlCommand("SELECT COLUMN_NAME," +
								 "COLUMNPROPERTY(OBJECT_ID('" +
								 DestinationTableName +
								 "'),COLUMN_NAME,'IsComputed')AS 'IsComputed' " +
								 "FROM INFORMATION_SCHEMA.COLUMNS " +
								 "WHERE TABLE_SCHEMA = '" +
								 "dbo" + "' AND TABLE_NAME = '" +
								 DestinationTableName + "'", (SqlConnection)Connection);

				SqlDataReader drcolumns = cmd1.ExecuteReader();
				while (drcolumns.Read())
				{
					if (drcolumns.GetInt32(1) != 1)
					{
                        if (excludeColumns != null)
                        {
                            string name = drcolumns.GetString(0);

                            bool shouldAdd = true;
                            for (int i = 0; i < excludeColumns.Length; i++)
                            {
                                if (String.Compare(excludeColumns[i], name, true) == 0)
                                {
                                    shouldAdd = false;
                                    break;
                                }
                            }

                            if (shouldAdd)
                            {
                                
                                SqlBulkCopy.ColumnMappings.Add(name, name);
                            }
                        }
                        else
                        {
                            string name = drcolumns.GetString(0);
                            SqlBulkCopy.ColumnMappings.Add(name, name);
                        }
					}
				}

				drcolumns.Close();
			}
			catch (Exception e)
			{
				Connection.Close();
				throw new Exception("Error occured GereratingColumnMappings: " + e.Message);
			}
			finally
			{
				Connection.Close();
			}
		}

		public override void WriteToServer(DataRow[] rows)
		{
			try
			{
				Connection.Open();
				InitSqlBulkCopy();
				SqlBulkCopy.WriteToServer(rows);
			}
			catch (Exception e)
			{
				Connection.Close();
				throw new Exception("Error occured WriteToServer: " + e.Message);
			}
			finally
			{
				Connection.Close();
			}
		}

		public override void WriteToServer(DataTable dataTable)
		{
			try
			{
				Connection.Open();
				InitSqlBulkCopy();
				SqlBulkCopy.WriteToServer(dataTable);
			}
			catch (Exception e)
			{
				Connection.Close();
				throw new Exception("Error occured WriteToServer: " + e.Message);
			}
			finally
			{
				Connection.Close();
			}
		}

		public override void WriteToServer(IDataReader dataReader)
		{
			try
			{
				Connection.Open();
				InitSqlBulkCopy();
				SqlBulkCopy.WriteToServer(dataReader);
			}
			catch (Exception e)
			{
				Connection.Close();
				throw new Exception("Error occured WriteToServer: " + e.Message);
			}
			finally
			{
				Connection.Close();
			}
		}

		public override void WriteToServer(DataTable dataTable, DataRowState dataRowState)
		{
			try
			{
				Connection.Open();
				InitSqlBulkCopy();
				SqlBulkCopy.WriteToServer(dataTable, dataRowState);
			}
			catch (Exception e)
			{
				Connection.Close();
				throw new Exception("Error occured WriteToServer: " + e.Message);
			}
			finally
			{
				Connection.Close();
			}
		}

		private void InitSqlBulkCopy()
		{
			SqlBulkCopy.BatchSize = BatchSize;
			SqlBulkCopy.BulkCopyTimeout = BulkCopyTimeout;
            SqlBulkCopy.DestinationTableName = "[dbo]." + DatabaseUtils.AsDbTableName(DestinationTableName, true, false);
			SqlBulkCopy.NotifyAfter = NotifyAfter;
			SqlBulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(sqlBulkCopy_SqlRowsCopied);
		}

		private void sqlBulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			OnSqlRowsCopied(sender, e);
		}

	}
}
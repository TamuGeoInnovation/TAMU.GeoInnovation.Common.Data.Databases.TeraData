using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;
using USC.GISResearchLab.Common.Databases.DataSources;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace USC.GISResearchLab.Common.Databases.Teradata
{
    public class TeradataDataSourceManager : AbstractDataSourceManager
    {
        public TeradataDataSourceManager()
        {
            ProviderType = DataProviderType.Teradata;
        }

        public TeradataDataSourceManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = DataProviderType.Teradata;
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override void CreateDatabase(DatabaseType databaseType, string databaseName)
        {
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(DatabaseType.Teradata, Location, "", UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.Teradata);
                QueryManager queryManager = new QueryManager(DataProviderType.Teradata, DatabaseType.Teradata, connectionString);

                string sql = "CREATE DATABASE " + databaseName + " ON PRIMARY"
                + "(Name=test_data, filename = 'C:\\mysql\\" + databaseName + "_data.mdf', size=3,"
                + "maxsize=5, filegrowth=10%)log on"
                + "(name=mydbb_log, filename='C:\\mysql\\" + databaseName + "_log.ldf',size=3,"
                + "maxsize=20,filegrowth=1)";

                queryManager.ExecuteNonQuery(System.Data.CommandType.Text, sql);
            }
            catch (Exception ex)
            {
                string msg = "Error creating database: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override bool Validate(DatabaseType databaseType, string databaseName)
        {
            bool ret = true;
            IQueryManager queryManager = null;
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(DatabaseType.Teradata, Location, databaseName, UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.Teradata);
                queryManager = new QueryManager(DataProviderType.Teradata, DatabaseType.Teradata, connectionString);
                queryManager.Open();
                queryManager.Close();
            }
            catch (Exception ex)
            {
                ret = false;
            }
            finally
            {
                queryManager.Dispose();
            }

            return ret;
        }
    }
}

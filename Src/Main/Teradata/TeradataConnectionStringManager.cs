using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;

namespace USC.GISResearchLab.Common.Databases.Teradata
{
    public class TeradataConnectionStringManager : AbstractConnectionStringManager
    {
        public TeradataConnectionStringManager()
        {
            DatabaseType = DatabaseType.Teradata;
        }

        public TeradataConnectionStringManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override string GetConnectionString(DataProviderType dataProviderType)
        {
            string ret = null;
            switch (dataProviderType)
            {
                case DataProviderType.Teradata:
                    ret = "Data Source=" + Location + ";User ID=" + UserName + ";Password=" + Password;
                    //ret = "Data Source=" + Location + "; Initial Catalog=" + DefaultDatabase + ";uid=" + UserName + ";pwd=" + Password;
                    break;
                case DataProviderType.Odbc:
                    ret = "Driver={SQL Server};Server=" + Location + ";UID=" + UserName + ";PWD=" + Password + ";Database=" + DefaultDatabase + ";";
                    break;
                case DataProviderType.OleDb:
                    ret = "";
                    break;
                default:
                    throw new Exception("Unexpected dataProviderType: " + dataProviderType);
            }
            return ret;
        }
    }
}

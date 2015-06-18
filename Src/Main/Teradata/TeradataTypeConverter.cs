using System;
using System.Data;
using Microsoft.SqlServer.Types;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.TypeConverters;

namespace USC.GISResearchLab.Common.Databases.Teradata
{
    public class TeradataTypeConverter : AbstractDatabaseDataProviderTypeConverterManager
    {

        #region TypeNames
        public static string TYPENAME_BigInt = "BigInt";
        public static string TYPENAME_Binary = "Binary ";
        public static string TYPENAME_Bit = "Bit";
        public static string TYPENAME_Char = "Char";
        public static string TYPENAME_Date = "Date";
        public static string TYPENAME_DateTime = "DateTime";
        public static string TYPENAME_DateTime2 = "DateTime2";
        public static string TYPENAME_DateTimeOffset = "DateTimeOffset";
        public static string TYPENAME_Decimal = "Decimal";
        public static string TYPENAME_Float = "Float";
        public static string TYPENAME_Geography = "Geography";
        public static string TYPENAME_Geometry = "Geometry";
        public static string TYPENAME_Image = "Image";
        public static string TYPENAME_Int = "Int";
        public static string TYPENAME_Money = "Money";
        public static string TYPENAME_NChar = "NChar";
        public static string TYPENAME_NText = "NText";
        public static string TYPENAME_Numeric = "Numeric";
        public static string TYPENAME_NVarChar = "NVarChar";
        public static string TYPENAME_Real = "Real";
        public static string TYPENAME_SmallDateTime = "SmallDateTime";
        public static string TYPENAME_SmallInt = "SmallInt";
        public static string TYPENAME_SmallMoney = "SmallMoney";
        public static string TYPENAME_Structured = "Structured";
        public static string TYPENAME_Text = "Text";
        public static string TYPENAME_Time = "Time";
        public static string TYPENAME_Timestamp = "Timestamp";
        public static string TYPENAME_TinyInt = "TinyInt";
        public static string TYPENAME_Udt = "Udt";
        public static string TYPENAME_UniqueIdentifier = "UniqueIdentifier";
        public static string TYPENAME_VarBinary = "VarBinary";
        public static string TYPENAME_VarChar = "VarChar";
        public static string TYPENAME_Variant = "Variant";
        public static string TYPENAME_Xml = "Xml";

        
        #endregion

        public TeradataTypeConverter()
        {
            DatabaseType = DatabaseType.Teradata;
            DataProviderType = DataProviderType.Teradata;
        }

        public override int GetDefaultLength(DatabaseSuperDataType type)
        {
            SqlDbType dbType = (SqlDbType)FromDatabaseSuperDataType(type);
            return GetDefaultLength(dbType);
        }

        public int GetDefaultLength(SqlDbType dbType)
        {
            int ret = -1;

            switch (dbType)
            {

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                    ret = 32;
                    break;
                case SqlDbType.VarChar:
                    ret = 255;
                    break;
                case SqlDbType.NText:
                    ret = Int32.MaxValue;
                    break;
                default:
                    break;
            }
            return ret;
        }

        public override int GetDefaultPrecision(DatabaseSuperDataType type)
        {
            SqlDbType dbType = (SqlDbType)FromDatabaseSuperDataType(type);
            return GetDefaultPrecision(dbType);
        }

        public int GetDefaultPrecision(SqlDbType dbType)
        {
            int ret = -1;
            SqlDbType type = (SqlDbType)dbType;

            switch (type)
            {
                case SqlDbType.Decimal:
                case SqlDbType.Money:
                    ret = 18;
                    break;
                default:
                    break;
            }
            return ret;
        }

        public override Type GetSystemType(object dbType)
        {
            Type ret = null;
            SqlDbType type = (SqlDbType)dbType;

            switch (type)
            {
                case SqlDbType.BigInt:
                    ret = typeof(Int64);
                    break;
                case SqlDbType.Binary:
                    ret = typeof(Byte[]);
                    break;
                case SqlDbType.Bit:
                    ret = typeof(Boolean);
                    break;
                case SqlDbType.Char:
                    ret = typeof(String);
                    break;
                case SqlDbType.Date:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.DateTime:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.DateTime2:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.DateTimeOffset:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.Decimal:
                    ret = typeof(Decimal);
                    break;
                case SqlDbType.Float:
                    ret = typeof(Double);
                    break;
                case SqlDbType.Image:
                    ret = typeof(Byte[]);
                    break;
                case SqlDbType.Int:
                    ret = typeof(Int32);
                    break;
                case SqlDbType.Money:
                    ret = typeof(Decimal);
                    break;
                case SqlDbType.NChar:
                    ret = typeof(String);
                    break;
                case SqlDbType.NVarChar:
                    ret = typeof(String);
                    break;
                case SqlDbType.Real:
                    ret = typeof(Double);
                    break;
                case SqlDbType.SmallDateTime:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.SmallInt:
                    ret = typeof(Int16);
                    break;
                case SqlDbType.SmallMoney:
                    ret = typeof(Decimal);
                    break;
                case SqlDbType.Structured:
                    ret = typeof(Object);
                    break;
                case SqlDbType.Text:
                    ret = typeof(String);
                    break;
                case SqlDbType.Time:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.Timestamp:
                    ret = typeof(DateTime);
                    break;
                case SqlDbType.TinyInt:
                    ret = typeof(Byte);
                    break;
                case SqlDbType.Udt:
                    ret = typeof(Object);
                    break;
                case SqlDbType.UniqueIdentifier:
                    ret = typeof(Guid);
                    break;
                case SqlDbType.VarBinary:
                    ret = typeof(Byte[]);
                    break;
                case SqlDbType.VarChar:
                    ret = typeof(String);
                    break;
                case SqlDbType.Variant:
                    ret = typeof(Object);
                    break;
                case SqlDbType.Xml:
                    ret = typeof(String);
                    break;
                default:
                    throw new Exception("Unexpected type: " + type);
            }
            return ret;
        }

        public override DatabaseSuperDataType ToSuperType(object dbType)
        {
            DatabaseSuperDataType ret;

         

            SqlDbType type = (SqlDbType)dbType;

            switch (type)
            {
                case SqlDbType.BigInt:
                    ret = DatabaseSuperDataType.BigInt;
                    break;
                case SqlDbType.Binary:
                    ret = DatabaseSuperDataType.Binary;
                    break;
                case SqlDbType.Bit:
                    ret = DatabaseSuperDataType.Bit;
                    break;
                case SqlDbType.Char:
                    ret = DatabaseSuperDataType.Char;
                    break;
                case SqlDbType.Date:
                    ret = DatabaseSuperDataType.Date;
                    break;
                case SqlDbType.DateTime:
                    ret = DatabaseSuperDataType.DateTime;
                    break;
                case SqlDbType.DateTime2:
                    ret = DatabaseSuperDataType.DateTime2;
                    break;
                case SqlDbType.DateTimeOffset:
                    ret = DatabaseSuperDataType.DateTimeOffset;
                    break;
                case SqlDbType.Decimal:
                    ret = DatabaseSuperDataType.Decimal;
                    break;
                case SqlDbType.Float:
                    ret = DatabaseSuperDataType.Float;
                    break;
                case SqlDbType.Image:
                    ret = DatabaseSuperDataType.Image;
                    break;
                case SqlDbType.Int:
                    ret = DatabaseSuperDataType.Int32;
                    break;
                case SqlDbType.Money:
                    ret = DatabaseSuperDataType.Decimal;
                    break;
                case SqlDbType.NChar:
                    ret = DatabaseSuperDataType.NChar;
                    break;
                case SqlDbType.NVarChar:
                    ret = DatabaseSuperDataType.NVarChar;
                    break;
                case SqlDbType.Real:
                    ret = DatabaseSuperDataType.Real;
                    break;
                case SqlDbType.SmallDateTime:
                    ret = DatabaseSuperDataType.SmallDateTime;
                    break;
                case SqlDbType.SmallInt:
                    ret = DatabaseSuperDataType.SmallInt;
                    break;
                case SqlDbType.SmallMoney:
                    ret = DatabaseSuperDataType.SmallMoney;
                    break;
                case SqlDbType.Structured:
                    ret = DatabaseSuperDataType.Structured;
                    break;
                case SqlDbType.Text:
                    ret = DatabaseSuperDataType.Text;
                    break;
                case SqlDbType.Time:
                    ret = DatabaseSuperDataType.Time;
                    break;
                case SqlDbType.Timestamp:
                    ret = DatabaseSuperDataType.Timestamp;
                    break;
                case SqlDbType.TinyInt:
                    ret = DatabaseSuperDataType.TinyInt;
                    break;
                case SqlDbType.Udt:
                    ret = DatabaseSuperDataType.Udt;
                    break;
                case SqlDbType.UniqueIdentifier:
                    ret = DatabaseSuperDataType.UniqueIdentifier;
                    break;
                case SqlDbType.VarBinary:
                    ret = DatabaseSuperDataType.VarBinary;
                    break;
                case SqlDbType.VarChar:
                    ret = DatabaseSuperDataType.VarChar;
                    break;
                case SqlDbType.Variant:
                    ret = DatabaseSuperDataType.Variant;
                    break;
                case SqlDbType.Xml:
                    ret = DatabaseSuperDataType.Xml;
                    break;
                default:
                    throw new Exception("Unexpected type: " + type);
            }
            return ret;
        }

        public override object FromDatabaseSuperDataType(DatabaseSuperDataType type)
        {
            SqlDbType ret = SqlDbType.Text;

            switch (type)
            {
                case DatabaseSuperDataType.BigInt:
                    ret = SqlDbType.BigInt;
                    break;
                case DatabaseSuperDataType.Binary:
                    ret = SqlDbType.Binary;
                    break;
                case DatabaseSuperDataType.Bit:
                    ret = SqlDbType.Bit;
                    break;
                case DatabaseSuperDataType.Blob:
                    ret = SqlDbType.VarBinary;
                    break;
                case DatabaseSuperDataType.Boolean:
                    ret = SqlDbType.Bit;
                    break;
                case DatabaseSuperDataType.BSTR:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Char:
                    ret = SqlDbType.Char;
                    break;
                case DatabaseSuperDataType.Counter:
                    ret = SqlDbType.Int;
                    break;
                case DatabaseSuperDataType.Currency:
                    ret = SqlDbType.Money;
                    break;
                case DatabaseSuperDataType.Date:
                    ret = SqlDbType.Date;
                    break;
                case DatabaseSuperDataType.DateTime:
                    ret = SqlDbType.DateTime;
                    break;
                case DatabaseSuperDataType.DateTime2:
                    ret = SqlDbType.DateTime2;
                    break;
                case DatabaseSuperDataType.DateTimeOffset:
                    ret = SqlDbType.DateTimeOffset;
                    break;
                case DatabaseSuperDataType.DBDate:
                    ret = SqlDbType.Date;
                    break;
                case DatabaseSuperDataType.DBTime:
                    ret = SqlDbType.Time;
                    break;
                case DatabaseSuperDataType.DBTimeStamp:
                    ret = SqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.Decimal:
                    ret = SqlDbType.Decimal;
                    break;
                case DatabaseSuperDataType.Double:
                    ret = SqlDbType.Float;
                    break;
                case DatabaseSuperDataType.Empty:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Error:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Filetime:
                    ret = SqlDbType.DateTime;
                    break;
                case DatabaseSuperDataType.Float:
                    ret = SqlDbType.Float;
                    break;
                case DatabaseSuperDataType.Geography:
                    ret = SqlDbType.Udt;
                    break;
                case DatabaseSuperDataType.Geometry:
                    ret = SqlDbType.Udt;
                    break;
                case DatabaseSuperDataType.Guid:
                    ret = SqlDbType.UniqueIdentifier;
                    break;
                case DatabaseSuperDataType.IDispatch:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Image:
                    ret = SqlDbType.Image;
                    break;
                case DatabaseSuperDataType.Int16:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.Int24:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.Int32:
                    ret = SqlDbType.Int;
                    break;
                case DatabaseSuperDataType.Int64:
                    ret = SqlDbType.BigInt;
                    break;
                case DatabaseSuperDataType.IUnknown:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Long:
                    ret = SqlDbType.Int;
                    break;
                case DatabaseSuperDataType.LongBinary:
                    ret = SqlDbType.VarBinary;
                    break;
                case DatabaseSuperDataType.LongText:
                    ret = SqlDbType.NText;
                    break;
                case DatabaseSuperDataType.LongVarBinary:
                    ret = SqlDbType.VarBinary;
                    break;
                case DatabaseSuperDataType.LongVarChar:
                    ret = SqlDbType.NVarChar;
                    break;
                case DatabaseSuperDataType.LongVarWChar:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.MediumBlob:
                    ret = SqlDbType.VarBinary;
                    break;
                case DatabaseSuperDataType.Money:
                    ret = SqlDbType.Money;
                    break;
                case DatabaseSuperDataType.NChar:
                    ret = SqlDbType.NChar;
                    break;
                case DatabaseSuperDataType.Newdate:
                    ret = SqlDbType.Date;
                    break;
                case DatabaseSuperDataType.NewDecimal:
                    ret = SqlDbType.Decimal;
                    break;
                case DatabaseSuperDataType.NText:
                    ret = SqlDbType.NText;
                    break;
                case DatabaseSuperDataType.Numeric:
                    ret = SqlDbType.Decimal;
                    break;
                case DatabaseSuperDataType.NVarChar:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.PropVariant:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Real:
                    ret = SqlDbType.Real;
                    break;
                case DatabaseSuperDataType.Set:
                    ret = SqlDbType.Binary;
                    break;
                case DatabaseSuperDataType.Short:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.Single:
                    ret = SqlDbType.Decimal;
                    break;
                case DatabaseSuperDataType.SmallDateTime:
                    ret = SqlDbType.SmallDateTime;
                    break;
                case DatabaseSuperDataType.SmallInt:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.SmallMoney:
                    ret = SqlDbType.SmallMoney;
                    break;
                case DatabaseSuperDataType.String:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.Structured:
                    ret = SqlDbType.Structured;
                    break;
                case DatabaseSuperDataType.Text:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.Time:
                    ret = SqlDbType.Time;
                    break;
                case DatabaseSuperDataType.Timestamp:
                    ret = SqlDbType.Timestamp;
                    break;
                case DatabaseSuperDataType.TinyBlob:
                    ret = SqlDbType.Binary;
                    break;
                case DatabaseSuperDataType.TinyInt:
                    ret = SqlDbType.TinyInt;
                    break;
                case DatabaseSuperDataType.TinyText:
                    ret = SqlDbType.Text;
                    break;
                case DatabaseSuperDataType.UByte:
                    ret = SqlDbType.Binary;
                    break;
                case DatabaseSuperDataType.Udt:
                    ret = SqlDbType.Udt;
                    break;
                case DatabaseSuperDataType.UInt16:
                    ret = SqlDbType.TinyInt;
                    break;
                case DatabaseSuperDataType.UInt24:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.UInt32:
                    ret = SqlDbType.Int;
                    break;
                case DatabaseSuperDataType.UInt64:
                    ret = SqlDbType.BigInt;
                    break;
                case DatabaseSuperDataType.UniqueIdentifier:
                    ret = SqlDbType.UniqueIdentifier;
                    break;
                case DatabaseSuperDataType.UnsignedBigInt:
                    ret = SqlDbType.BigInt;
                    break;
                case DatabaseSuperDataType.UnsignedInt:
                    ret = SqlDbType.Int;
                    break;
                case DatabaseSuperDataType.UnsignedSmallInt:
                    ret = SqlDbType.SmallInt;
                    break;
                case DatabaseSuperDataType.UnsignedTinyInt:
                    ret = SqlDbType.TinyInt;
                    break;
                case DatabaseSuperDataType.VarBinary:
                    ret = SqlDbType.VarBinary;
                    break;
                case DatabaseSuperDataType.VarChar:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.Variant:
                    ret = SqlDbType.Variant;
                    break;
                case DatabaseSuperDataType.VarNumeric:
                    ret = SqlDbType.Decimal;
                    break;
                case DatabaseSuperDataType.VarString:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.VarWChar:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.WChar:
                    ret = SqlDbType.VarChar;
                    break;
                case DatabaseSuperDataType.Xml:
                    ret = SqlDbType.Xml;
                    break;
                case DatabaseSuperDataType.Year:
                    ret = SqlDbType.Int;
                    break;
                default:
                    throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            }

            return ret;
        }

        public override object ConvertType(object dbType, DatabaseType databaseType)
        {
            throw new NotImplementedException();
        }

        public override string GetTypeAsString(DatabaseSuperDataType type)
        {
            string ret = null;

            switch (type)
            {
                case DatabaseSuperDataType.BigInt:
                    ret = TYPENAME_BigInt;
                    break;
                case DatabaseSuperDataType.Binary:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.Bit:
                    ret = TYPENAME_Bit;
                    break;
                case DatabaseSuperDataType.Blob:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.Boolean:
                    ret = TYPENAME_Bit;
                    break;
                case DatabaseSuperDataType.BSTR:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Char:
                    ret = TYPENAME_Char;
                    break;
                case DatabaseSuperDataType.Counter:
                    ret = TYPENAME_Int;
                    break;
                case DatabaseSuperDataType.Currency:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.Date:
                    ret = TYPENAME_Date;
                    break;
                case DatabaseSuperDataType.DateTime:
                    ret = TYPENAME_DateTime;
                    break;
                case DatabaseSuperDataType.DateTime2:
                    ret = TYPENAME_DateTime2;
                    break;
                case DatabaseSuperDataType.DateTimeOffset:
                    ret = TYPENAME_DateTimeOffset;
                    break;
                case DatabaseSuperDataType.DBDate:
                    ret = TYPENAME_Date;
                    break;
                case DatabaseSuperDataType.DBTime:
                    ret = TYPENAME_Time;
                    break;
                case DatabaseSuperDataType.DBTimeStamp:
                    ret = TYPENAME_Timestamp;
                    break;
                case DatabaseSuperDataType.Decimal:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.Double:
                    ret = TYPENAME_Float;
                    break;
                case DatabaseSuperDataType.Empty:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Error:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Filetime:
                    ret = TYPENAME_DateTime;
                    break;
                case DatabaseSuperDataType.Float:
                    ret = TYPENAME_Float;
                    break;
                case DatabaseSuperDataType.Geography:
                    ret = TYPENAME_Geography;
                    break;
                case DatabaseSuperDataType.Geometry:
                    ret = TYPENAME_Geometry;
                    break;
                case DatabaseSuperDataType.Guid:
                    ret = TYPENAME_UniqueIdentifier;
                    break;
                case DatabaseSuperDataType.IDispatch:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Image:
                    ret = TYPENAME_Image;
                    break;
                case DatabaseSuperDataType.Int16:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.Int24:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.Int32:
                    ret = TYPENAME_Int;
                    break;
                case DatabaseSuperDataType.Int64:
                    ret = TYPENAME_BigInt;
                    break;
                case DatabaseSuperDataType.IUnknown:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Long:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.LongBinary:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.LongText:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.LongVarBinary:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.LongVarChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.LongVarWChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.MediumBlob:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.Money:
                    ret = TYPENAME_Money;
                    break;
                case DatabaseSuperDataType.NChar:
                    ret = TYPENAME_NChar;
                    break;
                case DatabaseSuperDataType.Newdate:
                    ret = TYPENAME_Date;
                    break;
                case DatabaseSuperDataType.NewDecimal:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.NText:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Numeric:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.NVarChar:
                    ret = TYPENAME_NVarChar;
                    break;
                case DatabaseSuperDataType.PropVariant:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Real:
                    ret = TYPENAME_Real;
                    break;
                case DatabaseSuperDataType.Set:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.Short:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.Single:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.SmallDateTime:
                    ret = TYPENAME_DateTime;
                    break;
                case DatabaseSuperDataType.SmallInt:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.SmallMoney:
                    ret = TYPENAME_SmallMoney;
                    break;
                case DatabaseSuperDataType.String:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Structured:
                    ret = TYPENAME_Structured;
                    break;
                case DatabaseSuperDataType.Text:
                    ret = TYPENAME_Text;
                    break;
                case DatabaseSuperDataType.Time:
                    ret = TYPENAME_Time;
                    break;
                case DatabaseSuperDataType.Timestamp:
                    ret = TYPENAME_Timestamp;
                    break;
                case DatabaseSuperDataType.TinyBlob:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.TinyInt:
                    ret = TYPENAME_TinyInt;
                    break;
                case DatabaseSuperDataType.TinyText:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.UByte:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.Udt:
                    ret = TYPENAME_Udt;
                    break;
                case DatabaseSuperDataType.UInt16:
                    ret = TYPENAME_TinyInt;
                    break;
                case DatabaseSuperDataType.UInt24:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.UInt32:
                    ret = TYPENAME_Int;
                    break;
                case DatabaseSuperDataType.UInt64:
                    ret = TYPENAME_BigInt;
                    break;
                case DatabaseSuperDataType.UniqueIdentifier:
                    ret = TYPENAME_UniqueIdentifier;
                    break;
                case DatabaseSuperDataType.UnsignedBigInt:
                    ret = TYPENAME_BigInt;
                    break;
                case DatabaseSuperDataType.UnsignedInt:
                    ret = TYPENAME_Int;
                    break;
                case DatabaseSuperDataType.UnsignedSmallInt:
                    ret = TYPENAME_SmallInt;
                    break;
                case DatabaseSuperDataType.UnsignedTinyInt:
                    ret = TYPENAME_TinyInt;
                    break;
                case DatabaseSuperDataType.VarBinary:
                    ret = TYPENAME_VarBinary;
                    break;
                case DatabaseSuperDataType.VarChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Variant:
                    ret = TYPENAME_Binary;
                    break;
                case DatabaseSuperDataType.VarNumeric:
                    ret = TYPENAME_Decimal;
                    break;
                case DatabaseSuperDataType.VarString:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.VarWChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.WChar:
                    ret = TYPENAME_VarChar;
                    break;
                case DatabaseSuperDataType.Xml:
                    ret = TYPENAME_Xml;
                    break;
                case DatabaseSuperDataType.Year:
                    ret = TYPENAME_Int;
                    break;
                default:
                    throw new Exception("Unexpected or unimplemented DatabaseSuperDataType: " + type);
            }

            return ret;
        }

        public override Type ToSystemTypeFromDbTypeString(string dbTypeString)
        {
            Type ret = null;

            if (String.Compare(dbTypeString, TYPENAME_BigInt, true) == 0)
            {
                ret = typeof(Int64);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Binary, true) == 0)
            {
                ret = typeof(Byte[]);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Bit, true) == 0)
            {
                ret = typeof(Boolean);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Char, true) == 0)
            {
                ret = typeof(String);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Date, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTime, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTime2, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTimeOffset, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Decimal, true) == 0)
            {
                ret = typeof(Decimal);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Float, true) == 0)
            {
                ret = typeof(Double);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Geography, true) == 0)
            {
                ret = typeof(SqlGeography);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Geometry, true) == 0)
            {
                ret = typeof(SqlGeometry);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Image, true) == 0)
            {
                ret = typeof(Byte[]);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Int, true) == 0)
            {
                ret = typeof(Int32);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Money, true) == 0)
            {
                ret = typeof(Decimal);
            }
            else if (String.Compare(dbTypeString, TYPENAME_NChar, true) == 0)
            {
                ret = typeof(String);
            }
            else if (String.Compare(dbTypeString, TYPENAME_NVarChar, true) == 0)
            {
                ret = typeof(String);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Real, true) == 0)
            {
                ret = typeof(Double);
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallDateTime, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallInt, true) == 0)
            {
                ret = typeof(Int16);
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallMoney, true) == 0)
            {
                ret = typeof(Decimal);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Structured, true) == 0)
            {
                ret = typeof(Object);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Text, true) == 0)
            {
                ret = typeof(String);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Time, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Timestamp, true) == 0)
            {
                ret = typeof(DateTime);
            }
            else if (String.Compare(dbTypeString, TYPENAME_TinyInt, true) == 0)
            {
                ret = typeof(Byte);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Udt, true) == 0)
            {
                ret = typeof(Object);
            }
            else if (String.Compare(dbTypeString, TYPENAME_UniqueIdentifier, true) == 0)
            {
                ret = typeof(Guid);
            }
            else if (String.Compare(dbTypeString, TYPENAME_VarBinary, true) == 0)
            {
                ret = typeof(Byte[]);
            }
            else if (String.Compare(dbTypeString, TYPENAME_VarChar, true) == 0)
            {
                ret = typeof(String);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Variant, true) == 0)
            {
                ret = typeof(Object);
            }
            else if (String.Compare(dbTypeString, TYPENAME_Xml, true) == 0)
            {
                ret = typeof(String);
            }
            else
            {
                throw new Exception("Unexpected or unimplement dbTypeString: " + dbTypeString);
            }

            return ret;
        }


        public override DatabaseSuperDataType ToSuperTypeFromdbTypeString(string dbTypeString)
        {
            DatabaseSuperDataType ret;

            if (String.Compare(dbTypeString, TYPENAME_BigInt, true) == 0)
            {
                ret = DatabaseSuperDataType.BigInt;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Binary, true) == 0)
            {
                ret = DatabaseSuperDataType.Binary;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Bit, true) == 0)
            {

                ret = DatabaseSuperDataType.Bit;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Char, true) == 0)
            {

                ret = DatabaseSuperDataType.Char;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Date, true) == 0)
            {
                ret = DatabaseSuperDataType.Date;
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTime, true) == 0)
            {
                ret = DatabaseSuperDataType.DateTime;
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTime2, true) == 0)
            {
                ret = DatabaseSuperDataType.DateTime2;
            }
            else if (String.Compare(dbTypeString, TYPENAME_DateTimeOffset, true) == 0)
            {
                ret = DatabaseSuperDataType.DateTimeOffset;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Decimal, true) == 0)
            {
                ret = DatabaseSuperDataType.Decimal;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Float, true) == 0)
            {
                ret = DatabaseSuperDataType.Float;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Geography, true) == 0)
            {
                ret = DatabaseSuperDataType.Geography;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Geometry, true) == 0)
            {
                ret = DatabaseSuperDataType.Geometry;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Image, true) == 0)
            {
                ret = DatabaseSuperDataType.Image;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Int, true) == 0)
            {

                ret = DatabaseSuperDataType.Int32;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Money, true) == 0)
            {

                ret = DatabaseSuperDataType.Decimal;
            }
            else if (String.Compare(dbTypeString, TYPENAME_NChar, true) == 0)
            {

                ret = DatabaseSuperDataType.NChar;
            }
            else if (String.Compare(dbTypeString, TYPENAME_NText, true) == 0)
            {

                ret = DatabaseSuperDataType.NText;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Numeric, true) == 0)
            {
                ret = DatabaseSuperDataType.Decimal;
            }
            else if (String.Compare(dbTypeString, TYPENAME_NVarChar, true) == 0)
            {

                ret = DatabaseSuperDataType.NVarChar;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Real, true) == 0)
            {

                ret = DatabaseSuperDataType.Real;
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallDateTime, true) == 0)
            {

                ret = DatabaseSuperDataType.SmallDateTime;
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallInt, true) == 0)
            {

                ret = DatabaseSuperDataType.SmallInt;
            }
            else if (String.Compare(dbTypeString, TYPENAME_SmallMoney, true) == 0)
            {

                ret = DatabaseSuperDataType.SmallMoney;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Structured, true) == 0)
            {

                ret = DatabaseSuperDataType.Structured;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Text, true) == 0)
            {

                ret = DatabaseSuperDataType.Text;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Time, true) == 0)
            {

                ret = DatabaseSuperDataType.Time;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Timestamp, true) == 0)
            {

                ret = DatabaseSuperDataType.Timestamp;
            }
            else if (String.Compare(dbTypeString, TYPENAME_TinyInt, true) == 0)
            {

                ret = DatabaseSuperDataType.TinyInt;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Udt, true) == 0)
            {

                ret = DatabaseSuperDataType.Udt;
            }
            else if (String.Compare(dbTypeString, TYPENAME_UniqueIdentifier, true) == 0)
            {

                ret = DatabaseSuperDataType.UniqueIdentifier;
            }
            else if (String.Compare(dbTypeString, TYPENAME_VarBinary, true) == 0)
            {

                ret = DatabaseSuperDataType.VarBinary;
            }
            else if (String.Compare(dbTypeString, TYPENAME_VarChar, true) == 0)
            {

                ret = DatabaseSuperDataType.VarChar;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Variant, true) == 0)
            {

                ret = DatabaseSuperDataType.Variant;
            }
            else if (String.Compare(dbTypeString, TYPENAME_Xml, true) == 0)
            {
                ret = DatabaseSuperDataType.Xml;
            }
            else
            {
                throw new Exception("Unexpected dbTypeString: " + dbTypeString);
            }
            return ret;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using System.Dynamic;

using Microsoft.SqlServer.Management.Smo;
using System.Data.SqlClient;
using System.Configuration;

namespace CsvToSqlUtility
{
    class Program
    {
        static void Main( string[] args )
        {
            // Get Source Folder
            string sourceFolder = ConfigurationManager.AppSettings["SourceFolder"];
            if ( string.IsNullOrWhiteSpace(sourceFolder)) throw new Exception( "Missing Source Folder Application Setting" );

            var dir = new DirectoryInfo( sourceFolder );
            if ( dir == null ) throw new Exception( "Invalid Source Folder Directory" );

            var files = dir.GetFiles( "*.csv" );
            if ( !files.Any() ) throw new Exception( "There are not any *.csv files in the source folder" );

            // Get Database
            var configConnectionString = ConfigurationManager.ConnectionStrings["DbContext"];
            if ( configConnectionString == null ) throw new Exception( "Missing Connection String" );

            string connectionString = configConnectionString.ConnectionString;
            if ( string.IsNullOrWhiteSpace( connectionString ) ) throw new Exception( "Invalid Connection String" );

            var sqlConn = new SqlConnection( connectionString );
            if ( sqlConn == null ) throw new Exception( "Invalid Connection String" );

            var serverCon = new Microsoft.SqlServer.Management.Common.ServerConnection( sqlConn );
            Server server = new Server( serverCon );

            var db = server.Databases[sqlConn.Database];
            if ( db == null ) throw new Exception( "Invalid Database Name in Connection String" );

            string dbTablePrefix = ConfigurationManager.AppSettings["DbTablePrefix"];
            if ( string.IsNullOrWhiteSpace( dbTablePrefix ) ) dbTablePrefix = "_csv_";

            // Process Files
            foreach ( var file in files )
            {
                Console.WriteLine( string.Format( "Processing {0}...", file.Name ) );

                // Read all the records
                var records = GetRecords( file.FullName );

                // Determine the best field types
                var fields = GetFields( records );

                // Create the table
                string tableName = dbTablePrefix + Path.GetFileNameWithoutExtension( file.FullName );
                CreateTable( db, tableName, fields );

                // Import records
                InsertRecords( connectionString, tableName, fields, records );
            }
        }

        private static List<dynamic> GetRecords( string filePath )
        {
            if ( File.Exists( filePath ) )
            {
                using ( var s = File.OpenText( filePath ) )
                {
                    var reader = new CsvReader( s );
                    reader.Configuration.HasHeaderRecord = true;
                    var records = reader.GetRecords<dynamic>().ToList();
                    return records;
                }
            }

            return null;
        }

        private static List<CsvFieldInfo> GetFields( List<dynamic> records )
        {
            var fields = new List<CsvFieldInfo>();

            if ( records != null && records.Any() )
            {

                ExpandoObject obj = records[0];
                foreach ( var p in obj )
                {
                    fields.Add( new CsvFieldInfo { Name = p.Key } );
                }

                foreach ( dynamic record in records )
                {
                    foreach ( var p in record )
                    {
                        var fieldInfo = fields.FirstOrDefault( f => f.Name == p.Key );
                        if ( fieldInfo != null )
                        {
                            string value = p.Value.ToString().Trim();
                            if ( value != null && value != string.Empty )
                            {
                                int len = value.Length;
                                fieldInfo.MaxLength = len > fieldInfo.MaxLength ? len : fieldInfo.MaxLength;

                                // Check to see if data can be parsed
                                if ( fieldInfo.IsDateTime )
                                {
                                    DateTime datetime;
                                    if ( !DateTime.TryParse( value, out datetime ) )
                                    {
                                        fieldInfo.IsDateTime = false;
                                    }
                                }

                                if ( fieldInfo.IsBool )
                                {
                                    bool boolValue;
                                    if ( !bool.TryParse( value, out boolValue ) )
                                    {
                                        fieldInfo.IsBool = false;
                                    }
                                }

                                if ( fieldInfo.IsInteger )
                                {
                                    int number;
                                    if ( !int.TryParse( value, out number ) )
                                    {
                                        fieldInfo.IsInteger = false;
                                    }
                                }

                                if ( !fieldInfo.IsInteger && fieldInfo.IsDecimal )
                                {
                                    decimal number;
                                    if ( !decimal.TryParse( value, out number ) )
                                    {
                                        fieldInfo.IsDecimal = false;
                                    }
                                }
                            }
                        }
                    }
                }

                foreach ( var field in fields )
                {
                    var l = field.MaxLength;
                    field.MaxLength = 50 - ( l % 50 ) + l;
                }
            }

            return fields;
        }

        private static void CreateTable( Database database, string tableName, List<CsvFieldInfo> fields )
        {
            var tbl = new Table( database, tableName );
            foreach ( var fieldInfo in fields )
            {
                var dataType = fieldInfo.MaxLength >= 2000 ? DataType.VarCharMax : DataType.VarChar( fieldInfo.MaxLength );
                if ( fieldInfo.IsDecimal )
                {
                    dataType = DataType.Decimal( 2, 18 );
                }
                if ( fieldInfo.IsInteger )
                {
                    dataType = DataType.Int;
                }
                if ( fieldInfo.IsDateTime )
                {
                    dataType = DataType.DateTime;
                }
                if ( fieldInfo.IsBool )
                {
                    dataType = DataType.Bit;
                }

                var col = new Column( tbl, fieldInfo.Name, dataType );
                tbl.Columns.Add( col );
            }

            tbl.Create();
        }

        private static void InsertRecords( string connectionString, string tableName, List<CsvFieldInfo> fields, List<dynamic> records )
        {
            var insertPrefix = new StringBuilder();
            foreach ( var field in fields )
            {
                if ( insertPrefix.Length == 0 )
                {
                    insertPrefix.AppendFormat( "INSERT INTO [{0}] ( [{1}]", tableName, field.Name );
                }
                else
                {
                    insertPrefix.AppendFormat( ", [{0}]", field.Name );
                }
            }

            insertPrefix.AppendFormat( " ){0}    VALUES ", Environment.NewLine );

            using ( SqlConnection connection = new SqlConnection( connectionString ) )
            {
                connection.Open();

                foreach ( var record in records )
                {
                    var insertValues = new StringBuilder();

                    foreach ( var p in record )
                    {
                        var fieldInfo = fields.FirstOrDefault( f => f.Name == p.Key );
                        if ( fieldInfo != null )
                        {
                            string value = p.Value.ToString().Trim();

                            insertValues.Append( insertValues.Length == 0 ? "( " : ", " );

                            if ( fieldInfo.IsBool )
                            {
                                if ( value == null || value == "" )
                                {
                                    value = "NULL";
                                }
                                else
                                {
                                    if ( bool.TryParse( value, out bool selected ) )
                                    {
                                        value = selected ? "1" : "0";
                                    }
                                    else
                                    {
                                        value = "NULL";
                                    }
                                }
                            }
                            else if ( fieldInfo.IsDateTime )
                            {
                                if ( value == null || value == "" )
                                {
                                    value = "NULL";
                                }
                                else
                                {
                                    if ( DateTime.TryParse( value, out DateTime selected ) )
                                    {
                                        value = "'" + selected.ToString() + "'";
                                    }
                                    else
                                    {
                                        value = "NULL";
                                    }
                                }
                            }
                            else if ( fieldInfo.IsInteger )
                            {
                                if ( value == null || value == "" )
                                {
                                    value = "NULL";
                                }
                                else
                                {
                                    if ( !int.TryParse( value, out int selected ) )
                                    {
                                        value = "NULL";
                                    }
                                }
                            }
                            else if ( fieldInfo.IsDecimal )
                            {
                                if ( value == null || value == "" )
                                {
                                    value = "NULL";
                                }
                                else
                                {
                                    if ( !decimal.TryParse( value, out decimal selected ) )
                                    {
                                        value = "NULL";
                                    }
                                }
                            }
                            else
                            {
                                if ( value == null )
                                {
                                    value = "";
                                }
                                value = "'" + value.Replace( "'", "''" ) + "'";
                            }

                            insertValues.Append( value );
                        }
                    }

                    string insertStatement = insertPrefix.ToString() + insertValues.ToString() + " )" + Environment.NewLine;

                    using ( SqlCommand querySaveStaff = new SqlCommand( insertStatement, connection ) )
                    {
                        querySaveStaff.ExecuteNonQuery();
                    }
                }

                connection.Close();
            }
        }

        private class CsvFieldInfo
        {
            public string Name { get; set; }
            public int MaxLength { get; set; } = 0;
            public bool IsDateTime { get; set; } = true;
            public bool IsInteger { get; set; } = true;
            public bool IsDecimal { get; set; } = true;
            public bool IsBool { get; set; } = true;
        }
    }
}

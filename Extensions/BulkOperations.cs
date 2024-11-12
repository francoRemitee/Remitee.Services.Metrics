using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace Remitee.Services.Metrics.Extensions
{
    public static class BulkOperations
    {
        public static void UpdateData<T>(List<T> list, string connstring, string tableSchema, string tableName, string idFieldName)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        foreach (var prop in props)
                        {
                            var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                            if(propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                            tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                            if(prop.Name.ToLower() != idFieldName.ToLower())
                            {
                                setStatement = setStatement + "T." + prop.Name + "= temp." + prop.Name + ",";
                            }
                            
                        }

                        //Creating temp table on database
                        var createTableCommand = "CREATE TABLE #TmpTable(" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                        command.CommandText = createTableCommand;
                        command.ExecuteNonQuery();

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            bulkcopy.BulkCopyTimeout = 660;
                            bulkcopy.DestinationTableName = "#TmpTable";
                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = 600;
                        var updateTableCommand = "UPDATE T SET " + setStatement.Remove(setStatement.Length - 1) + " FROM " + tableName + " T INNER JOIN #TmpTable Temp ON T." + idFieldName + " = temp." + idFieldName + "; DROP TABLE #TmpTable;";
                        command.CommandText = updateTableCommand;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        public static void UpsertDataByBatch<T>(List<T> list, string connstring, string tableSchema, string tableName, string idFieldName, int batchSize)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();
            var tables = dt.AsEnumerable().ToChunks(batchSize)
                          .Select(rows => rows.CopyToDataTable());

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        string insertColumnValues = "";
                        string insertColumnNames = "";
                        foreach (var prop in props)
                        {
                            if (prop.CanWrite && !prop.GetMethod.IsVirtual)
                            {
                                var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                                if (propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                                tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                                if (prop.Name.ToLower() != idFieldName.ToLower())
                                {
                                    setStatement = setStatement + "Target." + prop.Name + "= Source." + prop.Name + ",";

                                }
                                insertColumnValues = insertColumnValues + "Source." + prop.Name + ",";
                                insertColumnNames = insertColumnNames + prop.Name + ",";
                            }
                        }

                        foreach(var table in tables)
                        {
                            //Creating temp table on database
                            var createTableCommand = "CREATE TABLE #TmpTable (" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                            command.CommandTimeout = 600;
                            command.CommandText = createTableCommand;
                            command.ExecuteNonQuery();

                            //Bulk insert into temp table
                            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                            {
                                bulkcopy.BulkCopyTimeout = 0;
                                bulkcopy.DestinationTableName = "#TmpTable";
                                bulkcopy.WriteToServer(table);
                                bulkcopy.Close();
                            }

                            // Updating destination table, and dropping temp table
                            command.CommandTimeout = 6000;
                            var updateTableCommand = @"MERGE " + tableSchema + "." + tableName + @" AS Target
    USING #TmpTable	AS Source
    ON Source." + idFieldName + " = Target." + idFieldName + @"

    -- For Inserts
    WHEN NOT MATCHED BY Target THEN
        INSERT (" + insertColumnNames.Remove(insertColumnNames.Length - 1) + @") 
        VALUES (" + insertColumnValues.Remove(insertColumnValues.Length - 1) + @")
    
    -- For Updates
    WHEN MATCHED THEN UPDATE SET
        " + setStatement.Remove(setStatement.Length - 1) + @";

DROP TABLE #TmpTable;";
                            command.CommandText = updateTableCommand;
                            command.ExecuteNonQuery();
                        }
                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }
        public static void UpdateDataByBatch<T>(List<T> list, string connstring, string tableSchema, string tableName, string idFieldName, int batchSize)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();
            var tables = dt.AsEnumerable().ToChunks(batchSize)
                          .Select(rows => rows.CopyToDataTable());

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        foreach (var prop in props)
                        {
                            var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                            if (propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                            tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                            if (prop.Name.ToLower() != idFieldName.ToLower())
                            {
                                setStatement = setStatement + "T." + prop.Name + "= temp." + prop.Name + ",";
                            }

                        }

                        foreach(var table in tables)
                        {
                            var createTableCommand = "CREATE TABLE #TmpTable(" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                            command.CommandText = createTableCommand;
                            command.ExecuteNonQuery();

                            //Bulk insert into temp table
                            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                            {
                                bulkcopy.BulkCopyTimeout = 660;
                                bulkcopy.DestinationTableName = "#TmpTable";
                                bulkcopy.WriteToServer(table);
                                bulkcopy.Close();
                            }

                            // Updating destination table, and dropping temp table
                            command.CommandTimeout = 600;
                            var updateTableCommand = "UPDATE T SET " + setStatement.Remove(setStatement.Length - 1) + " FROM " + tableName + " T INNER JOIN #TmpTable Temp ON T." + idFieldName + " = temp." + idFieldName + "; DROP TABLE #TmpTable;";
                            command.CommandText = updateTableCommand;
                            command.ExecuteNonQuery();
                        }
                        //Creating temp table on database
                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        public static void UpdateData<T>(List<T> list, string connstring, string tableSchema, string tableName, List<string> idFieldNames, DateTime datefrom, DateTime dateTo)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        string matchIdFields = "";
                        foreach (var prop in props)
                        {
                            var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                            if (propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                            tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                            if (!idFieldNames.Select(x => x.ToLower()).Contains(prop.Name.ToLower()))
                            {
                                setStatement = setStatement + "T." + prop.Name + "= temp." + prop.Name + ",";
                            }
                            else
                            {
                                matchIdFields = idFieldNames + "Target." + prop.Name + "= Source." + prop.Name + " and ";
                            }

                        }

                        //Creating temp table on database
                        var createTableCommand = "CREATE TABLE #TmpTable(" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                        command.CommandText = createTableCommand;
                        command.ExecuteNonQuery();

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            bulkcopy.BulkCopyTimeout = 660;
                            bulkcopy.DestinationTableName = "#TmpTable";
                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = 600;
                        var updateTableCommand = "UPDATE T SET " + setStatement.Remove(setStatement.Length - 1) + " FROM " + tableName + " T INNER JOIN #TmpTable Temp ON " + matchIdFields.Remove(matchIdFields.Length - 4) + "; DROP TABLE #TmpTable;";
                        command.CommandText = updateTableCommand;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        public static void UpsertData<T>(List<T> list, string connstring, string tableSchema, string tableName, string idFieldName)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        string insertColumnValues = "";
                        string insertColumnNames = "";
                        foreach (var prop in props)
                        {
                            if(prop.CanWrite && !prop.GetMethod.IsVirtual)
                            {
                                var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                                if (propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                                tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                                if (prop.Name.ToLower() != idFieldName.ToLower())
                                {
                                    setStatement = setStatement + "Target." + prop.Name + "= Source." + prop.Name + ",";

                                }
                                insertColumnValues = insertColumnValues + "Source." + prop.Name + ",";
                                insertColumnNames = insertColumnNames + prop.Name + ",";
                            }   
                        }

                        //Creating temp table on database
                        var createTableCommand = "CREATE TABLE #TmpTable (" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                        command.CommandTimeout = 600;
                        command.CommandText = createTableCommand;
                        command.ExecuteNonQuery();

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            bulkcopy.BatchSize = 10000;
                            bulkcopy.BulkCopyTimeout = 0;
                            bulkcopy.DestinationTableName = "#TmpTable";
                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = 6000;
                        var updateTableCommand = @"MERGE " + tableSchema + "." + tableName + @" AS Target
    USING #TmpTable	AS Source
    ON Source." + idFieldName + " = Target." + idFieldName + @"

    -- For Inserts
    WHEN NOT MATCHED BY Target THEN
        INSERT (" + insertColumnNames.Remove(insertColumnNames.Length - 1) + @") 
        VALUES (" + insertColumnValues.Remove(insertColumnValues.Length - 1) + @")
    
    -- For Updates
    WHEN MATCHED THEN UPDATE SET
        " + setStatement.Remove(setStatement.Length - 1) + @";

DROP TABLE #TmpTable;";
                        command.CommandText = updateTableCommand;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        public static int UpsertData<T>(List<T> list, string connstring, string tableSchema, string tableName, List<string> idFieldNames)
        {
            DataTable dt = new DataTable("MyTable");
            dt = list.ToDataTable();
            int affectedRows = 0;

            using (SqlConnection conn = new SqlConnection(connstring))
            {
                var tableColumns = new List<ColumnDescriptor>();

                using (SqlCommand command = new SqlCommand("", conn))
                {
                    command.Parameters.Add("@tableSchema", SqlDbType.Date).Value = tableSchema;
                    command.Parameters.Add("@tableName", SqlDbType.Date).Value = tableName;
                    tableColumns.AddRange(conn.Query<ColumnDescriptor>(@"WITH q AS (    
                                                SELECT
	                                                c.TABLE_SCHEMA,
                                                    c.TABLE_NAME,
                                                    c.COLUMN_NAME,
                                                    c.DATA_TYPE,
                                                    CASE
                                                        WHEN c.DATA_TYPE IN ( N'binary', N'varbinary'                    ) THEN ( CASE c.CHARACTER_OCTET_LENGTH   WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_OCTET_LENGTH  , N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'char', N'varchar', N'nchar', N'nvarchar' ) THEN ( CASE c.CHARACTER_MAXIMUM_LENGTH WHEN -1 THEN N'(max)' ELSE CONCAT( N'(', c.CHARACTER_MAXIMUM_LENGTH, N')' ) END )
                                                        WHEN c.DATA_TYPE IN ( N'datetime2', N'datetimeoffset'            ) THEN CONCAT( N'(', c.DATETIME_PRECISION, N')' )
                                                        WHEN c.DATA_TYPE IN ( N'decimal', N'numeric'                     ) THEN CONCAT( N'(', c.NUMERIC_PRECISION , N',', c.NUMERIC_SCALE, N')' )
                                                    END AS DATA_TYPE_PARAMETER
                                                FROM
                                                    INFORMATION_SCHEMA.COLUMNS AS c
                                            )
                                            SELECT
                                                q.COLUMN_NAME as ColumnName,
                                                CONCAT( q.DATA_TYPE, ISNULL( q.DATA_TYPE_PARAMETER, N'')) AS ColumnType

                                            FROM
                                                q
                                            WHERE
                                                q.TABLE_SCHEMA = @tableSchema AND
                                                q.TABLE_NAME   = @tableName ;", new { tableSchema = tableSchema, tableName = tableName }));

                }
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var props = typeof(T).GetProperties();
                        string tempTableColumns = "";
                        string setStatement = "";
                        string insertColumnValues = "";
                        string insertColumnNames = "";
                        string matchIdFields = "";
                        foreach (var prop in props)
                        {
                            if (prop.CanWrite && !prop.GetMethod.IsVirtual)
                            {
                                var propType = tableColumns.Where(x => x.ColumnName.ToLower() == prop.Name.ToLower()).FirstOrDefault().ColumnType;
                                if (propType is null) { throw new Exception("No column named " + prop.Name + " in table " + tableName); };
                                tempTableColumns = tempTableColumns + prop.Name + " " + propType + ",";
                                if (!idFieldNames.Select(x => x.ToLower()).Contains(prop.Name.ToLower()))
                                {
                                    setStatement = setStatement + "Target." + prop.Name + "= Source." + prop.Name + ",";

                                }
                                else
                                {
                                    matchIdFields = matchIdFields + "Target." + prop.Name + "= Source." + prop.Name + " and ";
                                }
                                insertColumnValues = insertColumnValues + "Source." + prop.Name + ",";
                                insertColumnNames = insertColumnNames + prop.Name + ",";
                            }
                        }

                        //Creating temp table on database
                        var createTableCommand = "CREATE TABLE #TmpTable (" + tempTableColumns.Remove(tempTableColumns.Length - 1) + ")";
                        command.CommandTimeout = 600;
                        command.CommandText = createTableCommand;
                        command.ExecuteNonQuery();

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            bulkcopy.BatchSize = 10000;
                            bulkcopy.BulkCopyTimeout = 0;
                            bulkcopy.DestinationTableName = "#TmpTable";
                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = 600;
                        var updateTableCommand = @"MERGE " + tableSchema + "." + tableName + @" AS Target
    USING #TmpTable	AS Source
    ON " + matchIdFields.Remove(matchIdFields.Length - 4) + @" 

    -- For Inserts
    WHEN NOT MATCHED BY Target THEN
        INSERT (" + insertColumnNames.Remove(insertColumnNames.Length - 1) + @") 
        VALUES (" + insertColumnValues.Remove(insertColumnValues.Length - 1) + @")
    
    -- For Updates
    WHEN MATCHED THEN UPDATE SET
        " + setStatement.Remove(setStatement.Length - 1) + @";
    ";
                        command.CommandText = updateTableCommand;
                        command.ExecuteNonQuery();

                        affectedRows = conn.Query<int>("select @@ROWCOUNT;DROP TABLE #TmpTable;").FirstOrDefault();
                        return affectedRows;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }

            return affectedRows;
        }

        public static DataTable ToDataTable<T>(this IList<T> data)
        {
            var properties = typeof(T).GetProperties();
            
            DataTable table = new DataTable();
            foreach (var prop in properties)
                if(prop.CanWrite && !prop.GetMethod.IsVirtual)
                {
                    table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
                }              
            foreach (T item in data)
            {
                DataRow row = table.NewRow();
                foreach (var prop in properties)
                    if(prop.CanWrite && !prop.GetMethod.IsVirtual)
                    {
                        row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                    }                   
                table.Rows.Add(row);
            }
            return table;
        }
    }
}

public class ColumnDescriptor
{
    public string ColumnName { get; set; }

    public string ColumnType { get; set; }
}

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("SqlBulkTools.UnitTests")]
[assembly: InternalsVisibleTo("SqlBulkTools.IntegrationTests")]
// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal static class BulkOperationsHelper
    {

        internal struct PrecisionType
        {
            public string NumericPrecision { get; set; }
            public string NumericScale { get; set; }
        }

        internal static string BuildCreateTempTable(HashSet<string> columns, DataTable schema, ColumnDirection outputIdentity)
        {
            Dictionary<string, string> actualColumns = new Dictionary<string, string>();
            Dictionary<string, string> actualColumnsMaxCharLength = new Dictionary<string, string>();
            Dictionary<string, PrecisionType> actualColumnsNumericPrecision = new Dictionary<string, PrecisionType>();
            Dictionary<string, string> actualColumnsDateTimePrecision = new Dictionary<string, string>();


            foreach (DataRow row in schema.Rows)
            {
                string columnType = row["DATA_TYPE"].ToString();
                string columnName = row["COLUMN_NAME"].ToString();

                actualColumns.Add(row["COLUMN_NAME"].ToString(), row["DATA_TYPE"].ToString());

                if (columnType == "varchar" || columnType == "nvarchar" ||
                    columnType == "char" || columnType == "binary" ||
                    columnType == "varbinary" || columnType == "nchar")

                {
                    actualColumnsMaxCharLength.Add(row["COLUMN_NAME"].ToString(),
                        row["CHARACTER_MAXIMUM_LENGTH"].ToString());
                }

                if (columnType == "datetime2" || columnType == "time")
                {
                    actualColumnsDateTimePrecision.Add(row["COLUMN_NAME"].ToString(), row["DATETIME_PRECISION"].ToString());
                }

                if (columnType == "numeric" || columnType == "decimal")
                {
                    PrecisionType p = new PrecisionType
                    {
                        NumericPrecision = row["NUMERIC_PRECISION"].ToString(),
                        NumericScale = row["NUMERIC_SCALE"].ToString()
                    };
                    actualColumnsNumericPrecision.Add(columnName, p);
                }

            }

            StringBuilder command = new StringBuilder();

            command.Append("CREATE TABLE " + Constants.TempTableName + "(");

            List<string> paramList = new List<string>();

            foreach (var column in columns.ToList())
            {
                if (column == Constants.InternalId)
                    continue;
                string columnType;
                if (actualColumns.TryGetValue(column, out columnType))
                {
                    columnType = GetVariableCharType(column, columnType, actualColumnsMaxCharLength);
                    columnType = GetDecimalPrecisionAndScaleType(column, columnType, actualColumnsNumericPrecision);
                    columnType = GetDateTimePrecisionType(column, columnType, actualColumnsDateTimePrecision);
                }

                paramList.Add("[" + column + "]" + " " + columnType);
            }

            string paramListConcatenated = string.Join(", ", paramList);

            command.Append(paramListConcatenated);

            if (outputIdentity == ColumnDirection.InputOutput)
            {
                command.Append(", [" + Constants.InternalId + "] int");
            }
            command.Append(");");

            return command.ToString();
        }

        private static string GetVariableCharType(string column, string columnType, Dictionary<string, string> actualColumnsMaxCharLength)
        {
            if (columnType == "varchar" || columnType == "nvarchar" ||
                    columnType == "char" || columnType == "binary" ||
                    columnType == "varbinary" || columnType == "nchar")
            {
                string maxCharLength;
                if (actualColumnsMaxCharLength.TryGetValue(column, out maxCharLength))
                {
                    if (maxCharLength == "-1")
                        maxCharLength = "max";

                    columnType = columnType + "(" + maxCharLength + ")";
                }
            }

            return columnType;
        }

        private static string GetDecimalPrecisionAndScaleType(string column, string columnType, Dictionary<string, PrecisionType> actualColumnsPrecision)
        {
            if (columnType == "decimal" || columnType == "numeric")
            {
                PrecisionType p;

                if (actualColumnsPrecision.TryGetValue(column, out p))
                {
                    columnType = columnType + "(" + p.NumericPrecision + ", " + p.NumericScale + ")";
                }
            }

            return columnType;
        }

        private static string GetDateTimePrecisionType(string column, string columnType, Dictionary<string, string> actualColumnsDateTimePrecision)
        {
            if (columnType == "datetime2" || columnType == "time")
            {
                string dateTimePrecision;
                if (actualColumnsDateTimePrecision.TryGetValue(column, out dateTimePrecision))
                {

                    columnType = columnType + "(" + dateTimePrecision + ")";
                }
            }

            return columnType;
        }

        internal static string BuildJoinConditionsForUpdateOrInsert(string[] updateOn, string sourceAlias, string targetAlias)
        {
            StringBuilder command = new StringBuilder();

            command.Append("ON " + "[" + targetAlias + "]" + "." + "[" + updateOn[0] + "]" + " = " + "[" + sourceAlias + "]" + "."
                + "[" + updateOn[0] + "]" + " ");

            if (updateOn.Length > 1)
            {
                // Start from index 1 to just append "AND" conditions
                for (int i = 1; i < updateOn.Length; i++)
                {
                    command.Append("AND " + "[" + targetAlias + "]" + "." + "[" + updateOn[i] + "]" + " = " + "[" +
                        sourceAlias + "]" + "." + "[" + updateOn[i] + "]" + " ");
                }
            }

            return command.ToString();
        }

        internal static string BuildPredicateQuery(string[] updateOn, IEnumerable<Condition> conditions, string targetAlias)
        {
            if (conditions == null)
                return null;

            if (updateOn == null || updateOn.Length == 0)
                throw new SqlBulkToolsException("MatchTargetOn is required for AndQuery.");

            StringBuilder command = new StringBuilder();

            foreach (var condition in conditions)
            {
                string targetColumn = condition.CustomColumnMapping ?? condition.LeftName;

                command.Append("AND [" + targetAlias + "].[" + targetColumn + "] " +
                               GetOperator(condition) + " " + (condition.Value != "NULL" ? ("@" + 
                               condition.LeftName + Constants.UniqueParamIdentifier + condition.SortOrder) : "NULL") + " ");
            }

            return command.ToString();

        }

        // Used for UpdateQuery and DeleteQuery where, and, or conditions. 
        internal static string BuildPredicateQuery(IEnumerable<Condition> conditions)
        {
            if (conditions == null)
                return null;

            conditions = conditions.OrderBy(x => x.SortOrder);

            StringBuilder command = new StringBuilder();

            foreach (var condition in conditions)
            {
                string targetColumn = condition.CustomColumnMapping ?? condition.LeftName;

                switch (condition.PredicateType)
                {
                    case PredicateType.Where:
                    {
                        command.Append(" WHERE ");
                        break;
                    }

                    case PredicateType.And:
                    {
                            command.Append(" AND ");
                            break;
                    }

                    case PredicateType.Or:
                        {
                            command.Append(" OR ");
                            break;
                        }

                    default:
                    {
                        throw new KeyNotFoundException("Predicate not found");
                    }
                }

                command.Append("[" + targetColumn + "] " +
                               GetOperator(condition) + " " + (condition.Value != "NULL" ? ("@" + condition.LeftName + Constants.UniqueParamIdentifier + condition.SortOrder) : "NULL"));
            }

            return command.ToString();

        }

        internal static string GetOperator(Condition condition)
        {
            switch (condition.Expression)
            {
                case ExpressionType.NotEqual:
                    {
                        if (condition.ValueType == null)
                        {
                            condition.Value = condition.Value?.ToUpper();
                            return "IS NOT";
                        }

                        return "!=";
                    }
                case ExpressionType.Equal:
                    {
                        if (condition.ValueType == null)
                        {
                            condition.Value = condition.Value?.ToUpper();
                            return "IS";
                        }

                        return "=";
                    }
                case ExpressionType.LessThan:
                    {
                        return "<";
                    }
                case ExpressionType.LessThanOrEqual:
                    {
                        return "<=";
                    }
                case ExpressionType.GreaterThan:
                    {
                        return ">";
                    }
                case ExpressionType.GreaterThanOrEqual:
                    {
                        return ">=";
                    }
            }

            throw new SqlBulkToolsException("ExpressionType not found when trying to map logical operator.");

        }

        internal static string BuildUpdateSet(HashSet<string> columns, string sourceAlias, string targetAlias, string identityColumn)
        {
            StringBuilder command = new StringBuilder();
            List<string> paramsSeparated = new List<string>();

            command.Append("SET ");

            foreach (var column in columns.ToList())
            {
                if (identityColumn != null && column != identityColumn || identityColumn == null)
                {
                    if (column != Constants.InternalId)
                        paramsSeparated.Add("[" + targetAlias + "]" + "." + "[" + column + "]" + " = " + "[" + sourceAlias + "]" + "."
                            + "[" + column + "]");
                }
            }

            command.Append(string.Join(", ", paramsSeparated) + " ");

            return command.ToString();
        }

        /// <summary>
        /// Specificially for UpdateQuery and DeleteQuery
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="transactionCount"></param>
        /// <param name="identityColumn"></param>
        /// <returns></returns>
        internal static string BuildUpdateSet(HashSet<string> columns, string identityColumn)
        {
            StringBuilder command = new StringBuilder();
            List<string> paramsSeparated = new List<string>();

            command.Append("SET ");

            foreach (var column in columns.ToList())
            {
                if (column != identityColumn)
                    paramsSeparated.Add($"[{column}] = @{column}");
            }

            command.Append(string.Join(", ", paramsSeparated));

            return command.ToString();
        }

        internal static string BuildInsertSet(HashSet<string> columns, string sourceAlias, string identityColumn)
        {
            StringBuilder command = new StringBuilder();
            List<string> insertColumns = new List<string>();
            List<string> values = new List<string>();

            command.Append("INSERT (");

            foreach (var column in columns.ToList())
            {

                if (column != Constants.InternalId && column != identityColumn)
                {
                    insertColumns.Add("[" + column + "]");
                    values.Add("[" + sourceAlias + "]" + "." + "[" + column + "]");
                }

            }

            command.Append(string.Join(", ", insertColumns));
            command.Append(") values (");
            command.Append(string.Join(", ", values));
            command.Append(")");

            return command.ToString();
        }

        internal static string BuildInsertIntoSet(HashSet<string> columns, string identityColumn, string fullQualifiedTableName)
        {
            StringBuilder command = new StringBuilder();
            List<string> insertColumns = new List<string>();

            command.Append("INSERT INTO ");
            command.Append(fullQualifiedTableName);
            command.Append(" (");

            foreach (var column in columns)
            {
                if (column != Constants.InternalId && column != identityColumn)
                    insertColumns.Add("[" + column + "]");
            }

            command.Append(string.Join(", ", insertColumns));
            command.Append(") ");

            return command.ToString();
        }

        internal static string BuildValueSet(HashSet<string> columns, string identityColumn)
        {
            StringBuilder command = new StringBuilder();
            List<string> valueList = new List<string>();

            command.Append("(");
            foreach (var column in columns)
            {
                if (column != identityColumn)
                    valueList.Add($"@{column}");
            }
            command.Append(string.Join(", ", valueList));
            command.Append(")");

            return command.ToString();
        }

        internal static string BuildSelectSet(HashSet<string> columns, string sourceAlias, string identityColumn)
        {
            StringBuilder command = new StringBuilder();
            List<string> selectColumns = new List<string>();
            List<string> values = new List<string>();

            command.Append("SELECT ");

            foreach (var column in columns.ToList())
            {
                if (identityColumn != null && column != identityColumn || identityColumn == null)
                {
                    if (column != Constants.InternalId)
                    {
                        selectColumns.Add("[" + sourceAlias + "].[" + column + "]");
                        values.Add("[" + column + "]");
                    }
                }
            }

            command.Append(string.Join(", ", selectColumns));

            return command.ToString();
        }

        internal static string GetPropertyName(Expression method)
        {
            LambdaExpression lambda = method as LambdaExpression;
            if (lambda == null)
                throw new ArgumentNullException("method");

            MemberExpression memberExpr = null;

            if (lambda.Body.NodeType == ExpressionType.Convert)
            {
                memberExpr =
                    ((UnaryExpression)lambda.Body).Operand as MemberExpression;
            }
            else if (lambda.Body.NodeType == ExpressionType.MemberAccess)
            {
                memberExpr = lambda.Body as MemberExpression;
            }

            if (memberExpr == null)
                throw new ArgumentException("method");

            return memberExpr.Member.Name;
        }

        internal static DataTable CreateDataTable<T>(HashSet<string> columns, Dictionary<string, string> columnMappings,
            List<string> matchOnColumns = null, ColumnDirection? outputIdentity = null)
        {
            if (columns == null)
                return null;

            DataTable dataTable = new DataTable(typeof(T).Name);

            if (matchOnColumns != null)
            {
                columns = CheckForAdditionalColumns(columns, matchOnColumns);
            }

            if (outputIdentity.HasValue && outputIdentity.Value == ColumnDirection.InputOutput)
            {
                columns.Add(Constants.InternalId);
            }

            //Get all the properties
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var column in columns.ToList())
            {
                if (columnMappings != null && columnMappings.ContainsKey(column))
                {
                    dataTable.Columns.Add(columnMappings[column]);
                }

                else
                    dataTable.Columns.Add(column);
            }

            AssignTypes(props, columns, dataTable);

            return dataTable;
        }

        public static DataTable ConvertListToDataTable<T>(DataTable dataTable, IEnumerable<T> list, HashSet<string> columns,
            Dictionary<int, T> outputIdentityDic = null)
        {

            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            int counter = 0;

            foreach (T item in list)
            {

                var values = new List<object>();

                foreach (var column in columns.ToList())
                {
                    if (column == Constants.InternalId)
                    {
                        values.Add(counter);
                        outputIdentityDic?.Add(counter, item);
                    }
                    else
                        for (int i = 0; i < props.Length; i++)
                        {
                            if (props[i].Name == column && item != null
                                && CheckForValidDataType(props[i].PropertyType, throwIfInvalid: true))
                                values.Add(props[i].GetValue(item, null));


                        }

                }
                counter++;
                dataTable.Rows.Add(values.ToArray());

            }
            return dataTable;

        }

        // Loops through object properties, checks if column has been added, adds as sql parameter. Used for the UpdateQuery method. 
        public static void AddSqlParamsForQuery<T>(List<SqlParameter> sqlParameters, HashSet<string> columns, T item, 
            string identityColumn = null, ColumnDirection direction = ColumnDirection.Input, Dictionary<string, string> customColumns = null)
        {
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var column in columns.ToList())
            {
                for (int i = 0; i < props.Length; i++)
                {
                    if (props[i].Name == column && item != null && CheckForValidDataType(props[i].PropertyType, throwIfInvalid: true))
                    {
                        DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(props[i].PropertyType);

                        string actualColumnName;
                        SqlParameter param;

                        if (customColumns != null && customColumns.TryGetValue(column, out actualColumnName))
                        {
                            param = new SqlParameter($"@{actualColumnName}", sqlType);
                        }
                        else
                            param = new SqlParameter($"@{column}", sqlType);

                        object propValue = props[i].GetValue(item, null);

                        if (propValue == null)
                        {
                            param.Value = DBNull.Value;
                        }

                        else
                            param.Value = propValue;
                       
                        if (column == identityColumn && direction == ColumnDirection.InputOutput)
                            param.Direction = ParameterDirection.Output;

                        sqlParameters.Add(param);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="throwIfInvalid">
        /// Set this to true if user is manually adding columns. If AddAllColumns is used, then this can be omitted. 
        /// </param>
        /// <returns></returns>
        private static bool CheckForValidDataType(Type type, bool throwIfInvalid = false)
        {
            if (type.IsValueType ||
                type == typeof(string) ||
                type == typeof(byte[]) ||
                type == typeof(char[]) ||
                type == typeof(SqlXml)
                )
                return true;

            if (throwIfInvalid)
                throw new SqlBulkToolsException("Only value, string, char[], byte[] " +
                                                    "and SqlXml types can be used with SqlBulkTools. " +
                                                    "Refer to https://msdn.microsoft.com/en-us/library/cc716729(v=vs.110).aspx for more details.");

            return false;
        }

        private static void AssignTypes(PropertyInfo[] props, HashSet<string> columns, DataTable dataTable)
        {
            int count = 0;

            foreach (var column in columns.ToList())
            {
                if (column == Constants.InternalId)
                {
                    dataTable.Columns[count].DataType = typeof(int);
                }
                else
                    for (int i = 0; i < props.Length; i++)
                    {
                        if (props[i].Name == column)
                        {
                            dataTable.Columns[count].DataType = Nullable.GetUnderlyingType(props[i].PropertyType) ??
                                                                props[i].PropertyType;
                        }
                    }
                count++;
            }
        }

        internal static SqlConnection GetSqlConnection(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            SqlConnection conn = null;

            if (connection != null)
            {
                conn = connection;
                return conn;
            }

            if (connectionName != null)
            {
                conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings[connectionName].ConnectionString, credentials);
                return conn;
            }

            throw new SqlBulkToolsException("Could not create SQL connection. Please check your arguments into CommitTransaction");
        }

        internal static string GetFullQualifyingTableName(string databaseName, string schemaName, string tableName)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("[");
            sb.Append(databaseName);
            sb.Append("].[");
            sb.Append(schemaName);
            sb.Append("].[");
            sb.Append(tableName);
            sb.Append("]");

            return sb.ToString();
        }


        /// <summary>
        /// If there are MatchOnColumns that don't exist in columns, add to columns.
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="matchOnColumns"></param>
        /// <returns></returns>
        internal static HashSet<string> CheckForAdditionalColumns(HashSet<string> columns, List<string> matchOnColumns)
        {
            foreach (var col in matchOnColumns)
            {
                if (!columns.Contains(col))
                {
                    columns.Add(col);
                }
            }

            return columns;
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings, HashSet<string> columns,
        List<string> updateOnList)
        {
            if (columnMappings.Count > 0)
            {
                foreach (var column in columnMappings)
                {
                    if (columns.Contains(column.Key))
                    {
                        columns.Remove(column.Key);
                        columns.Add(column.Value);
                    }

                    for (int i = 0; i < updateOnList.ToArray().Length; i++)
                    {
                        if (updateOnList[i] == column.Key)
                        {
                            updateOnList[i] = column.Value;
                        }
                    }
                }
            }
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings, HashSet<string> columns)
        {
            if (columnMappings.Count > 0)
            {
                foreach (var column in columnMappings)
                {
                    if (columns.Contains(column.Key))
                    {
                        columns.Remove(column.Key);
                        columns.Add(column.Value);
                    }
                }
            }
        }

        internal static void DoColumnMappings(Dictionary<string, string> columnMappings, List<Condition> predicateConditions)
        {
            foreach (var condition in predicateConditions)
            {
                string columnName;

                if (columnMappings.TryGetValue(condition.LeftName, out columnName))
                {
                    condition.CustomColumnMapping = columnName;
                }
            }
        }

        /// <summary>
        /// Advanced Settings for SQLBulkCopy class. 
        /// </summary>
        /// <param name="bulkcopy"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyDelegates"></param>
        internal static void SetSqlBulkCopySettings(SqlBulkCopy bulkcopy, bool bulkCopyEnableStreaming, int? bulkCopyBatchSize,
            int? bulkCopyNotifyAfter, int bulkCopyTimeout, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            bulkcopy.EnableStreaming = bulkCopyEnableStreaming;

            if (bulkCopyBatchSize.HasValue)
            {
                bulkcopy.BatchSize = bulkCopyBatchSize.Value;
            }

            if (bulkCopyNotifyAfter.HasValue)
            {
                bulkcopy.NotifyAfter = bulkCopyNotifyAfter.Value;
                bulkCopyDelegates?.ToList().ForEach(x => bulkcopy.SqlRowsCopied += x);
            }

            bulkcopy.BulkCopyTimeout = bulkCopyTimeout;
        }


        /// <summary>
        /// This is used only for the BulkInsert method at this time.  
        /// </summary>
        /// <param name="bulkCopy"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        internal static void MapColumns(SqlBulkCopy bulkCopy, HashSet<string> columns, Dictionary<string, string> customColumnMappings)
        {

            foreach (var column in columns.ToList())
            {
                string mapping;

                if (customColumnMappings.TryGetValue(column, out mapping))
                {
                    bulkCopy.ColumnMappings.Add(mapping, mapping);
                }

                else
                    bulkCopy.ColumnMappings.Add(column, column);
            }

        }

        internal static HashSet<string> GetAllValueTypeAndStringColumns(Type type)
        {
            HashSet<string> columns = new HashSet<string>();

            //Get all the properties
            PropertyInfo[] props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            for (int i = 0; i < props.Length; i++)
            {
                if (CheckForValidDataType(props[i].PropertyType))
                {
                    columns.Add(props[i].Name);
                }
            }

            return columns;

        }

        internal static string GetOutputIdentityCmd(string identityColumn, ColumnDirection outputIdentity, string tmpTableName, OperationType operation)
        {

            StringBuilder sb = new StringBuilder();
            if (identityColumn == null || outputIdentity != ColumnDirection.InputOutput)
            {
                return null;
            }
            if (operation == OperationType.Insert)
                sb.Append("OUTPUT INSERTED." + identityColumn + " INTO " + tmpTableName + "(" + identityColumn + "); ");

            else if (operation == OperationType.InsertOrUpdate || operation == OperationType.Update)
                sb.Append("OUTPUT Source." + Constants.InternalId + ", INSERTED." + identityColumn + " INTO " + tmpTableName
                    + "(" + Constants.InternalId + ", " + identityColumn + "); ");

            else if (operation == OperationType.Delete)
                sb.Append("OUTPUT Source." + Constants.InternalId + ", DELETED." + identityColumn + " INTO " + tmpTableName
                    + "(" + Constants.InternalId + ", " + identityColumn + "); ");

            return sb.ToString();
        }

        internal static string GetOutputCreateTableCmd(ColumnDirection outputIdentity, string tmpTablename, OperationType operation, string identityColumn)
        {

            if (operation == OperationType.Insert)
                return (outputIdentity == ColumnDirection.InputOutput ? "CREATE TABLE " + tmpTablename + "(" + "[" + identityColumn + "] int); " : "");

            else if (operation == OperationType.InsertOrUpdate || operation == OperationType.Update || operation == OperationType.Delete)
                return (outputIdentity == ColumnDirection.InputOutput ? "CREATE TABLE " + tmpTablename + "("
                    + "[" + Constants.InternalId + "]" + " int, [" + identityColumn + "] int); " : "");

            return string.Empty;
        }

        internal static string GetDropTmpTableCmd()
        {
            return "DROP TABLE " + Constants.TempOutputTableName + ";";
        }

        internal static string GetIndexManagementCmd(string action, string tableName,
            string schema, IDbConnection conn, HashSet<string> disableIndexList, bool disableAllIndexes = false)
        {
            StringBuilder sb = new StringBuilder();

            if (disableIndexList != null && disableIndexList.Any())
            {
                foreach (var index in disableIndexList)
                {
                    sb.Append(" AND sys.indexes.name = \'");
                    sb.Append(index);
                    sb.Append("\'");
                }
            }

            string cmd = "DECLARE @sql AS VARCHAR(MAX)=''; " +
                                "SELECT @sql = @sql + " +
                                "'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' " + action + ";' " +
                                "FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id " +
                                "WHERE sys.indexes.type_desc = 'NONCLUSTERED' " +
                                "AND sys.objects.type_desc = 'USER_TABLE' " +
                                "AND sys.objects.name = '" + GetFullQualifyingTableName(conn.Database, schema, tableName)
                                + "'" + (sb.Length > 0 ? sb.ToString() : "") + "; EXEC(@sql);";

            return cmd;
        }

        /// <summary>
        /// Gets schema information for a table. Used to get SQL type of property. 
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal static DataTable GetDatabaseSchema(SqlConnection conn, string schema, string tableName)
        {
            string[] restrictions = new string[4];
            restrictions[0] = conn.Database;
            restrictions[1] = schema;
            restrictions[2] = tableName;
            var dtCols = conn.GetSchema("Columns", restrictions);

            if (dtCols.Rows.Count == 0 && schema != null)
                throw new SqlBulkToolsException("Table name '" + tableName
                + "\' with schema name \'" + schema + "\' not found. Check your setup and try again.");

            if (dtCols.Rows.Count == 0)
                throw new SqlBulkToolsException("Table name \'" + tableName
                + "\' not found. Check your setup and try again.");
            return dtCols;
        }

        internal static void InsertToTmpTable(SqlConnection conn, DataTable dt, bool bulkCopyEnableStreaming,
            int? bulkCopyBatchSize, int? bulkCopyNotifyAfter, int bulkCopyTimeout, SqlBulkCopyOptions sqlBulkCopyOptions, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, sqlBulkCopyOptions, null))
            {
                bulkcopy.DestinationTableName = Constants.TempTableName;

                SetSqlBulkCopySettings(bulkcopy, bulkCopyEnableStreaming,
                    bulkCopyBatchSize,
                    bulkCopyNotifyAfter, bulkCopyTimeout, bulkCopyDelegates);

                bulkcopy.WriteToServer(dt);
            }
        }

        internal static async Task InsertToTmpTableAsync(SqlConnection conn, SqlTransaction transaction, DataTable dt, bool bulkCopyEnableStreaming,
            int? bulkCopyBatchSize, int? bulkCopyNotifyAfter, int bulkCopyTimeout, SqlBulkCopyOptions sqlBulkCopyOptions, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, sqlBulkCopyOptions, transaction))
            {
                bulkcopy.DestinationTableName = Constants.TempTableName;

                SetSqlBulkCopySettings(bulkcopy, bulkCopyEnableStreaming,
                    bulkCopyBatchSize,
                    bulkCopyNotifyAfter, bulkCopyTimeout, bulkCopyDelegates);

                await bulkcopy.WriteToServerAsync(dt);
            }
        }

        internal static void LoadFromTmpOutputTable<T>(SqlCommand command, string identityColumn, Dictionary<int, T> outputIdentityDic,
            OperationType operationType, IEnumerable<T> list)
        {
            if (operationType == OperationType.InsertOrUpdate
                || operationType == OperationType.Update
                || operationType == OperationType.Delete)
            {
                command.CommandText = "SELECT " + Constants.InternalId + ", " + identityColumn + " FROM "
                    + Constants.TempOutputTableName + ";";

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        T item;

                        if (outputIdentityDic.TryGetValue((int)reader[0], out item))
                        {
                            PropertyInfo p = item.GetType().GetProperty(identityColumn);

                            if (p.CanWrite)
                                p.SetValue(item, reader[1], null);

                            else
                                throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));
                        }

                    }
                }

                command.CommandText = GetDropTmpTableCmd();
                command.ExecuteNonQuery();
            }

            if (operationType == OperationType.Insert)
            {
                command.CommandText = "SELECT " + identityColumn + " FROM " + Constants.TempOutputTableName + ";";

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var items = list.ToList();
                    int counter = 0;

                    while (reader.Read())
                    {
                        PropertyInfo p = items[counter].GetType().GetProperty(identityColumn);

                        if (p.CanWrite)
                            p.SetValue(items[counter], reader[0], null);

                        else
                            throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));

                        counter++;
                    }
                }

                command.CommandText = GetDropTmpTableCmd();
                command.ExecuteNonQuery();
            }
        }

        internal static async Task LoadFromTmpOutputTableAsync<T>(SqlCommand command, string identityColumn,
            Dictionary<int, T> outputIdentityDic, OperationType operationType, IEnumerable<T> list)
        {
            if (operationType == OperationType.InsertOrUpdate
                || operationType == OperationType.Update
                || operationType == OperationType.Delete)
            {
                command.CommandText = "SELECT " + Constants.InternalId + ", " + identityColumn + " FROM "
                    + Constants.TempOutputTableName + ";";

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        T item;

                        if (outputIdentityDic.TryGetValue((int)reader[0], out item))
                        {
                            PropertyInfo p = item.GetType().GetProperty(identityColumn);

                            if (p.CanWrite)
                                p.SetValue(item, reader[1], null);

                            else
                                throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));
                        }

                    }
                }

                command.CommandText = GetDropTmpTableCmd();
                await command.ExecuteNonQueryAsync();
            }

            if (operationType == OperationType.Insert)
            {
                command.CommandText = "SELECT " + identityColumn + " FROM " + Constants.TempOutputTableName + ";";

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    var items = list.ToList();
                    int counter = 0;

                    while (reader.Read())
                    {
                        PropertyInfo p = items[counter].GetType().GetProperty(identityColumn);

                        if (p.CanWrite)
                            p.SetValue(items[counter], reader[0], null);

                        else
                            throw new SqlBulkToolsException(GetPrivateSetterExceptionMessage(identityColumn));

                        counter++;
                    }
                }

                command.CommandText = GetDropTmpTableCmd();
                await command.ExecuteNonQueryAsync();
            }
        }

        private static string GetPrivateSetterExceptionMessage(string columnName)
        {
            return $"No setter method available on property '{columnName}'. Could not write output back to property.";
        }

        internal static string GetInsertIntoStagingTableCmd(SqlCommand command, SqlConnection conn, string schema, string tableName,
            HashSet<string> columns, string identityColumn, ColumnDirection outputIdentity)
        {

            string fullTableName = GetFullQualifyingTableName(conn.Database, schema,
                tableName);

            string comm =
            GetOutputCreateTableCmd(outputIdentity, Constants.TempOutputTableName,
            OperationType.Insert, identityColumn) +
            BuildInsertIntoSet(columns, identityColumn, fullTableName)
            + "OUTPUT INSERTED.[" + identityColumn + "] INTO "
            + Constants.TempOutputTableName + "([" + identityColumn + "]) "
            + BuildSelectSet(columns, Constants.SourceAlias, identityColumn)
            + " FROM " + Constants.TempTableName + " AS Source; " +
            "DROP TABLE " + Constants.TempTableName + ";";

            return comm;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="predicateType"></param>
        /// <param name="predicateList"></param>
        /// <param name="sqlParamsList"></param>
        /// <param name="sortOrder"></param>
        /// <param name="appendParam"></param>
        internal static void AddPredicate<T>(Expression<Func<T, bool>> predicate, PredicateType predicateType, List<Condition> predicateList, 
            List<SqlParameter> sqlParamsList, int sortOrder, string appendParam)
        {
            string leftName;
            string value;
            Condition condition;

            BinaryExpression binaryBody = predicate.Body as BinaryExpression;

            if (binaryBody == null)
                throw new SqlBulkToolsException($"Expression not supported for {GetPredicateMethodName(predicateType)}");

            // For expression types Equal and NotEqual, it's possible for user to pass null value. This handles the null use case. 
            // SqlParameter is not added when comparison to null value is used. 
            switch (predicate.Body.NodeType)
            {
                case ExpressionType.NotEqual:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();


                        if (value != null)
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.NotEqual,
                                LeftName = leftName,
                                ValueType = binaryBody.Right.Type,
                                Value = value,
                                PredicateType = predicateType,
                                SortOrder = sortOrder
                            };

                            DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);

                            string paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
                            SqlParameter param = new SqlParameter($"@{paramName}", sqlType);
                            param.Value = condition.Value;
                            sqlParamsList.Add(param);
                        }
                        else
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.NotEqual,
                                LeftName = leftName,
                                Value = "NULL",
                                PredicateType = predicateType,
                                SortOrder = sortOrder
                            };
                        }

                        predicateList.Add(condition);


                        break;
                    }

                // For expression types Equal and NotEqual, it's possible for user to pass null value. This handles the null use case. 
                // SqlParameter is not added when comparison to null value is used. 
                case ExpressionType.Equal:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();

                        if (value != null)
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.Equal,
                                LeftName = leftName,
                                ValueType = binaryBody.Right.Type,
                                Value = value, 
                                PredicateType = predicateType,
                                SortOrder = sortOrder
                            };

                            DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);
                            string paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
                            SqlParameter param = new SqlParameter($"@{paramName}", sqlType);
                            param.Value = condition.Value;
                            sqlParamsList.Add(param);
                        }
                        else
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.Equal,
                                LeftName = leftName,
                                Value = "NULL", 
                                PredicateType = predicateType,
                                SortOrder = sortOrder
                            };
                        }

                            predicateList.Add(condition);

                        break;
                    }
                case ExpressionType.LessThan:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThan, predicateList, sqlParamsList, 
                            predicateType, sortOrder, appendParam);
                        break;
                    }
                case ExpressionType.LessThanOrEqual:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThanOrEqual, predicateList, 
                            sqlParamsList, predicateType, sortOrder, appendParam);
                        break;
                    }
                case ExpressionType.GreaterThan:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThan, predicateList, 
                            sqlParamsList, predicateType, sortOrder, appendParam);
                        break;
                    }
                case ExpressionType.GreaterThanOrEqual:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThanOrEqual, predicateList, 
                            sqlParamsList, predicateType, sortOrder, appendParam);
                        break;
                    }
                case ExpressionType.AndAlso:
                    {
                        throw new SqlBulkToolsException($"And && expression not supported for {GetPredicateMethodName(predicateType)}. " +
                                                        $"Try chaining predicates instead e.g. {GetPredicateMethodName(predicateType)}." +
                                                        $"{GetPredicateMethodName(predicateType)}");
                    }
                case ExpressionType.OrElse:
                    {
                        throw new SqlBulkToolsException($"Or || expression not supported for {GetPredicateMethodName(predicateType)}.");
                    }

                default:
                    {
                        throw new SqlBulkToolsException($"Expression used in {GetPredicateMethodName(predicateType)} not supported. " +
                                                        $"Only == != < <= > >= expressions are accepted.");
                    }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="predicateType"></param>
        /// <returns></returns>
        internal static string GetPredicateMethodName(PredicateType predicateType)
        {
            return predicateType == PredicateType.Update
                ? "UpdateWhen(...)"
                : predicateType == PredicateType.Delete ?
                "DeleteWhen(...)"
                : predicateType == PredicateType.Where ? 
                "Where(...)"
                : predicateType == PredicateType.And ?
                "And(...)"
                : predicateType == PredicateType.Or ?
                "Or(...)"
                : string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="leftName"></param>
        /// <param name="value"></param>
        /// <param name="valueType"></param>
        /// <param name="expressionType"></param>
        /// <param name="predicateList"></param>
        /// <param name="sqlParamsList"></param>
        /// <param name="sortOrder"></param>
        /// <param name="appendParam"></param>
        /// <param name="predicateType"></param>
        internal static void BuildCondition(string leftName, string value, Type valueType, ExpressionType expressionType, 
            List<Condition> predicateList, List<SqlParameter> sqlParamsList, PredicateType predicateType, int sortOrder, string appendParam)
        {

            Condition condition = new Condition()
            {
                Expression = expressionType,
                LeftName = leftName,
                ValueType = valueType,
                Value = value, 
                PredicateType = predicateType,
                SortOrder = sortOrder
            };

            predicateList.Add(condition);


            DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);
            string paramName = appendParam != null ? leftName + appendParam + sortOrder : leftName;
            SqlParameter param = new SqlParameter($"@{paramName}", sqlType);
            param.Value = condition.Value;
            sqlParamsList.Add(param);

        }
    }
}

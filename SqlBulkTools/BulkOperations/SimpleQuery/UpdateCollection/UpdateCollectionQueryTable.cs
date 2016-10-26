using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpdateCollectionQueryTable<T>
    {
        private readonly DataTable _smallCollection;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private readonly BulkOperations _ext;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;
        private int _transactionCount;
        private string _databaseIdentifier;
        private List<string> _concatTrans;
        private List<SqlParameter> _sqlParams;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="ext"></param>
        public UpdateCollectionQueryTable(DataTable smallCollection, string tableName, BulkOperations ext, int transactionCount, string databaseIdentifier, List<string> concatTrans, List<SqlParameter> sqlParams)
        {
            _smallCollection = smallCollection;
            _sqlTimeout = 600;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            _ext = ext;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _transactionCount = transactionCount;
            _databaseIdentifier = databaseIdentifier;
            _concatTrans = concatTrans;
            _sqlParams = sqlParams;
        }

        /// <summary>
        /// Add each column that you want to include in the query.
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public UpdateCollectionQueryAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new UpdateCollectionQueryAddColumn<T>(_smallCollection, _tableName, Columns, _schema, 
                _sqlTimeout, _ext, _transactionCount, _databaseIdentifier, _concatTrans, _sqlParams);
        }

        /// <summary>
        /// Explicitley set a schema if your table may have a naming conflict within your database. 
        /// If a schema is not added, the system default schema name 'dbo' will used.. 
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public UpdateCollectionQueryTable<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public UpdateCollectionQueryTable<T> WithSqlCommandTimeout(int seconds)
        {
            _sqlTimeout = seconds;
            return this;
        }
    }
}
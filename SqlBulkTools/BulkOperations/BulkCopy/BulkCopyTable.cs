using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.BulkCopy
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkCopyTable<T>
    {
        private readonly IEnumerable<T> _list;
        private int _bulkCopyTimeout;
        private bool _bulkCopyEnableStreaming;
        private int? _bulkCopyNotifyAfter;
        private int? _bulkCopyBatchSize;
        private SqlBulkCopyOptions _sqlBulkCopyOptions;
        private IEnumerable<SqlRowsCopiedEventHandler> _bulkCopyDelegates;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private readonly BulkOperations _ext;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="ext"></param>
        public BulkCopyTable(IEnumerable<T> list, string tableName, BulkOperations ext)
        {
            _bulkCopyBatchSize = null;
            _bulkCopyNotifyAfter = null;
            _bulkCopyEnableStreaming = false;
            _sqlTimeout = 600;
            _bulkCopyTimeout = 600;
            _list = list;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _sqlBulkCopyOptions = SqlBulkCopyOptions.Default;
            _tableName = tableName;
            _ext = ext;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public ColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new ColumnSelect<T>(_list, _tableName, Columns, _schema, 
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext, _bulkCopyDelegates);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public AllColumnSelect<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(typeof(T));
            return new AllColumnSelect<T>(_list, _tableName, Columns, _schema, 
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext, _bulkCopyDelegates);
        }

        /// <summary>
        /// Explicitley set a schema if your table may have a naming conflict within your database. 
        /// If a schema is not added, the system default schema name 'dbo' will used.. 
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithSqlCommandTimeout(int seconds)
        {
            _sqlTimeout = seconds;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithBulkCopyCommandTimeout(int seconds)
        {
            _bulkCopyTimeout = seconds;
            return this;
        }

        /// <summary>
        /// Default is false. See docs for more info.
        /// </summary>
        /// <param name="status"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithBulkCopyEnableStreaming(bool status)
        {
            _bulkCopyEnableStreaming = status;
            return this;
        }

        /// <summary>
        /// Triggers an event after x rows inserted. See docs for more info. 
        /// </summary>
        /// <param name="rows"></param>
        /// <param name="bulkCopyDelegates"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithBulkCopyNotifyAfter(int rows, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            _bulkCopyNotifyAfter = rows;
            _bulkCopyDelegates = bulkCopyDelegates;
            return this;
        }

        /// <summary>
        /// Default is 0. See docs for more info. 
        /// </summary>
        /// <param name="rows"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithBulkCopyBatchSize(int rows)
        {
            _bulkCopyBatchSize = rows;
            return this;
        }

        /// <summary>
        /// Enum representing options for SqlBulkCopy. Unless explicitely set, the default option will be used. 
        /// See https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopyoptions(v=vs.110).aspx for a list of available options. 
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public BulkCopyTable<T> WithSqlBulkCopyOptions(SqlBulkCopyOptions options)
        {           
            _sqlBulkCopyOptions = options;
            return this;
        }

    }
}
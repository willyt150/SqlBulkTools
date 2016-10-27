using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryTable<T>
    {
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private readonly BulkOperations _ext;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="ext"></param>
        public DeleteQueryTable(string tableName, BulkOperations ext)
        {
            _sqlTimeout = 600;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            _ext = ext;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DeleteQuery<T> Delete()
        {
            return new DeleteQuery<T>(_tableName, _schema, _sqlTimeout, _ext);
        }

        /// <summary>
        /// Explicitley set a schema if your table may have a naming conflict within your database. 
        /// If a schema is not added, the system default schema name 'dbo' will used.. 
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public DeleteQueryTable<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public DeleteQueryTable<T> WithSqlCommandTimeout(int seconds)
        {
            _sqlTimeout = seconds;
            return this;
        }
    }
}
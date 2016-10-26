using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpsertQueryAddColumnList<T>
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private Dictionary<string, string> _customColumnMappings { get; }
        private HashSet<string> _columns;
        private readonly string _schema;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private List<SqlParameter> _parameters;
        private List<string> _concatTrans;
        private string _databaseIdentifier;
        private List<SqlParameter> _sqlParams;
        private int _transactionCount;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="ext"></param>
        /// <param name="concatTrans"></param>
        /// <param name="databaseIdentifier"></param>
        /// <param name="sqlParams"></param>
        /// <param name="insertMode"></param>
        public UpsertQueryAddColumnList(T singleEntity, string tableName, HashSet<string> columns, string schema,
            int sqlTimeout, BulkOperations ext, List<string> concatTrans, string databaseIdentifier, List<SqlParameter> sqlParams, int transactionCount)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _columns = columns;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _customColumnMappings = new Dictionary<string, string>();
            _parameters = new List<SqlParameter>();
            _concatTrans = concatTrans;
            _databaseIdentifier = databaseIdentifier;
            _sqlParams = sqlParams;
            _transactionCount = transactionCount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public UpsertQueryReady<T> Insert()
        {
            return new UpsertQueryReady<T>(_singleEntity, _tableName, _schema, _columns, _customColumnMappings,
                _sqlTimeout, _ext, _concatTrans, _databaseIdentifier, _sqlParams, _transactionCount);
        }

        /// <summary>
        /// By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this method to set up a custom mapping.  
        /// </summary>
        /// <param name="source">
        /// The object member that has a different name in SQL table. 
        /// </param>
        /// <param name="destination">
        /// The actual name of column as represented in SQL table. 
        /// </param>
        /// <returns></returns>
        public UpsertQueryAddColumnList<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(source);
            _customColumnMappings.Add(propertyName, destination);
            return this;
        }
    }
}

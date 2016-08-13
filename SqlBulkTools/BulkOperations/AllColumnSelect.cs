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
    public class AllColumnSelect<T> : AbstractColumnSelect<T>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        public AllColumnSelect(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema,
            int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions,
            BulkOperations ext) :
            base(list, tableName, columns, schema, sqlTimeout, bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext)
        {

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
        public AllColumnSelect<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = _helper.GetPropertyName(source);
            _customColumnMappings.Add(propertyName, destination);
            return this;
        }

        /// <summary>
        /// Disables non-clustered index. You can select One to Many non-clustered indexes. This option should be considered on 
        /// a case-by-case basis. Understand the consequences before using this option.  
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public AllColumnSelect<T> AddTmpDisableNonClusteredIndex(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _disableIndexList.Add(indexName);

            return this;
        }

        /// <summary>
        /// Disables all Non-Clustered indexes on the table before the transaction and rebuilds after the 
        /// transaction. This option should be considered on a case-by-case basis. Understand the 
        /// consequences before using this option.  
        /// </summary>
        /// <returns></returns>
        public AllColumnSelect<T> TmpDisableAllNonClusteredIndexes()
        {
            _disableAllIndexes = true;
            return this;
        }

        /// <summary>
        /// Remove a column that you want to be excluded. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public AllColumnSelect<T> RemoveColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            if (_columns.Contains(propertyName))
                _columns.Remove(propertyName);

            else           
                throw new SqlBulkToolsException("Could not remove the column with name " 
                    + columnName +  
                    ". This could be because it's not a value or string type and therefore not included.");

            return this;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class AbstractColumnSelect<T>
    {
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        // ReSharper disable InconsistentNaming
        internal readonly BulkOperationsHelper _helper;
        protected IEnumerable<T> _list;
        protected string _tableName;
        protected string _schema;
        protected int _sqlTimeout;
        protected int _bulkCopyTimeout;
        protected bool _bulkCopyEnableStreaming;
        protected int? _bulkCopyNotifyAfter;
        protected int? _bulkCopyBatchSize;
        protected BulkOperations _ext;
        protected Dictionary<string, string> _customColumnMappings { get; }        
        protected HashSet<string> _columns;
        protected bool _disableAllIndexes;
        protected HashSet<string> _disableIndexList;
        protected SqlBulkCopyOptions _sqlBulkCopyOptions;
        #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member   

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
        protected AbstractColumnSelect(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema,
            int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions,
            BulkOperations ext)
        {
            _helper = new BulkOperationsHelper();
            _disableAllIndexes = false;
            _disableIndexList = new HashSet<string>();
            _customColumnMappings = new Dictionary<string, string>();
            _list = list;
            _tableName = tableName;
            _columns = columns;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _sqlBulkCopyOptions = sqlBulkCopyOptions;
            _ext = ext;
        }

        /// <summary>
        /// A bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected. 
        /// Notes: (1) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table. 
        /// </summary>
        /// <returns></returns>
        public BulkInsert<T> BulkInsert()
        {
            return new BulkInsert<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes,
                _customColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter,
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
        /// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least 
        /// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn) 
        /// will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> BulkInsertOrUpdate()
        {
            return new BulkInsertOrUpdate<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes,
                _customColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter,
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn 
        /// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkUpdate<T> BulkUpdate()
        {
            return new BulkUpdate<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes,
                _customColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter,
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK) Notes: 
        /// (1) BulkUpdate requires at least one MatchTargetOn property to be configured.
        /// </summary>
        /// <returns></returns>
        public BulkDelete<T> BulkDelete()
        {
            return new BulkDelete<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes, _customColumnMappings,
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class AbstractOperation<T> : ITransaction
    {
        internal readonly BulkOperationsHelper _helper;
        protected ColumnDirection _outputIdentity;
        protected BulkOperations _ext;
        protected string _identityColumn;
        protected Dictionary<int, T> _outputIdentityDic;
        protected bool _disableAllIndexes;
        protected int _sqlTimeout;
        protected HashSet<string> _columns;
        protected int? _bulkCopyBatchSize;
        protected int? _bulkCopyNotifyAfter;
        protected HashSet<string> _disableIndexList;
        protected bool _bulkCopyEnableStreaming;
        protected int _bulkCopyTimeout;
        protected string _schema;
        protected string _tableName;
        protected Dictionary<string, string> _customColumnMappings;

        protected AbstractOperation()
        {
            _helper = new BulkOperationsHelper();
            _outputIdentityDic = new Dictionary<int, T>();
            _outputIdentity = ColumnDirection.Input;
        } 

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <exception cref="InvalidOperationException"></exception>
        protected void SetIdentity(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new InvalidOperationException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;

            else
            {
                throw new InvalidOperationException("Can't have more than one identity column");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void CommitTransaction(string connectionName = null, SqlCredential credentials = null, SqlConnection connection = null)
        {
            throw new NotImplementedException();
        }

        public Task CommitTransactionAsync(string connectionName = null, SqlCredential credentials = null,
            SqlConnection connection = null)
        {
            throw new NotImplementedException();
        }
    }
}

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
    public abstract class AbstractOperation<T>
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
        protected IEnumerable<T> _list;
        protected List<string> _matchTargetOn;

        /// <summary>
        /// 
        /// </summary>
        protected AbstractOperation()
        {
            _helper = new BulkOperationsHelper();
            _outputIdentityDic = new Dictionary<int, T>();
            _outputIdentity = ColumnDirection.Input;
            _matchTargetOn = new List<string>();
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
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        protected void SetIdentity(Expression<Func<T, object>> columnName, ColumnDirection outputIdentity)
        {
            _outputIdentity = outputIdentity;
            SetIdentity(columnName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        protected void MatchTargetCheck()
        {
            if (_matchTargetOn.Count == 0)
            {
                throw new InvalidOperationException("MatchTargetOn list is empty when it's required for this operation. This is usually the primary key of your table but can also be more than one column depending on your business rules.");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        protected void IndexCheck()
        {
            if (_disableAllIndexes && (_disableIndexList != null && _disableIndexList.Any()))
            {
                throw new InvalidOperationException("Invalid setup. If \'TmpDisableAllNonClusteredIndexes\' is invoked, you can not use the \'AddTmpDisableNonClusteredIndex\' method.");
            }
        }
    }
}

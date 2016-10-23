using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class AbstractOperation<T>
    {
        #pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        // ReSharper disable InconsistentNaming
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
        protected SqlBulkCopyOptions _sqlBulkCopyOptions;
        protected IEnumerable<SqlRowsCopiedEventHandler> _bulkCopyDelegates;
        protected List<Condition> _updatePredicates;
        protected List<Condition> _deletePredicates;
        protected List<SqlParameter> _parameters; 
        #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="disableIndexList"></param>
        /// <param name="disableAllIndexes"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        /// <param name="bulkCopyDelegates"></param>
        protected AbstractOperation(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            HashSet<string> disableIndexList, bool disableAllIndexes,
            Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout,
            bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext, 
            IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _disableIndexList = disableIndexList;
            _disableAllIndexes = disableAllIndexes;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _sqlBulkCopyOptions = sqlBulkCopyOptions;
            _ext = ext;
            _identityColumn = null;
            _helper = new BulkOperationsHelper();
            _outputIdentityDic = new Dictionary<int, T>();
            _outputIdentity = ColumnDirection.Input;
            _matchTargetOn = new List<string>();
            _bulkCopyDelegates = bulkCopyDelegates;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <exception cref="SqlBulkToolsException"></exception>
        
        protected void SetIdentity(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;

            else
            {
                throw new SqlBulkToolsException("Can't have more than one identity column");
            }
        }

        public static TParameter GetParameterValue<TParameter>(Expression<Func<TParameter>> parameterToCheck)
        {
            TParameter parameterValue = (TParameter)parameterToCheck.Compile().Invoke();

            return parameterValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="predicateType"></param>
        protected void AddPredicate(Expression<Func<T, bool>> predicate, PredicateType predicateType)
        {
            string leftName;
            string value;
            Condition condition;

            BinaryExpression binaryBody = predicate.Body as BinaryExpression;

            if (binaryBody == null && (predicate.Body.Type == typeof (bool) || predicate.Body.Type == typeof (bool?)))
            {
                throw new SqlBulkToolsException($"Expression not supported for {GetPredicateMethodName(predicateType)}. For " +
                                                $"comparing boolean values, use the fully qualified syntax e.g. 'condition == true'");
            }

            if (binaryBody == null)
                throw new SqlBulkToolsException($"Expression not supported for {GetPredicateMethodName(predicateType)}.");

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
                                Value = value
                            };

                            DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);
                            SqlParameter param = new SqlParameter($"@{leftName}", sqlType);
                            param.Value = condition.Value;
                            _parameters.Add(param);
                        }
                        else
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.NotEqual,
                                LeftName = leftName,
                                Value = "NULL"
                            };
                        }

                        if (predicateType == PredicateType.Update)
                            _updatePredicates.Add(condition);

                        else if (predicateType == PredicateType.Delete)
                            _deletePredicates.Add(condition);


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
                                Value = value
                            };

                            DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);
                            SqlParameter param = new SqlParameter($"@{leftName}", sqlType);
                            param.Value = condition.Value;
                            _parameters.Add(param);
                        }
                        else
                        {
                            condition = new Condition()
                            {
                                Expression = ExpressionType.Equal,
                                LeftName = leftName,
                                Value = "NULL"
                            };
                        }

                        if (predicateType == PredicateType.Update)
                            _updatePredicates.Add(condition);

                        else if (predicateType == PredicateType.Delete)
                            _deletePredicates.Add(condition);

                        break;
                    }
                case ExpressionType.LessThan:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThan, predicateType);
                        break;
                    }
                case ExpressionType.LessThanOrEqual:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.LessThanOrEqual, predicateType);
                        break;
                    }
                case ExpressionType.GreaterThan:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThan, predicateType);
                        break;
                    }
                case ExpressionType.GreaterThanOrEqual:
                    {
                        leftName = ((MemberExpression)binaryBody.Left).Member.Name;
                        value = Expression.Lambda(binaryBody.Right).Compile().DynamicInvoke()?.ToString();
                        BuildCondition(leftName, value, binaryBody.Right.Type, ExpressionType.GreaterThanOrEqual, predicateType);
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
        protected string GetPredicateMethodName(PredicateType predicateType)
        {
            return predicateType == PredicateType.Update
                ? "UpdateWhen(...)"
                : predicateType == PredicateType.Delete ? 
                "DeleteWhen(...)" : string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="leftName"></param>
        /// <param name="value"></param>
        /// <param name="valueType"></param>
        /// <param name="expressionType"></param>
        /// <param name="predicateType"></param>
        protected void BuildCondition(string leftName, string value, Type valueType, ExpressionType expressionType, PredicateType predicateType)
        {

            Condition condition = null;
            if (predicateType == PredicateType.Update)
            {
                condition = new Condition()
                {
                    Expression = expressionType,
                    LeftName = leftName,
                    ValueType = valueType,
                    Value = value
                };

                _updatePredicates.Add(condition);
            }


            else if (predicateType == PredicateType.Delete)
            {
                condition = new Condition()
                {
                    Expression = expressionType,
                    LeftName = leftName,
                    ValueType = valueType,
                    Value = value
                };

                _deletePredicates.Add(condition);
            }

            if (condition != null)
            {
                DbType sqlType = SqlTypeMap.GetSqlTypeFromNetType(condition.ValueType);
                SqlParameter param = new SqlParameter($"@{leftName}", sqlType);
                param.Value = condition.Value;
                _parameters.Add(param);
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
        /// <exception cref="SqlBulkToolsException"></exception>
        protected void MatchTargetCheck()
        {
            if (_matchTargetOn.Count == 0)
            {
                throw new SqlBulkToolsException("MatchTargetOn list is empty when it's required for this operation. " +
                                                    "This is usually the primary key of your table but can also be more than one " +
                                                    "column depending on your business rules.");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="SqlBulkToolsException"></exception>
        protected void IndexCheck()
        {
            if (_disableAllIndexes && (_disableIndexList != null && _disableIndexList.Any()))
            {
                throw new SqlBulkToolsException("Invalid setup. If \'TmpDisableAllNonClusteredIndexes\' is invoked, you can not use " +
                                                    "the \'AddTmpDisableNonClusteredIndex\' method.");
            }
        }
    }
}

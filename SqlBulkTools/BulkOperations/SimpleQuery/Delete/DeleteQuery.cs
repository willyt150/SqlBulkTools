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
    public class DeleteQuery<T>
    {
        private readonly string _tableName;
        private readonly string _schema;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private readonly List<Condition> _whereConditions;
        private readonly List<SqlParameter> _parameters;
        private int _conditionSortOrder;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="ext"></param>
        public DeleteQuery(string tableName, string schema, int sqlTimeout, BulkOperations ext)
        {
            _tableName = tableName;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _whereConditions = new List<Condition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
        }

        /// <summary>
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public DeleteQueryReady<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters, 
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new DeleteQueryReady<T>(_tableName, _schema, _sqlTimeout, _ext, _conditionSortOrder, _whereConditions, _parameters);
        }

    }
}

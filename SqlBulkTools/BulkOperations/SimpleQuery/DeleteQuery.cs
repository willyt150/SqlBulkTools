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
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private readonly List<Condition> _whereConditions;
        private readonly List<Condition> _andConditions;
        private readonly List<Condition> _orConditions;
        private readonly List<SqlParameter> _parameters;
        private int _conditionSortOrder;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="ext"></param>
        public DeleteQuery(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings, 
            int sqlTimeout, BulkOperations ext)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _whereConditions = new List<Condition>();
            _andConditions = new List<Condition>();
            _orConditions = new List<Condition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;

        }

        /// <summary>
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public DeleteQueryWhere<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters, 
                _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new DeleteQueryWhere<T>(_singleEntity, _tableName, _schema, _columns, _customColumnMappings, 
                _sqlTimeout, _ext, _conditionSortOrder, _whereConditions, _parameters);
        }

    }
}

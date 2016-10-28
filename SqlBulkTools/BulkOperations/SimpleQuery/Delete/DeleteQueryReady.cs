using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryReady<T> : ITransaction
    {
        private readonly string _tableName;
        private readonly string _schema;
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
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="ext"></param>
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="parameters"></param>
        public DeleteQueryReady(string tableName, string schema,
            int sqlTimeout, BulkOperations ext, int conditionSortOrder, List<Condition> whereConditions, List<SqlParameter> parameters)
        {
            _tableName = tableName;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _whereConditions = whereConditions;
            _andConditions = new List<Condition>();
            _orConditions = new List<Condition>();
            _conditionSortOrder = conditionSortOrder;
            _parameters = parameters;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public DeleteQueryReady<T> And(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _parameters,
                _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public DeleteQueryReady<T> Or(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Or, _orConditions, _parameters,
                _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection)
        {
            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;

            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);

            string comm = $"DELETE FROM {fullQualifiedTableName} " +
                          $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery)}";

            command.CommandText = comm;

            if (_parameters.Count > 0)
            {
                command.Parameters.AddRange(_parameters.ToArray());
            }

            int affectedRows = command.ExecuteNonQuery();

            return affectedRows;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection)
        {
            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);

            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();

            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;
            command.CommandTimeout = _sqlTimeout;

            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);

            string comm = $"DELETE FROM {fullQualifiedTableName} " +
                          $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery)}";

            command.CommandText = comm;

            if (_parameters.Count > 0)
            {
                command.Parameters.AddRange(_parameters.ToArray());
            }

            int affectedRows = await command.ExecuteNonQueryAsync();

            return affectedRows;
        }
    }
}

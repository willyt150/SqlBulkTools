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
    public class UpdateQueryWhere<T> : ITransaction
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
        private readonly BulkOperationsHelper _helper;

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
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="parameters"></param>
        public UpdateQueryWhere(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings, 
            int sqlTimeout, BulkOperations ext, int conditionSortOrder, List<Condition> whereConditions, List<SqlParameter> parameters)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _conditionSortOrder = conditionSortOrder;
            _ext.SetBulkExt(this);
            _whereConditions = whereConditions;  
            _andConditions = new List<Condition>();
            _orConditions = new List<Condition>();
            _parameters = parameters;
            _helper = new BulkOperationsHelper();

        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public UpdateQueryWhere<T> And(Expression<Func<T, bool>> expression)
        {
            _helper.AddPredicate(expression, PredicateType.And, _andConditions, _parameters, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public UpdateQueryWhere<T> Or(Expression<Func<T, bool>> expression)
        {
            _helper.AddPredicate(expression, PredicateType.Or, _orConditions, _parameters, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (_singleEntity == null)
            {
                throw new SqlBulkToolsException("Nothing to update");
            }

            _helper.DoColumnMappings(_customColumnMappings, _whereConditions);
            _helper.DoColumnMappings(_customColumnMappings, _orConditions);
            _helper.DoColumnMappings(_customColumnMappings, _andConditions);

            _helper.AddSqlParamsForUpdateQuery(_parameters, _columns, _singleEntity);

            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {
                conn.Open();

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    try
                    {
                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        string fullQualifiedTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema,
                            _tableName);

                        string comm = $"UPDATE {fullQualifiedTableName} " +
                                      $"{_helper.BuildUpdateSet(_columns, fullQualifiedTableName)}" +
                                      $"{_helper.BuildPredicateQuery(concatenatedQuery)}";

                        command.CommandText = comm;

                        if (_parameters.Count > 0)
                        {
                            command.Parameters.AddRange(_parameters.ToArray());
                        }

                        command.ExecuteNonQuery();
                        transaction.Commit();
                    }

                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }

                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        async Task ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {

        }
    }
}

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
    public class DeleteQueryWhere<T> : ITransaction
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
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="parameters"></param>
        public DeleteQueryWhere(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings,
            int sqlTimeout, BulkOperations ext, int conditionSortOrder, List<Condition> whereConditions, List<SqlParameter> parameters)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _ext.SetBulkExt(this);
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
        public DeleteQueryWhere<T> And(Expression<Func<T, bool>> expression)
        {
           
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public DeleteQueryWhere<T> Or(Expression<Func<T, bool>> expression)
        {
            
            return this;
        }

        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (_singleEntity == null)
            {
                throw new SqlBulkToolsException("Nothing to delete");
            }

            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _whereConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _orConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _andConditions);

            BulkOperationsHelper.AddSqlParamsForUpdateQuery(_parameters, _columns, _singleEntity);

            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            

            using (SqlConnection conn = BulkOperationsHelper.GetSqlConnection(connectionName, credentials, connection))
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

                        string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema,
                            _tableName);

                        string comm = $"DELETE FROM {fullQualifiedTableName} " +
                                      $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery)}";

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

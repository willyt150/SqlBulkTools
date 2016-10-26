using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class InsertQueryReady<T> : ITransaction
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private int _conditionSortOrder;
        private string _identityColumn;
        private List<string> _concatTrans;
        private string _databaseIdentifier;
        private List<SqlParameter> _sqlParams;
        private int _transactionCount;

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
        public InsertQueryReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings, 
            int sqlTimeout, BulkOperations ext, List<string> concatTrans, string databaseIdentifier, List<SqlParameter> sqlParams, int transactionCount)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _ext.SetBulkExt(this);
            _concatTrans = concatTrans;
            _databaseIdentifier = databaseIdentifier;
            _sqlParams = sqlParams;
            _transactionCount = transactionCount;
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public InsertQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;

            else
            {
                throw new SqlBulkToolsException("Can't have more than one identity column");
            }

            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <typeparam name="T2"></typeparam>
        /// <returns></returns>
        public InsertQueryObject<T2> ThenDoInsert<T2>(T2 entity)
        {
            _concatTrans.Add(GetQuery());
            return new InsertQueryObject<T2>(entity, _ext, _concatTrans, _databaseIdentifier, _sqlParams, ++_transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="smallCollection"></param>
        /// <typeparam name="T2"></typeparam>
        /// <returns></returns>
        public InsertCollectionQueryObject<T2> ThenDoInsert<T2>(IEnumerable<T2> smallCollection)
        {
            _concatTrans.Add(GetQuery());
            return new InsertCollectionQueryObject<T2>(smallCollection, _ext, _concatTrans, _databaseIdentifier, _sqlParams, ++_transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <typeparam name="T2"></typeparam>
        /// <returns></returns>
        public InsertQueryObject<T2> ThenDoUpdate<T2>(T2 entity)
        {
            _concatTrans.Add(GetQuery());
            return new InsertQueryObject<T2>(entity, _ext, _concatTrans, _databaseIdentifier, _sqlParams, ++_transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <typeparam name="T2"></typeparam>
        /// <returns></returns>
        public DeleteQueryObject<T2> ThenDoDelete<T2>(T2 entity)
        {
            _concatTrans.Add(GetQuery());
            return new DeleteQueryObject<T2>(_ext);
        } 

        private string GetQuery()
        {
            string comm = $"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, _databaseIdentifier)} " +
                          $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn, _transactionCount)} ";

            BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _transactionCount);

            return comm;
        }

        int ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);
            
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

                        StringBuilder sb = new StringBuilder();
                        _concatTrans.Add(GetQuery());                        
                        _concatTrans.ForEach(x => sb.Append(x));

                        sb.Replace(_databaseIdentifier, fullQualifiedTableName);
                         
                        command.CommandText = sb.ToString();

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }

                        affectedRows = command.ExecuteNonQuery();
                        transaction.Commit();

                        return affectedRows;
                    }

                    catch (SqlException e)
                    {
                        for (int i = 0; i < e.Errors.Count; i++)
                        {
                            // Error 8102 is identity error. 
                            if (e.Errors[i].Number == 544)
                            {
                                // Expensive but neccessary to inform user of an important configuration setup. 
                                throw new IdentityException(e.Errors[i].Message);
                            }
                        }

                        transaction.Rollback();
                        throw;
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

        async Task<int> ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

           // BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _whereConditions);
           // BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _orConditions);
           // BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _andConditions);

            BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn);

            //var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);


            using (SqlConnection conn = BulkOperationsHelper.GetSqlConnection(connectionName, credentials, connection))
            {
                await conn.OpenAsync();

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

                        //string comm = $"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                        //              $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)}";
                                              

                        //command.CommandText = comm;

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }

                        affectedRows = await command.ExecuteNonQueryAsync();
                        transaction.Commit();

                        return affectedRows;
                    }

                    catch (SqlException e)
                    {
                        for (int i = 0; i < e.Errors.Count; i++)
                        {
                            // Error 8102 is identity error. 
                            if (e.Errors[i].Number == 544)
                            {
                                // Expensive but neccessary to inform user of an important configuration setup. 
                                throw new IdentityException(e.Errors[i].Message);
                            }
                        }

                        transaction.Rollback();
                        throw;
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
    }
}

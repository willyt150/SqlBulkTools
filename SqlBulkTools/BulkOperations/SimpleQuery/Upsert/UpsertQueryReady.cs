using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpsertQueryReady<T> : ITransaction
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
        private List<SqlParameter> _sqlParams;
        private string _matchTargetOn;
        private ColumnDirection _outputIdentity;

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
        public UpsertQueryReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings, 
            int sqlTimeout, BulkOperations ext, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _ext.SetBulkExt(this);
            _sqlParams = sqlParams;
            _matchTargetOn = string.Empty;
            _outputIdentity = ColumnDirection.Input;
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public UpsertQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirection outputIdentity)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _outputIdentity = outputIdentity;

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
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public UpsertQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
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
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating 
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed 
        /// for matching composite relationships. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public UpsertQueryReady<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            _matchTargetOn = propertyName;

            return this;
        }

        int ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (_matchTargetOn == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);
            
            using (SqlConnection conn = BulkOperationsHelper.GetSqlConnection(connectionName, credentials, connection))
            {
                conn.Open();

                //using (SqlTransaction transaction = conn.BeginTransaction())
                //{                   
                    try
                    {
                        
                        //_ext.SetTransaction(transaction);

                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        //command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                        StringBuilder sb = new StringBuilder();

                        sb.Append($"UPDATE {fullQualifiedTableName} {BulkOperationsHelper.BuildUpdateSet(_columns, _identityColumn)} WHERE [{_matchTargetOn}] = @{_matchTargetOn} " +
                          $"IF (@@ROWCOUNT = 0) BEGIN " +
                          $"{ BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                          $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)} " +
                          $"END ");

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            sb.Append($"SET @{_identityColumn}=SCOPE_IDENTITY()");
                        }

                        BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity);

                        command.CommandText = sb.ToString();

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }

                        affectedRows = command.ExecuteNonQuery();

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            foreach (var x in _sqlParams)
                            {
                                if (x.Direction == ParameterDirection.Output 
                                    && x.ParameterName == $"@{_identityColumn}")
                                {
                                    PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                                    propertyInfo.SetValue(_singleEntity, x.Value);
                                    break;
                                }
                            }
                        }

                        //transaction.Commit();

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

                        //transaction.Rollback();
                        throw;
                    }

                    catch (Exception)
                    {
                        //transaction.Rollback();
                        throw;
                    }

                    finally
                    {
                        //conn.Close();
                    }
              //  }
            }
        }

        async Task<int> ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (_matchTargetOn == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

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

                        string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                        StringBuilder sb = new StringBuilder();

                        sb.Append($"UPDATE {fullQualifiedTableName} {BulkOperationsHelper.BuildUpdateSet(_columns, _identityColumn)} " +
                                  $"WHERE [{_matchTargetOn}] = @{_matchTargetOn} " +
                                  $"IF (@@ROWCOUNT = 0) BEGIN " +
                                  $"{ BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                                  $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)} " +
                                  $"END ");

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            sb.Append($"SET @{_identityColumn}=SCOPE_IDENTITY()");
                        }

                        BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity);

                        command.CommandText = sb.ToString();

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }

                        affectedRows = await command.ExecuteNonQueryAsync();

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            foreach (var x in _sqlParams)
                            {
                                if (x.Direction == ParameterDirection.Output
                                    && x.ParameterName == $"@{_identityColumn}")
                                {
                                    PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                                    propertyInfo.SetValue(_singleEntity, x.Value);
                                    break;
                                }
                            }
                        }

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

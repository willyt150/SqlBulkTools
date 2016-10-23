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
    public class BulkDelete<T> : AbstractOperation<T>, ITransaction
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="disableAllIndexes"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        /// <param name="disableIndexList"></param>
        /// <param name="bulkCopyDelegates"></param>
        public BulkDelete(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns, HashSet<string> disableIndexList, 
            bool disableAllIndexes,
            Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout,
            bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, 
            BulkOperations ext, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)            
            :
            base(list, tableName, schema, columns, disableIndexList, disableAllIndexes, customColumnMappings, sqlTimeout,
                bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext, bulkCopyDelegates)
        {
            _ext.SetBulkExt(this);
            _deletePredicates = new List<Condition>();
            _parameters = new List<SqlParameter>();
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating 
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed 
        /// for matching composite relationships. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            _matchTargetOn.Add(propertyName);
            return this;
        }

        /// <summary>
        /// Only delete records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples.  
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public BulkDelete<T> DeleteWhen(Expression<Func<T, bool>> predicate)
        {
            base.AddPredicate(predicate, PredicateType.Delete);

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            base.SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkDelete<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirection outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (!_list.Any())
            {
                return;
            }

            base.IndexCheck();
            base.MatchTargetCheck();

            DataTable dt = _helper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = _helper.ConvertListToDataTable(dt, _list, _columns, _outputIdentityDic);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns);
            _helper.DoColumnMappings(_customColumnMappings, _deletePredicates);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {
                conn.Open();
                var dtCols = _helper.GetDatabaseSchema(conn, _schema, _tableName);

                using (SqlTransaction transaction = conn.BeginTransaction())
                {

                    try
                    {
                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        //Creating temp table on database
                        command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                        command.ExecuteNonQuery();

                        _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming,
                            _bulkCopyBatchSize, _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates);

                        if (_disableIndexList != null && _disableIndexList.Any())
                        {
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, 
                                _schema, conn, _disableIndexList, _disableAllIndexes);
                            command.ExecuteNonQuery();
                        }

                        string comm = _helper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
                        OperationType.InsertOrUpdate, _identityColumn);

                        if (!string.IsNullOrWhiteSpace(comm))
                        {
                            command.CommandText = comm;
                            command.ExecuteNonQuery();
                        }

                        comm = "MERGE INTO " + _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName) + " WITH (HOLDLOCK) AS Target " +
                                      "USING " + Constants.TempTableName + " AS Source " +
                                      _helper.BuildJoinConditionsForUpdateOrInsert(_matchTargetOn.ToArray(),
                                      Constants.SourceAlias, Constants.TargetAlias) +
                                      "WHEN MATCHED " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), _deletePredicates, Constants.TargetAlias) +
                                      "THEN DELETE " +
                                      _helper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                                      OperationType.Delete) +
                                      "DROP TABLE " + Constants.TempTableName + ";";
                        command.CommandText = comm;

                        if (_parameters.Count > 0)
                        {
                            command.Parameters.AddRange(_parameters.ToArray());
                        }

                        command.ExecuteNonQuery();

                        if (_disableIndexList != null && _disableIndexList.Any())
                        {
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, 
                                _schema, conn, _disableIndexList);
                            command.ExecuteNonQuery();
                        }

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            _helper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.Delete, _list);
                        }

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        async Task ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (!_list.Any())
            {
                return;
            }
            base.IndexCheck();
            base.MatchTargetCheck();

            DataTable dt = _helper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = _helper.ConvertListToDataTable(dt, _list, _columns, _outputIdentityDic);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns);
            _helper.DoColumnMappings(_customColumnMappings, _deletePredicates);


            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {
                conn.Open();
                var dtCols = _helper.GetDatabaseSchema(conn, _schema, _tableName);

                using (SqlTransaction transaction = conn.BeginTransaction())
                {

                    try
                    {
                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        //Creating temp table on database
                        command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                        await command.ExecuteNonQueryAsync();

                        await _helper.InsertToTmpTableAsync(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize, 
                            _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates);

                        if (_disableIndexList != null && _disableIndexList.Any())
                        {
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName,
                                _schema, conn, _disableIndexList, _disableAllIndexes);
                            await command.ExecuteNonQueryAsync();
                        }

                        string comm = _helper.GetOutputCreateTableCmd(_outputIdentity, Constants.TempOutputTableName,
                        OperationType.InsertOrUpdate, _identityColumn);

                        if (!string.IsNullOrWhiteSpace(comm))
                        {
                            command.CommandText = comm;
                            command.ExecuteNonQuery();
                        }

                        // Updating destination table, and dropping temp table
                        comm = "MERGE INTO " + _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName) + " WITH (HOLDLOCK) AS Target " +
                                      "USING " + Constants.TempTableName + " AS Source " +
                                      _helper.BuildJoinConditionsForUpdateOrInsert(_matchTargetOn.ToArray(),
                                      Constants.SourceAlias, Constants.TargetAlias) +
                                      "WHEN MATCHED " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), _deletePredicates, Constants.TargetAlias) + 
                                      "THEN DELETE " +
                                      _helper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                                      OperationType.Delete) +
                                      "DROP TABLE " + Constants.TempTableName + ";";
                        command.CommandText = comm;

                        if (_parameters.Count > 0)
                        {
                            command.Parameters.AddRange(_parameters.ToArray());
                        }

                        await command.ExecuteNonQueryAsync();

                        if (_disableIndexList != null && _disableIndexList.Any())
                        {
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, 
                                _schema, conn, _disableIndexList);
                            await command.ExecuteNonQueryAsync();
                        }

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            await
                                _helper.LoadFromTmpOutputTableAsync(command, _identityColumn, _outputIdentityDic,
                                OperationType.Delete, _list);
                        }

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
    }
}

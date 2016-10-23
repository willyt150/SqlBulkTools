using System;
using System.Collections;
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
    public class BulkInsertOrUpdate<T> : AbstractOperation<T>, ITransaction
    {
        private bool _deleteWhenNotMatchedFlag;

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
        public BulkInsertOrUpdate(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns, HashSet<string> disableIndexList, bool disableAllIndexes,
            Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates) :

            base(list, tableName, schema, columns, disableIndexList, disableAllIndexes, customColumnMappings, sqlTimeout,
            bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext, bulkCopyDelegates)
        {
            _ext.SetBulkExt(this);
            _deleteWhenNotMatchedFlag = false;
            _updatePredicates = new List<Condition>();
            _deletePredicates = new List<Condition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating 
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed 
        /// for matching composite relationships. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new NullReferenceException("MatchTargetOn column name can't be null.");

            _matchTargetOn.Add(propertyName);

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            SetIdentity(columnName);
            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="outputIdentity"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirection outputIdentity)
        {
            base.SetIdentity(columnName, outputIdentity);
            return this;
        }

        /// <summary>
        /// Only delete records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples. Notes: (1) DeleteWhenNotMatched must be set to true. 
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> DeleteWhen(Expression<Func<T, bool>> predicate)
        {
            _helper.AddPredicate(predicate, PredicateType.Delete, _deletePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// Only update records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
        /// See help docs for examples.  
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public BulkInsertOrUpdate<T> UpdateWhen(Expression<Func<T, bool>> predicate)
        {
            _helper.AddPredicate(predicate, PredicateType.Update, _updatePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            return this;
        }

        /// <summary>
        /// If a target record can't be matched to a source record, it's deleted. Notes: (1) This is false by default. (2) Use at your own risk.
        /// </summary>
        /// <param name="flag"></param>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> DeleteWhenNotMatched(bool flag)
        {
            _deleteWhenNotMatchedFlag = flag;
            return this;
        }

        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (!_list.Any())
            {
                return;
            }

            if (!_deleteWhenNotMatchedFlag && _deletePredicates.Count > 0)
                throw new SqlBulkToolsException($"{_helper.GetPredicateMethodName(PredicateType.Delete)} only usable on BulkInsertOrUpdate " +
                                                $"method when 'DeleteWhenNotMatched' is set to true.");

            base.IndexCheck();
            base.MatchTargetCheck();

            DataTable dt = _helper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = _helper.ConvertListToDataTable(dt, _list, _columns, _outputIdentityDic);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            _helper.DoColumnMappings(_customColumnMappings, _deletePredicates);
            _helper.DoColumnMappings(_customColumnMappings, _updatePredicates);

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

                        _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                            _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates);

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

                        comm =
                            "MERGE INTO " + _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName) +
                            " WITH (HOLDLOCK) AS Target " +
                            "USING " + Constants.TempTableName + " AS Source " +
                            _helper.BuildJoinConditionsForUpdateOrInsert(_matchTargetOn.ToArray(),
                                Constants.SourceAlias, Constants.TargetAlias) +                            
                            "WHEN MATCHED " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias) + 
                            "THEN UPDATE " +
                            _helper.BuildUpdateSet(_columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn) +
                            "WHEN NOT MATCHED BY TARGET THEN " +
                            _helper.BuildInsertSet(_columns, Constants.SourceAlias, _identityColumn) +
                            (_deleteWhenNotMatchedFlag ? " WHEN NOT MATCHED BY SOURCE " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), 
                            _deletePredicates, Constants.TargetAlias) + 
                            "THEN DELETE " : " ") +
                            _helper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                                OperationType.InsertOrUpdate) + "; " +
                            "DROP TABLE " + Constants.TempTableName + ";";

                        command.CommandText = comm;

                        if (_parameters.Count > 0)
                        {
                            command.Parameters.AddRange(_parameters.ToArray());
                        }

                        command.ExecuteNonQuery();


                        if (_disableIndexList != null && _disableIndexList.Any())
                        {
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild,
                                _tableName, _schema, conn, _disableIndexList);
                            command.ExecuteNonQuery();
                        }

                        if (_outputIdentity == ColumnDirection.InputOutput)
                        {
                            _helper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list);
                        }

                        transaction.Commit();

                    }

                    catch (SqlException e)
                    {
                        for (int i = 0; i < e.Errors.Count; i++)
                        {
                            // Error 8102 is identity error. 
                            if (e.Errors[i].Number == 8102)
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
            _helper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);
            _helper.DoColumnMappings(_customColumnMappings, _deletePredicates);
            _helper.DoColumnMappings(_customColumnMappings, _updatePredicates);

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
                            command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, _schema,
                                conn, _disableIndexList, _disableAllIndexes);
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
                                      "WHEN MATCHED " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias) +
                                      "THEN UPDATE " +
                                      _helper.BuildUpdateSet(_columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn) +
                                      "WHEN NOT MATCHED BY TARGET THEN " +
                                      _helper.BuildInsertSet(_columns, Constants.SourceAlias, _identityColumn) +
                                      (_deleteWhenNotMatchedFlag ? " WHEN NOT MATCHED BY SOURCE " + _helper.BuildPredicateQuery(_matchTargetOn.ToArray(), 
                                      _deletePredicates, Constants.TargetAlias) +
                                      "THEN DELETE " : " ") +
                                       _helper.GetOutputIdentityCmd(_identityColumn, _outputIdentity, Constants.TempOutputTableName,
                                       OperationType.InsertOrUpdate) + "; " +
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
                                    OperationType.InsertOrUpdate, _list);

                        }

                        transaction.Commit();

                    }

                    catch (SqlException e)
                    {
                        for (int i = 0; i < e.Errors.Count; i++)
                        {
                            // Error 8102 is identity error. 
                            if (e.Errors[i].Number == 8102)
                            {
                                // Expensive call but neccessary to inform user of an important configuration setup. 
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

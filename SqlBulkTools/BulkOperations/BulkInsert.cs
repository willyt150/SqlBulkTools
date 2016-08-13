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
    public class BulkInsert<T> : AbstractOperation<T>, ITransaction
    {
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
        public BulkInsert(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            HashSet<string> disableIndexList, bool disableAllIndexes,
            Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext) : 

            base(list, tableName, schema, columns, disableIndexList, disableAllIndexes, customColumnMappings, sqlTimeout,
                bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext)
        {
            _ext.SetBulkExt(this);
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
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
        public BulkInsert<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirection outputIdentity)
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

            DataTable dt = _helper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = _helper.ConvertListToDataTable(dt, _list, _columns);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {

                conn.Open();
                DataTable dtCols = null;
                if (_outputIdentity == ColumnDirection.InputOutput)
                    dtCols = _helper.GetDatabaseSchema(conn, _schema, _tableName);

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, _sqlBulkCopyOptions, transaction))
                    {
                        try
                        {
                            bulkcopy.DestinationTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                            _helper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            SqlCommand command = conn.CreateCommand();
                            command.Connection = conn;
                            command.Transaction = transaction;

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, 
                                    _schema, conn, _disableIndexList, _disableAllIndexes);
                                command.ExecuteNonQuery();
                            }

                            // If InputOutput identity is selected, must use staging table.
                            if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                            {
                                command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                                command.ExecuteNonQuery();

                                _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions);

                                command.CommandText = _helper.GetInsertIntoStagingTableCmd(command, conn, _schema, _tableName,
                                    _columns, _identityColumn, _outputIdentity);
                                command.ExecuteNonQuery();

                                _helper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);

                            }
                            
                            else
                                bulkcopy.WriteToServer(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, 
                                    _schema, conn, _disableIndexList, _disableAllIndexes);
                                command.ExecuteNonQuery();
                            }

                            transaction.Commit();

                            bulkcopy.Close();
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

            DataTable dt = _helper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = _helper.ConvertListToDataTable(dt, _list, _columns);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {

                conn.Open();
                DataTable dtCols = null;
                if (_outputIdentity == ColumnDirection.InputOutput)
                    dtCols = _helper.GetDatabaseSchema(conn, _schema, _tableName);

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, _sqlBulkCopyOptions, transaction))
                    {
                        try
                        {
                            bulkcopy.DestinationTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                            _helper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            SqlCommand command = conn.CreateCommand();

                            command.Connection = conn;
                            command.Transaction = transaction;

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, 
                                    _schema, conn, _disableIndexList, _disableAllIndexes);
                                await command.ExecuteNonQueryAsync();
                            }

                            // If InputOutput identity is selected, must use staging table.
                            if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                            {
                                command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                                await command.ExecuteNonQueryAsync();

                                _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions);

                                command.CommandText = _helper.GetInsertIntoStagingTableCmd(command, conn, _schema, _tableName,
                                    _columns, _identityColumn, _outputIdentity);
                                await command.ExecuteNonQueryAsync();

                                await
                                    _helper.LoadFromTmpOutputTableAsync(command, _identityColumn, _outputIdentityDic,
                                        OperationType.Insert, _list);

                            }

                            else
                                await bulkcopy.WriteToServerAsync(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, _schema, 
                                    conn, _disableIndexList, _disableAllIndexes);
                                await command.ExecuteNonQueryAsync();
                            }
                            transaction.Commit();

                            bulkcopy.Close();
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
}

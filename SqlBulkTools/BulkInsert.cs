using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkInsert<T> : AbstractOperation<T>, ITransaction
    {
        
        private readonly SqlBulkCopyOptions _sqlBulkCopyOptions;

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
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        public BulkInsert(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns, HashSet<string> disableIndexList, bool disableAllIndexes, 
            Dictionary<string, string> customColumnMappings, int bulkCopyTimeout, bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _disableIndexList = disableIndexList;
            _disableAllIndexes = disableAllIndexes;
            _customColumnMappings = customColumnMappings;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _ext = ext;
            _sqlBulkCopyOptions = sqlBulkCopyOptions;
            _outputIdentity = ColumnDirection.Input;
            _identityColumn = null;
            _ext.SetBulkExt(this);
        }

        /// <summary>
        /// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
        /// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
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
        /// <exception cref="InvalidOperationException"></exception>
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
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, _disableIndexList, _disableAllIndexes);
                                command.ExecuteNonQuery();
                            }

                            // If InputOutput identity is selected, must use staging table.
                            if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                            {
                                command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                                command.ExecuteNonQuery();

                                _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions);

                                string fullTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema,
                                    _tableName);

                                string comm =
                                _helper.GetOutputCreateTableCmd(_outputIdentity, "#TmpOutput", OperationType.Insert) +
                                _helper.BuildInsertIntoSet(_columns, _identityColumn, fullTableName) + "OUTPUT INSERTED.Id INTO #TmpOutput(Id)" + " " + _helper.BuildSelectSet(_columns, Constants.SourceAlias, _identityColumn) + " FROM " + Constants.TempTableName + " AS Source; " +
                                "DROP TABLE " + Constants.TempTableName + ";";
                                command.CommandText = comm;
                                command.ExecuteNonQuery();

                                command.CommandText = "SELECT " + _identityColumn + " FROM #TmpOutput;";

                                using (SqlDataReader reader = command.ExecuteReader())
                                {
                                    var items = _list.ToList();
                                    int counter = 0;

                                    while (reader.Read())
                                    {
                                        items[counter].GetType().GetProperty(_identityColumn).SetValue(items[counter], reader[0], null);
                                        counter++;
                                    }
                                }

                                command.CommandText = "DROP TABLE " + "#TmpOutput" + ";";
                                command.ExecuteNonQuery();

                            }
                            
                            else
                                bulkcopy.WriteToServer(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, _disableIndexList, _disableAllIndexes);
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

            if (_disableAllIndexes && (_disableIndexList != null && _disableIndexList.Any()))
            {
                throw new InvalidOperationException("Invalid setup. If \'TmpDisableAllNonClusteredIndexes\' is invoked, you can not use the \'AddTmpDisableNonClusteredIndex\' method.");
            }

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
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, _disableIndexList, _disableAllIndexes);
                                await command.ExecuteNonQueryAsync();
                            }

                            // If InputOutput identity is selected, must use staging table.
                            if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                            {
                                command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                                await command.ExecuteNonQueryAsync();

                                _helper.InsertToTmpTable(conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions);

                                string fullTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema,
                                    _tableName);

                                string comm =
                                _helper.GetOutputCreateTableCmd(_outputIdentity, "#TmpOutput", OperationType.Insert) +
                                _helper.BuildInsertIntoSet(_columns, _identityColumn, fullTableName) + "OUTPUT INSERTED.Id INTO #TmpOutput(Id)" + " " + _helper.BuildSelectSet(_columns, Constants.SourceAlias, _identityColumn) + " FROM " + Constants.TempTableName + " AS Source; " +
                                "DROP TABLE " + Constants.TempTableName + ";";
                                command.CommandText = comm;
                                await command.ExecuteNonQueryAsync();

                                command.CommandText = "SELECT " + _identityColumn + " FROM #TmpOutput;";

                                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                                {
                                    var items = _list.ToList();
                                    int counter = 0;

                                    while (reader.Read())
                                    {
                                        items[counter].GetType().GetProperty(_identityColumn).SetValue(items[counter], reader[0], null);
                                        counter++;
                                    }
                                }

                                command.CommandText = "DROP TABLE " + "#TmpOutput" + ";";
                                await command.ExecuteNonQueryAsync();

                            }

                            else
                                await bulkcopy.WriteToServerAsync(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, _disableIndexList, _disableAllIndexes);
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

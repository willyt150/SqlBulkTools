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
        /// <param name="bulkCopyDelegates"></param>
        public BulkInsert(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns,
            HashSet<string> disableIndexList, bool disableAllIndexes,
            Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates) :

            base(list, tableName, schema, columns, disableIndexList, disableAllIndexes, customColumnMappings, sqlTimeout,
                bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext, bulkCopyDelegates)
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection)
        {
            int affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            base.IndexCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(dt, _list, _columns);

            // Must be after ToDataTable is called. 
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State == ConnectionState.Closed)
                connection.Open();


            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirection.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(connection, _schema, _tableName);

            //Bulk insert into temp table
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection, _sqlBulkCopyOptions, null))
            {
                bulkcopy.DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _bulkCopyDelegates);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;

                if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(IndexOperation.Disable, _tableName,
                        _schema, connection, _disableIndexList, _disableAllIndexes);
                    command.ExecuteNonQuery();
                }

                // If InputOutput identity is selected, must use staging table.
                if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                {
                    command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                    command.ExecuteNonQuery();

                    BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                        _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates);

                    command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(command, connection, _schema, _tableName,
                        _columns, _identityColumn, _outputIdentity);
                    command.ExecuteNonQuery();

                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);

                }

                else
                    bulkcopy.WriteToServer(dt);

                if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName,
                        _schema, connection, _disableIndexList, _disableAllIndexes);
                    command.ExecuteNonQuery();
                }

                bulkcopy.Close();

                affectedRows = dt.Rows.Count;
                return affectedRows;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection)
        {

            int affectedRows = 0;

            if (!_list.Any())
            {
                return affectedRows;
            }

            base.IndexCheck();

            DataTable dt = BulkOperationsHelper.CreateDataTable<T>(_columns, _customColumnMappings, _matchTargetOn, _outputIdentity);
            dt = BulkOperationsHelper.ConvertListToDataTable(dt, _list, _columns);

            // Must be after ToDataTable is called. 
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns, _matchTargetOn);

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();


            DataTable dtCols = null;
            if (_outputIdentity == ColumnDirection.InputOutput)
                dtCols = BulkOperationsHelper.GetDatabaseSchema(connection, _schema, _tableName);

            //Bulk insert into temp table
            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(connection.ConnectionString, _sqlBulkCopyOptions))
            {
                bulkcopy.DestinationTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema, _tableName);
                BulkOperationsHelper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                BulkOperationsHelper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                    _bulkCopyNotifyAfter, _bulkCopyTimeout, _bulkCopyDelegates);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;

                if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(IndexOperation.Disable, _tableName,
                        _schema, connection, _disableIndexList, _disableAllIndexes);
                    await command.ExecuteNonQueryAsync();
                }

                // If InputOutput identity is selected, must use staging table.
                if (_outputIdentity == ColumnDirection.InputOutput && dtCols != null)
                {
                    command.CommandText = BulkOperationsHelper.BuildCreateTempTable(_columns, dtCols, _outputIdentity);
                    await command.ExecuteNonQueryAsync();

                    BulkOperationsHelper.InsertToTmpTable(connection, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                        _bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates);

                    command.CommandText = BulkOperationsHelper.GetInsertIntoStagingTableCmd(command, connection, _schema, _tableName,
                        _columns, _identityColumn, _outputIdentity);
                    await command.ExecuteNonQueryAsync();

                    BulkOperationsHelper.LoadFromTmpOutputTable(command, _identityColumn, _outputIdentityDic, OperationType.Insert, _list);

                }

                else
                    await bulkcopy.WriteToServerAsync(dt);

                if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                {
                    command.CommandText = BulkOperationsHelper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName,
                        _schema, connection, _disableIndexList, _disableAllIndexes);
                    await command.ExecuteNonQueryAsync();
                }

                bulkcopy.Close();

                affectedRows = dt.Rows.Count;
                return affectedRows;

            }
        }
    }
}

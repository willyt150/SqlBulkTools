﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    public class BulkDelete<T> : ITransaction
    {
        private readonly List<T> _list; 
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private List<string> DeleteOnList { get; set; }
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly SqlBulkToolsHelpers _helper;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;
        private readonly SqlBulkTools _ext;

        public BulkDelete(List<T> list, string tableName, string schema, HashSet<string> columns, string sourceAlias, 
            string targetAlias, Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout, 
            bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkTools ext)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            DeleteOnList = new List<string>();
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _helper = new SqlBulkToolsHelpers();
            _ext = ext;           
            _ext.SetBulkExt(this);
        }

        /// <summary>
        /// Column(s) needing to match to delete record. At least one column is required. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> DeleteOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            DeleteOnList.Add(propertyName);
            return this;
        }

        public void CommitTransaction(string connectionString, SqlCredential credentials )
        {
            if (_list.Count == 0)
            {
                throw new ArgumentException("The collection provided does not contain any objects.");
            }

            if (DeleteOnList.Count == 0)
            {
                throw new InvalidOperationException("BulkDelete requires at least one DeleteOn column. This is usually the primary key of the table.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns);

            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings[connectionString].ConnectionString, credentials))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var dtCols = _helper.GetSchema(conn, _schema, _tableName);

                        //Creating temp table on database
                        command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols);
                        command.ExecuteNonQuery();

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            bulkcopy.DestinationTableName = "#TmpTable";

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = _sqlTimeout;
                        string comm = "MERGE INTO " + _tableName + " AS Target " +
                                      "USING #TmpTable AS Source " +
                                      _helper.BuildJoinConditionsForUpdateOrInsert(DeleteOnList.ToArray(), 
                                      _sourceAlias, _targetAlias) +
                                      "WHEN MATCHED THEN DELETE; " +
                                      "DROP TABLE #TmpTable;";
                        command.CommandText = comm;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                        command.CommandText = "ROLLBACK Transaction;";
                        command.ExecuteNonQuery();
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
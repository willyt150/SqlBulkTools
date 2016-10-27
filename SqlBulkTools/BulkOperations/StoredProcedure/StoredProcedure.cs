using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class StoredProcedure : ITransaction
    {
        private readonly IEnumerable<SqlParameter> _sqlParameters;
        private readonly int _timeout;
        private readonly string _storedProcedureName;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="storedProcedureName"></param>
        /// <param name="sqlParameters"></param>
        /// <param name="timeout"></param>
        public StoredProcedure(BulkOperations ext, string storedProcedureName, IEnumerable<SqlParameter> sqlParameters = null, int timeout = 600)
        {
            _sqlParameters = sqlParameters;
            _storedProcedureName = storedProcedureName;
            _timeout = timeout;
            _ext = ext;
            _ext.SetBulkExt(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int CommitTransaction(string connectionName = null, SqlCredential credentials = null, SqlConnection connection = null)
        {
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
                        command.CommandTimeout = _timeout;
                        command.CommandText = _storedProcedureName;

                        if (_sqlParameters.Any())
                        {
                            command.Parameters.AddRange(_sqlParameters.ToArray());
                        }

                        int affectedRows = command.ExecuteNonQuery();

                        transaction.Commit();
                        return affectedRows;
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
        /// <exception cref="NotImplementedException"></exception>
        public async Task<int> CommitTransactionAsync(string connectionName = null, SqlCredential credentials = null,
            SqlConnection connection = null)
        {
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
                        command.CommandTimeout = _timeout;
                        command.CommandText = _storedProcedureName;

                        if (_sqlParameters.Any())
                        {
                            command.Parameters.AddRange(_sqlParameters.ToArray());
                        }

                        int affectedRows = await command.ExecuteNonQueryAsync();

                        transaction.Commit();
                        return affectedRows;
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

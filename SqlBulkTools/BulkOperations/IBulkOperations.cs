using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public interface IBulkOperations
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        int CommitTransaction(SqlConnection connection);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        Task<int> CommitTransactionAsync(SqlConnection connection);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        int CommitTransaction(string connectionName, SqlCredential credentials = null);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <returns></returns>
        Task<int> CommitTransactionAsync(string connectionName, SqlCredential credentials = null);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        BulkForCollection<T> Setup<T>(Func<Setup<T>, BulkForCollection<T>> list);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        Setup<T> Setup<T>();
    }
}
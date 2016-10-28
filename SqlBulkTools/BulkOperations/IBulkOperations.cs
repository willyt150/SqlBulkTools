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
        int Commit(SqlConnection connection);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        Task<int> CommitAsync(SqlConnection connection);
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
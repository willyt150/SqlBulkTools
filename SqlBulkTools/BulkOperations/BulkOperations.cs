using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations : IBulkOperations
    {
        private ITransaction _transaction; 

        internal void SetBulkExt(ITransaction transaction)
        {
            _transaction = transaction;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. Notes: (1) The connectionName parameter is a name that you provide to 
        /// uniquely identify a connection string so that it can be retrieved at run time.
        /// </summary>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        public void CommitTransaction(string connectionName, SqlCredential credentials = null)
        {
            if (connectionName == null)
                throw new ArgumentNullException(nameof(connectionName) + " not given");

            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
                throw new SqlBulkToolsException("Connection name \'" + connectionName + "\' not found. A valid connection name is required for this operation.");

            if (_transaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");
            

            _transaction.CommitTransaction(connectionName, credentials);
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. Notes: (1) The connectionName parameter is a name that you provide to 
        /// uniquely identify a connection string so that it can be retrieved at run time.
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        public async Task CommitTransactionAsync(string connectionName, SqlCredential credentials = null)
        {
            if (connectionName == null)
                throw new ArgumentNullException(nameof(connectionName) + " not given");

            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
                throw new SqlBulkToolsException("Connection name \'" + connectionName + "\' not found. A valid connection name is required for this operation.");

            if (_transaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            await _transaction.CommitTransactionAsync(connectionName, credentials);
        }


        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. 
        /// </summary>
        /// <param name="connection"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        public void CommitTransaction(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (_transaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            _transaction.CommitTransaction(connection : connection);

        }


        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        public async Task CommitTransactionAsync(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (_transaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            await _transaction.CommitTransactionAsync(connection : connection);
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T">The type of collection to be used.</typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        [Obsolete("This method is deprecated and will be removed from a future release, please use the more user-friendly Setup<T>() instead.")]
        public CollectionSelect<T> Setup<T>(Func<Setup<T>, CollectionSelect<T>> list)
        {
            CollectionSelect<T> tableSelect = list(new Setup<T>(this));
            return tableSelect;
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Setup<T> Setup<T>()
        {
            return new Setup<T>(this);
        }


    }

}
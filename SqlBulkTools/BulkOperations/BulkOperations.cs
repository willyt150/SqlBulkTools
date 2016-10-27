using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Transactions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations : IBulkOperations
    {
        private ITransaction _sqlBulkToolsTransaction;
        private SqlTransaction _sqlTransaction;

        public BulkOperations()
        {
            //Transaction.Current.EnlistVolatile(this, EnlistmentOptions.None);
        }

        internal void SetBulkExt(ITransaction sqlBulkToolsTransaction)
        {
            _sqlBulkToolsTransaction = sqlBulkToolsTransaction;
        }

        internal void SetTransaction(SqlTransaction sqlTransaction)
        {
            _sqlTransaction = sqlTransaction;
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
        public int CommitTransaction(string connectionName, SqlCredential credentials = null)
        {
            
            if (connectionName == null)
                throw new ArgumentNullException(nameof(connectionName) + " not given");

            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
                throw new SqlBulkToolsException("Connection name \'" + connectionName + "\' not found. A valid connection name is required for this operation.");

            if (_sqlBulkToolsTransaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");
            

            return _sqlBulkToolsTransaction.CommitTransaction(connectionName, credentials);
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
        public async Task<int> CommitTransactionAsync(string connectionName, SqlCredential credentials = null)
        {
            if (connectionName == null)
                throw new ArgumentNullException(nameof(connectionName) + " not given");

            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
                throw new SqlBulkToolsException("Connection name \'" + connectionName + "\' not found. A valid connection name is required for this operation.");

            if (_sqlBulkToolsTransaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            return await _sqlBulkToolsTransaction.CommitTransactionAsync(connectionName, credentials);
        }


        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. 
        /// </summary>
        /// <param name="connection"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        public int CommitTransaction(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (_sqlBulkToolsTransaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            return _sqlBulkToolsTransaction.CommitTransaction(connection : connection);
        }


        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="SqlBulkToolsException"></exception>
        public async Task<int> CommitTransactionAsync(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            if (_sqlBulkToolsTransaction == null)
                throw new SqlBulkToolsException("No setup found. Use the Setup method to build a new setup then try again.");

            return await _sqlBulkToolsTransaction.CommitTransactionAsync(connection : connection);
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T">The type of collection to be used.</typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        [Obsolete("This method is deprecated and will be removed from a future release, please use the more user-friendly Setup<T>() instead.")]
        public BulkForCollection<T> Setup<T>(Func<Setup<T>, BulkForCollection<T>> list)
        {
            BulkForCollection<T> tableSelect = list(new Setup<T>(this));
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

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <returns></returns>
        public Setup Setup()
        {
            return new Setup(this);
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            
           // _sqlTransaction.Rollback();

            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }

    }

}
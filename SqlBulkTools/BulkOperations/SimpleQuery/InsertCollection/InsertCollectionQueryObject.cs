using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class InsertCollectionQueryObject<T>
    {
        private IEnumerable<T> _smallCollection;
        private readonly BulkOperations _ext;
        private List<string> _concatTrans;
        public string _databaseIdentifier;
        private List<SqlParameter> _sqlParams;
        private int _transactionCount;
        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="smallCollection"></param>
        /// <param name="ext"></param>
        /// <param name="transactionCount"></param>
        public InsertCollectionQueryObject(IEnumerable<T> smallCollection, BulkOperations ext, List<string> concatTrans, string databaseIdentifier, List<SqlParameter> sqlParams, int transactionCount)
        {
            _ext = ext;
            _smallCollection = smallCollection;
            _concatTrans = concatTrans;
            _databaseIdentifier = databaseIdentifier;
            _sqlParams = sqlParams;
            _transactionCount = transactionCount;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public InsertCollectionQueryTable<T> WithTable(string tableName)
        {        
            return new InsertCollectionQueryTable<T>(_smallCollection, tableName, _ext, _concatTrans, _databaseIdentifier, _sqlParams, _transactionCount);
        }
    }
}

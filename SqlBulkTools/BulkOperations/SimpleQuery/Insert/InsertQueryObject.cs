using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class InsertQueryObject<T>
    {
        private readonly T _singleEntity;
        private readonly BulkOperations _ext;
        private List<string> _concatTrans;
        public string _databaseIdentifier;
        private List<SqlParameter> _sqlParams;
        private int _transactionCount;
        private IEnumerable<T> _smallCollection;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="smallCollection"></param>
        /// <param name="ext"></param>
        /// <param name="transactionCount"></param>
        public InsertQueryObject(T singleEntity, BulkOperations ext, List<string> concatTrans, string databaseIdentifier, List<SqlParameter> sqlParams, int transactionCount)
        {
            _ext = ext;
            _singleEntity = singleEntity;
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
        public InsertQueryTable<T> WithTable(string tableName)
        {        
            return new InsertQueryTable<T>(_singleEntity, tableName, _ext, _sqlParams);
        }
    }
}

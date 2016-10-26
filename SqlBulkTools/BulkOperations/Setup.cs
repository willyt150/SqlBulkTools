using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Security.Principal;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class Setup
    {
        private readonly BulkOperations _ext; 
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        public Setup(BulkOperations ext)
        {
            _ext = ext;
        }

        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public BulkForCollection<T> ForCollection<T>(IEnumerable<T> list)
        {
            return new BulkForCollection<T>(list, _ext);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Setup<T>
    {
        private readonly BulkOperations _ext;
        private List<string> _concatTrans;
        private string _databaseIdentifier;
        private List<SqlParameter> _sqlParams;
        private int _transactionCount;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        public Setup(BulkOperations ext)
        {
            _ext = ext;
            _concatTrans = new List<string>();
            _sqlParams = new List<SqlParameter>();
            _transactionCount = 1;
            // When we commit transaction, this will be replaced with the actual database name. 
            _databaseIdentifier = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Use this option for simple updates or deletes where you are only dealing with a single table 
        /// and conditions are not complex. For anything more advanced, use a stored procedure.  
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public UpdateQueryObject<T> ForSimpleUpdateQuery(T entity)
        {
            return new UpdateQueryObject<T>(entity, _ext, _transactionCount, _concatTrans, _sqlParams);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="smallCollection"></param>
        /// <returns></returns>
        public UpdateCollectionQueryObject<T> ForSimpleUpdateQuery(DataTable smallCollection)
        {
            return new UpdateCollectionQueryObject<T>(smallCollection, _ext, _transactionCount, _concatTrans, _sqlParams);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public InsertQueryObject<T> ForSimpleInsertQuery(T entity)
        {
            return new InsertQueryObject<T>(entity, _ext, _concatTrans, _databaseIdentifier, _sqlParams, _transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public InsertCollectionQueryObject<T> ForSimpleInsertQuery(IEnumerable<T> smallCollection)
        {
            return new InsertCollectionQueryObject<T>(smallCollection, _ext, _concatTrans, _databaseIdentifier, _sqlParams, _transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="insertMode"></param>
        /// <returns></returns>
        public UpsertQueryObject<T> ForSimpleUpsertQuery(T entity)
        {
            return new UpsertQueryObject<T>(entity, _ext, _concatTrans, _databaseIdentifier, _sqlParams, _transactionCount);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DeleteQueryObject<T> ForSimpleDeleteQuery()
        {
            return new DeleteQueryObject<T>(_ext);
        }



        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public BulkForCollection<T> ForCollection(IEnumerable<T> list)
        {
            return new BulkForCollection<T>(list, _ext);
        }
    }
}

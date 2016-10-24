using System.Collections.Generic;

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
        public CollectionSelect<T> ForCollection<T>(IEnumerable<T> list)
        {
            return new CollectionSelect<T>(list, _ext);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entityRepresentation"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public QueryObject<T> ForBasicQuery<T>(T entityRepresentation)
        {
            return new QueryObject<T>(entityRepresentation, _ext);
        }

    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Setup<T>
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
        /// Use this option for simple updates or deletes where you are only dealing with a single table 
        /// and conditions are not complex. For anything more advanced, use a stored procedure.  
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public QueryObject<T> ForSimpleQuery(T entity)
        {
            return new QueryObject<T>(entity, _ext);
        }

        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public CollectionSelect<T> ForCollection(IEnumerable<T> list)
        {
            return new CollectionSelect<T>(list, _ext);
        }
    }
}

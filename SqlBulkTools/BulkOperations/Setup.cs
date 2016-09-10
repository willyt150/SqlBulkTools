using System.Collections.Generic;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
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

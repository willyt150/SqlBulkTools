using System.Collections.Generic;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CollectionSelect<T>
    {
        private readonly IEnumerable<T> _list;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="ext"></param>
        public CollectionSelect(IEnumerable<T> list, BulkOperations ext)
        {
            _list = list;
            _ext = ext;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public Table<T> WithTable(string tableName)
        {
            return new Table<T>(_list, tableName, _ext);
        }
    }
}

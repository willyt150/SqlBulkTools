using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryObject<T>
    {
        private readonly T _singleEntity;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        public DeleteQueryObject(BulkOperations ext)
        {
            _ext = ext;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public DeleteQueryTable<T> WithTable(string tableName)
        {
            return new DeleteQueryTable<T>(_singleEntity, tableName, _ext);
        }
    }
}

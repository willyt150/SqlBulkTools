namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpdateQueryObject<T>
    {
        private readonly T _singleEntity;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="ext"></param>
        public UpdateQueryObject(T singleEntity, BulkOperations ext)
        {
            _ext = ext;
            _singleEntity = singleEntity;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public QueryTable<T> WithTable(string tableName)
        {
            return new QueryTable<T>(_singleEntity, tableName, _ext);
        }
    }
}

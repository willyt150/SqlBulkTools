using System;
using System.Collections.Generic;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class DataTableColumns<T>
    {
        private HashSet<string> Columns { get; set; }
        private readonly IEnumerable<T> _list;
        private readonly DataTableOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        public DataTableColumns(IEnumerable<T> list, DataTableOperations ext)
        {
            _list = list;
            _ext = ext;
            Columns = new HashSet<string>();
        }

        /// <summary>
        /// Add each column that you want to include in the DataTable manually. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public DataTableSingularColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new DataTableSingularColumnSelect<T>(_ext, _list, Columns);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public DataTableAllColumnSelect<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(typeof(T));
            return new DataTableAllColumnSelect<T>(_ext, _list, Columns);
        }

    }
}

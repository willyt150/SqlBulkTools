using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataTableAllColumnSelect<T> : IDataTableTransaction
    {
        private readonly DataTableOperations _ext;
        private readonly IEnumerable<T> _list;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private readonly BulkOperationsHelper _helper;
        private readonly HashSet<string> _columns;
        private readonly HashSet<string> _removedColumns; 
        private DataTable _dt;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ext"></param>
        /// <param name="list"></param>
        /// <param name="columns"></param>
        public DataTableAllColumnSelect(DataTableOperations ext, IEnumerable<T> list, HashSet<string> columns)
        {
            _helper = new BulkOperationsHelper();
            _ext = ext;
            _list = list;
            _columns = columns;
            _removedColumns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
        }

        /// <summary>
        /// If a column name in your model does not match the designated column name in the actual SQL table, 
        /// you can add a custom column mapping.   
        /// </summary>
        /// <returns></returns>
        public DataTableAllColumnSelect<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = _helper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }

        /// <summary>
        /// Remove a column that you want to be excluded. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public DataTableAllColumnSelect<T> RemoveColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            if (_columns.Contains(propertyName))
            {
                _removedColumns.Add(propertyName);
                _columns.Remove(propertyName);
            }
                

            else           
                throw new InvalidOperationException("Could not remove the column with name " 
                    + columnName +  
                    ". This could be because it's not a value or string type and therefore not included.");

            return this;
        }

        /// <summary>
        /// Returns a data table to be used in a stored procedure. 
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public DataTable PrepareDataTable()
        {
            _ext.SetBulkExt(this, _columns, CustomColumnMappings, typeof(T), _removedColumns);
            _dt = _helper.CreateDataTable<T>(_columns, CustomColumnMappings);
            return _dt;
        }

        DataTable IDataTableTransaction.BuildDataTable()
        {
            return _helper.ConvertListToDataTable(_dt, _list, _columns);
        }

    }
}

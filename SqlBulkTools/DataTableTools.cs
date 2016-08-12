using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("SqlBulkTools.UnitTests")]
[assembly: InternalsVisibleTo("SqlBulkTools.IntegrationTests")]
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class DataTableTools<T>
    {
        private readonly BulkOperationsHelper _helper;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        /// <summary>
        /// 
        /// </summary>
        public DataTable DataTable { get; private set; }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        internal DataTableTools(HashSet<string> columns, Dictionary<string, string> customColumnMappings)
        {
            _helper = new BulkOperationsHelper();
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            DataTable = _helper.CreateDataTable<T>(_columns, _customColumnMappings);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetColumn(Expression<Func<T, object>> columnName)
        {
            if (_columns == null)
                throw new NullReferenceException("No columns have been added. Use AddColumn or AddColumns and/or refer to documentation.");

            var propertyName = _helper.GetPropertyName(columnName);


            if (_customColumnMappings != null)
            {
                string customColumn;

                if (_customColumnMappings.TryGetValue(propertyName, out customColumn))
                {
                    return customColumn;
                }
            }

            if (_columns.Contains(propertyName))
            {
                return propertyName;
            }

            throw new InvalidOperationException("The column \'" + columnName + "\' has not been added to the data table. Use AddColumn or AddColumns to add it and/or refer to documentation.");

        }
    }
}

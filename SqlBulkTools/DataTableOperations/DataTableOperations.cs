using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class DataTableOperations
    {
        private HashSet<string> _columns;
        private HashSet<string> _removedColumns; 
        private Dictionary<string, string> _customColumnMappings;
        private IDataTableTransaction _dataTableTransaction;
        private BulkOperationsHelper _helper;
        private Type _expectedType;


        /// <summary>
        /// 
        /// </summary>
        public DataTableOperations()
        {
            _helper = new BulkOperationsHelper();
            _expectedType = null;
        }

        internal void SetBulkExt(IDataTableTransaction dataTableTransaction, HashSet<string> columns, Dictionary<string, string> customColumnMappings, Type expectedType, HashSet<string> removedColumns = null)
        {
            _dataTableTransaction = dataTableTransaction;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _expectedType = expectedType;
            _removedColumns = removedColumns;
        }

        /// <summary>
        /// Set up a DataTable. Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public SetupDataTable<T> SetupDataTable<T>()
        {
            return new SetupDataTable<T>(this);
        }

        /// <summary>
        /// Builds a prepared DataTable. PrepareDataTable must be called during Setup for this operation to work. 
        /// You can add further settings to your 
        /// DataTable between PrepareDataTable and BuildPreparedDataTable. See documentation for examples. 
        /// </summary>
        /// <returns>Populated DataTable</returns>
        public DataTable BuildPreparedDataDable()
        {
            this.CheckSetup();

            return _dataTableTransaction.BuildDataTable();
        }

        /// <summary>
        /// Returns a column that has been added during SetupDataTable. Any custom column mappings adding during setup are respected. 
        /// Notes: (1) SetupDataTable must called first. GetColumn will fail without a Setup. 
        /// (2) Type must be of the same type used during setup. (3) Column must be added using AddColumn or AddAllColumns. 
        /// Refer to documentation for examples. 
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetColumn<T>(Expression<Func<T, object>> columnName)
        {

            this.CheckType(typeof(T));
            this.CheckSetup();          
            var propertyName = _helper.GetPropertyName(columnName);

            this.CheckRemovedColumns(propertyName);

            if (_customColumnMappings != null)
            {
                string customColumn;

                if (_customColumnMappings.TryGetValue(propertyName, out customColumn))
                    return customColumn;
                
            }

            if (_columns.Contains(propertyName))
                return propertyName;            



            throw new SqlBulkToolsException("The property \'" + propertyName + "\' was not added during setup. Use AddColumn or AddColumns to add it and/or refer to documentation.");
        }

        private void CheckSetup()
        {
            if (_dataTableTransaction == null)
            {
                throw new SqlBulkToolsException("SetupDataTable has not been completed. Use the SetupDataTable method and prepare a DataTable first and/or refer to documentation.");
            }
        }

        private void CheckType(Type type)
        {
            if (_expectedType != null && _expectedType != type)
            {
                throw new SqlBulkToolsException("GetColumn can only retrieve columns of type \'" + _expectedType.Name + "\'");
            }
        }

        private void CheckRemovedColumns(string propertyName)
        {
            if (_removedColumns != null && _removedColumns.Contains(propertyName))
                throw new SqlBulkToolsException("The property \'" + propertyName + "\' has already been explicitly removed.");
        }


    }
}

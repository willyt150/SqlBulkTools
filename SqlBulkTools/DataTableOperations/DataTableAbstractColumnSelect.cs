using System.Collections.Generic;
using System.Data;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class DataTableAbstractColumnSelect<T>
    {
        #pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        // ReSharper disable InconsistentNaming
        protected DataTableOperations _ext;
        protected IEnumerable<T> _list;
        protected Dictionary<string, string> CustomColumnMappings { get; set; }
        protected HashSet<string> _columns;
        protected DataTable _dt;
        #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member   

        /// <summary>
        /// 
        /// </summary>
        protected DataTableAbstractColumnSelect(DataTableOperations ext, IEnumerable<T> list, HashSet<string> columns)
        {            
            CustomColumnMappings = new Dictionary<string, string>();
            _dt = null;
            _ext = ext;
            _list = list;
            _columns = columns;
        }
    }
}

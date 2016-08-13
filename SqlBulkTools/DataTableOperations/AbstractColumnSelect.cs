using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class AbstractColumnSelect<T>
    {
        #pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        // ReSharper disable InconsistentNaming
        protected DataTableOperations _ext;
        protected IEnumerable<T> _list;
        protected Dictionary<string, string> CustomColumnMappings { get; set; }
        internal BulkOperationsHelper _helper;
        protected HashSet<string> _columns;
        protected DataTable _dt;
        #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member   

        /// <summary>
        /// 
        /// </summary>
        protected AbstractColumnSelect(DataTableOperations ext, IEnumerable<T> list, HashSet<string> columns)
        {
            _helper = new BulkOperationsHelper();
            
            CustomColumnMappings = new Dictionary<string, string>();
            _dt = null;
            _ext = ext;
            _list = list;
            _columns = columns;
        }
    }
}

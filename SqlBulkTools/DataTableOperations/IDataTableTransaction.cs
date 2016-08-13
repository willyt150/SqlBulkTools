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
    public interface IDataTableTransaction
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        DataTable BuildDataTable();
    }
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    public interface IDataTableTransaction
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        DataTable BuildDataTable();
    }
}

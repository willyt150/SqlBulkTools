using System.Data;

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

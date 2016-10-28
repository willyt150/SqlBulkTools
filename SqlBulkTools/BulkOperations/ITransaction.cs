using System.Data.SqlClient;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal interface ITransaction
    {
        int Commit(SqlConnection connection);
        Task<int> CommitAsync(SqlConnection connection);
    }
}

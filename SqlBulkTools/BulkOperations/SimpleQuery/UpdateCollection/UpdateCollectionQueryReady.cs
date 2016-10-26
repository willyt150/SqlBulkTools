using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class UpdateCollectionQueryReady<T> : ITransaction
    {
        private readonly DataTable _smallCollection;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private readonly List<Condition> _whereConditions;
        private readonly List<Condition> _andConditions;
        private readonly List<Condition> _orConditions;
        private readonly List<SqlParameter> _sqlParams;
        private int _conditionSortOrder;
        private string _identityColumn;
        private int _transactionCount;
        private string _databaseIdentifier;
        private List<string> _concatTrans;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="ext"></param>
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="parameters"></param>
        public UpdateCollectionQueryReady(DataTable smallCollection, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings, 
            int sqlTimeout, BulkOperations ext, int conditionSortOrder, List<Condition> whereConditions, List<SqlParameter> sqlParams, int transactionCount, string databaseIdentifier, List<string> concatTrans)
        {
            _smallCollection = smallCollection;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _conditionSortOrder = conditionSortOrder;
            _ext.SetBulkExt(this);
            _whereConditions = whereConditions;  
            _andConditions = new List<Condition>();
            _orConditions = new List<Condition>();
            _sqlParams = sqlParams;
            _identityColumn = string.Empty;
            _transactionCount = transactionCount;
            _databaseIdentifier = databaseIdentifier;
            _concatTrans = concatTrans;
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public UpdateCollectionQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;

            else
            {
                throw new SqlBulkToolsException("Can't have more than one identity column");
            }

            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public UpdateCollectionQueryReady<T> And(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public UpdateCollectionQueryReady<T> Or(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Or, _orConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        private string GetQuery()
        {
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _whereConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _orConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _andConditions);

            StringBuilder sb = new StringBuilder();

            //sb.Append("CREATE TABLE " + Constants.TempTableName + "(");

           
            // Update idea...
//            UPDATE table1
            //SET table1.name = table2.name,
            //    table1.[desc] = table2.[desc]
            //FROM table1 JOIN table2
            //   ON table1.id = table2.id

            sb.Append(BulkOperationsHelper.CreateTableFromDataTable(_smallCollection) + " ");
            sb.Append(
                $"UPDATE {_databaseIdentifier} SET {_databaseIdentifier}.Price = {Constants.TempTableName}.Price FROM {_databaseIdentifier} JOIN {Constants.TempTableName} ON {_databaseIdentifier}.Id = {Constants.TempTableName}.Id");
            //List<string> paramList = new List<string>();

            //foreach (var column in _columns.ToList())
            //{

            //    paramList.Add("[" + column + "]" + " " + SqlTypeMap.GetSqlTypeFromNetType(typeof(string)));
            //}

            //string paramListConcatenated = string.Join(", ", paramList);

            //command.Append(paramListConcatenated);

            //sb.Append($"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, Constants.TempTableName)} ");

            //List<string> statements = new List<string>();


            /* Create a table type. */
    //        CREATE TYPE LocationTableType AS TABLE
    //        (LocationName VARCHAR(50)
    //        , CostRate INT);
    //        GO

    //        /* Create a procedure to receive data for the table-valued parameter. */
    //        CREATE PROCEDURE dbo. usp_InsertProductionLocation
    //            @TVP LocationTableType READONLY
    //AS
    //SET NOCOUNT ON
    //INSERT INTO AdventureWorks2012.Production.Location
    //       (Name
    //       , CostRate
    //       , Availability
    //       , ModifiedDate)
    //    SELECT *, 0, GETDATE()
    //    FROM @TVP;
    //        GO



            //int before = _transactionCount;
            //foreach (var entity in _smallCollection)
            //{
            //    BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, entity, _identityColumn, _transactionCount);
            //    _transactionCount++;
            //}

            //_transactionCount = before;

            //for (int i = 0; i < _smallCollection.Count(); i++)
            //{
            //    if (i == 0)
            //        statements.Add($"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn, _transactionCount)}");

            //    else
            //        statements.Add($"{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn, _transactionCount)}");

            //    _transactionCount++;
            //}

            //sb.Append(string.Join(", ", statements));

            return sb.ToString();

            //BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _transactionCount);

            //var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            //string comm = $"UPDATE {_databaseIdentifier} " +
            // $"{BulkOperationsHelper.BuildUpdateSet(_columns, _transactionCount, _identityColumn)}" +
            // $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery)}";

            return sb.ToString();
        }

        int ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_smallCollection == null)
            {
                return affectedRows;
            }


            using (SqlConnection conn = BulkOperationsHelper.GetSqlConnection(connectionName, credentials, connection))
            {
                conn.Open();

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    try
                    {
                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema,
                            _tableName);


                        StringBuilder sb = new StringBuilder();
                        _concatTrans.Add(GetQuery());
                        _concatTrans.ForEach(x => sb.Append(x));

                        sb.Replace(_databaseIdentifier, fullQualifiedTableName);

                        command.CommandText = sb.ToString();

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }                       

                        affectedRows = command.ExecuteNonQuery();
                        transaction.Commit();

                        return affectedRows;
                    }

                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }

                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        async Task<int> ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            int affectedRows = 0;
            if (_smallCollection == null)
            {
                return affectedRows;
            }

            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _whereConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _orConditions);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _andConditions);

            //BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, null);

            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);


            using (SqlConnection conn = BulkOperationsHelper.GetSqlConnection(connectionName, credentials, connection))
            {
                await conn.OpenAsync();

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    try
                    {
                        SqlCommand command = conn.CreateCommand();
                        command.Connection = conn;
                        command.Transaction = transaction;
                        command.CommandTimeout = _sqlTimeout;

                        string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(conn.Database, _schema,
                            _tableName);

                        string comm = $"UPDATE {fullQualifiedTableName} " +
                                      $"{BulkOperationsHelper.BuildUpdateSet(_columns, _transactionCount, _identityColumn)}" +
                                      $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery)}";

                        command.CommandText = comm;

                        if (_sqlParams.Count > 0)
                        {
                            command.Parameters.AddRange(_sqlParams.ToArray());
                        }

                        affectedRows = await command.ExecuteNonQueryAsync();
                        transaction.Commit();

                        return affectedRows;
                    }

                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }

                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }
    }
}

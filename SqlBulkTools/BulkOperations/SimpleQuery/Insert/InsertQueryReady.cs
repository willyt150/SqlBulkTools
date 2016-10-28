using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class InsertQueryReady<T> : ITransaction
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly BulkOperations _ext;
        private int _conditionSortOrder;
        private string _identityColumn;
        private ColumnDirection _outputIdentity;
        private List<SqlParameter> _sqlParams;


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
        public InsertQueryReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings,
            int sqlTimeout, BulkOperations ext, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _ext = ext;
            _ext.SetBulkExt(this);
            _sqlParams = sqlParams;
            _outputIdentity = ColumnDirection.Input;
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public InsertQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
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

            _columns.Add(propertyName);

            return this;
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="direction"></param>
        /// <returns></returns>
        public InsertQueryReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName, ColumnDirection direction)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _outputIdentity = direction;

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;


            else
            {
                throw new SqlBulkToolsException("Can't have more than one identity column");
            }

            _columns.Add(propertyName);

            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        /// <exception cref="IdentityException"></exception>
        public int Commit(SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }      

            if (connection.State != ConnectionState.Open)
                connection.Open();

            try
            {
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandTimeout = _sqlTimeout;

                string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);

                StringBuilder sb = new StringBuilder();

                sb.Append($"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                              $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)} ");

                if (_outputIdentity == ColumnDirection.InputOutput)
                {
                    sb.Append($"SET @{_identityColumn}=SCOPE_IDENTITY();");
                }

                BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity);

                command.CommandText = sb.ToString();

                if (_sqlParams.Count > 0)
                {
                    command.Parameters.AddRange(_sqlParams.ToArray());
                }

                affectedRows = command.ExecuteNonQuery();

                if (_outputIdentity == ColumnDirection.InputOutput)
                {
                    foreach (var x in _sqlParams)
                    {
                        if (x.Direction == ParameterDirection.Output
                            && x.ParameterName == $"@{_identityColumn}")
                        {
                            PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                            propertyInfo.SetValue(_singleEntity, x.Value);
                            break;
                        }
                    }
                }

                return affectedRows;
            }

            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error. 
                    if (e.Errors[i].Number == 544)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup. 
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }
                throw;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        /// <exception cref="IdentityException"></exception>
        public async Task<int> CommitAsync(SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync();

            try
            {
                BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

                SqlCommand command = connection.CreateCommand();
                command.Connection = connection;
                command.CommandTimeout = _sqlTimeout;

                string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);

                StringBuilder sb = new StringBuilder();

                sb.Append($"{BulkOperationsHelper.BuildInsertIntoSet(_columns, _identityColumn, fullQualifiedTableName)} " +
                              $"VALUES{BulkOperationsHelper.BuildValueSet(_columns, _identityColumn)} ");

                if (_outputIdentity == ColumnDirection.InputOutput)
                {
                    sb.Append($"SET @{_identityColumn}=SCOPE_IDENTITY();");
                }

                BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, _identityColumn, _outputIdentity);

                command.CommandText = sb.ToString();

                if (_sqlParams.Count > 0)
                {
                    command.Parameters.AddRange(_sqlParams.ToArray());
                }

                affectedRows = await command.ExecuteNonQueryAsync();

                if (_outputIdentity == ColumnDirection.InputOutput)
                {
                    foreach (var x in _sqlParams)
                    {
                        if (x.Direction == ParameterDirection.Output
                            && x.ParameterName == $"@{_identityColumn}")
                        {
                            PropertyInfo propertyInfo = _singleEntity.GetType().GetProperty(_identityColumn);
                            propertyInfo.SetValue(_singleEntity, x.Value);
                            break;
                        }
                    }
                }

                return affectedRows;
            }

            catch (SqlException e)
            {
                for (int i = 0; i < e.Errors.Count; i++)
                {
                    // Error 8102 is identity error. 
                    if (e.Errors[i].Number == 544)
                    {
                        // Expensive but neccessary to inform user of an important configuration setup. 
                        throw new IdentityException(e.Errors[i].Message);
                    }
                }
                throw;
            }
        }
    }
}

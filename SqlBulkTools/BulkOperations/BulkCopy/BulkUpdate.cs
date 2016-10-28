using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
	/// <summary>
	/// 
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class BulkUpdate<T> : AbstractOperation<T>, ITransaction
	{
		/// <summary>
		/// Updates existing records in bulk. 
		/// </summary>
		/// <param name="list"></param>
		/// <param name="tableName"></param>
		/// <param name="schema"></param>
		/// <param name="columns"></param>
		/// <param name="disableAllIndexes"></param>
		/// <param name="customColumnMappings"></param>
		/// <param name="sqlTimeout"></param>
		/// <param name="bulkCopyTimeout"></param>
		/// <param name="bulkCopyEnableStreaming"></param>
		/// <param name="bulkCopyNotifyAfter"></param>
		/// <param name="bulkCopyBatchSize"></param>
		/// <param name="sqlBulkCopyOptions"></param>
		/// <param name="ext"></param>
		/// <param name="disableIndexList"></param>
		/// <param name="bulkCopyDelegates"></param>
		public BulkUpdate( IEnumerable<T> list, string tableName, string schema, HashSet<string> columns, HashSet<string> disableIndexList,
			bool disableAllIndexes, Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout,
			bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions,
			BulkOperations ext, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates )
			:
			base( list, tableName, schema, columns, disableIndexList, disableAllIndexes, customColumnMappings, sqlTimeout,
				bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, ext, bulkCopyDelegates )
		{
			_ext.SetBulkExt( this );
			_updatePredicates = new List<Condition>();
			_parameters = new List<SqlParameter>();
			_conditionSortOrder = 1;
		}

		/// <summary>
		/// Only update records when the target satisfies a speicific requirement. This is used in conjunction with MatchTargetOn.
		/// See help docs for examples.  
		/// </summary>
		/// <param name="predicate"></param>
		/// <returns></returns>
		/// <exception cref="SqlBulkToolsException"></exception>
		public BulkUpdate<T> UpdateWhen( Expression<Func<T, bool>> predicate )
		{
			BulkOperationsHelper.AddPredicate( predicate, PredicateType.Update, _updatePredicates, _parameters, _conditionSortOrder, Constants.UniqueParamIdentifier );
			_conditionSortOrder++;

			return this;
		}

		/// <summary>
		/// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating 
		/// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed 
		/// for matching composite relationships. 
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public BulkUpdate<T> MatchTargetOn( Expression<Func<T, object>> columnName )
		{
			var propertyName = BulkOperationsHelper.GetPropertyName( columnName );

			if ( propertyName == null )
				throw new SqlBulkToolsException( "MatchTargetOn column name can't be null." );

			_matchTargetOn.Add( propertyName );

			return this;
		}

		/// <summary>
		/// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
		/// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns></returns>
		public BulkUpdate<T> SetIdentityColumn( Expression<Func<T, object>> columnName )
		{
			base.SetIdentity( columnName );
			return this;
		}

		/// <summary>
		/// Sets the identity column for the table. Required if an Identity column exists in table and one of the two 
		/// following conditions is met: (1) MatchTargetOn list contains an identity column (2) AddAllColumns is used in setup. 
		/// </summary>
		/// <param name="columnName"></param>
		/// <param name="outputIdentity"></param>
		/// <returns></returns>
		public BulkUpdate<T> SetIdentityColumn( Expression<Func<T, object>> columnName, ColumnDirection outputIdentity )
		{
			base.SetIdentity( columnName, outputIdentity );
			return this;
		}

		int ITransaction.CommitTransaction( string connectionName, SqlCredential credentials, SqlConnection connection, SqlTransaction transaction )
		{
			int affectedRows = 0;
			if ( !_list.Any() )
			{
				return affectedRows;
			}

			base.IndexCheck();
			base.MatchTargetCheck();

			DataTable dt = BulkOperationsHelper.CreateDataTable<T>( _columns, _customColumnMappings, _matchTargetOn, _outputIdentity );
			dt = BulkOperationsHelper.ConvertListToDataTable( dt, _list, _columns, _outputIdentityDic );

			// Must be after ToDataTable is called. 
			BulkOperationsHelper.DoColumnMappings( _customColumnMappings, _columns, _matchTargetOn );
			BulkOperationsHelper.DoColumnMappings( _customColumnMappings, _updatePredicates );

			bool handleConnectionInternally = false;
			SqlConnection conn = BulkOperationsHelper.GetSqlConnection( connectionName, credentials, connection );
			if ( conn.State != ConnectionState.Open )
			{
				conn.Open();
				handleConnectionInternally = true;
			}
			var dtCols = BulkOperationsHelper.GetDatabaseSchema( conn, _schema, _tableName, transaction );

			bool handleTransactionInternally = false;
			if ( transaction == null )
			{
				transaction = conn.BeginTransaction();
				handleTransactionInternally = true;
			}
			try
			{
				SqlCommand command = conn.CreateCommand();
				command.Connection = conn;
				command.Transaction = transaction;
				command.CommandTimeout = _sqlTimeout;

				//Creating temp table on database
				command.CommandText = BulkOperationsHelper.BuildCreateTempTable( _columns, dtCols, _outputIdentity );
				command.ExecuteNonQuery();

				//Bulk insert into temp table
				BulkOperationsHelper.InsertToTmpTable( conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
					_bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates );

				// Updating destination table, and dropping temp table
				if ( _disableIndexList != null && _disableIndexList.Any() )
				{
					command.CommandText = BulkOperationsHelper.GetIndexManagementCmd( IndexOperation.Disable, _tableName,
						_schema, conn, _disableIndexList, _disableAllIndexes );
					command.ExecuteNonQuery();
				}

				string comm = BulkOperationsHelper.GetOutputCreateTableCmd( _outputIdentity, Constants.TempOutputTableName,
				OperationType.InsertOrUpdate, _identityColumn );

				if ( !string.IsNullOrWhiteSpace( comm ) )
				{
					command.CommandText = comm;
					command.ExecuteNonQuery();
				}

				comm = "MERGE INTO " + BulkOperationsHelper.GetFullQualifyingTableName( conn.Database, _schema, _tableName ) + " WITH (HOLDLOCK) AS Target " +
							  "USING " + Constants.TempTableName + " AS Source " +
							  BulkOperationsHelper.BuildJoinConditionsForUpdateOrInsert( _matchTargetOn.ToArray(),
								  Constants.SourceAlias, Constants.TargetAlias ) +
							  "WHEN MATCHED " + BulkOperationsHelper.BuildPredicateQuery( _matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias ) +
							  "THEN UPDATE " +
							  BulkOperationsHelper.BuildUpdateSet( _columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn ) +
							  BulkOperationsHelper.GetOutputIdentityCmd( _identityColumn, _outputIdentity, Constants.TempOutputTableName,
						OperationType.Update ) + "; " +
							  "DROP TABLE " + Constants.TempTableName + ";";
				command.CommandText = comm;

				if ( _parameters.Count > 0 )
				{
					command.Parameters.AddRange( _parameters.ToArray() );
				}

				affectedRows = command.ExecuteNonQuery();

				if ( _disableIndexList != null && _disableIndexList.Any() )
				{
					command.CommandText = BulkOperationsHelper.GetIndexManagementCmd( IndexOperation.Rebuild, _tableName, _schema,
						conn, _disableIndexList );
					command.ExecuteNonQuery();
				}

				if ( _outputIdentity == ColumnDirection.InputOutput )
				{
					BulkOperationsHelper.LoadFromTmpOutputTable( command, _identityColumn, _outputIdentityDic, OperationType.InsertOrUpdate, _list );
				}

				if ( handleTransactionInternally )
				{
					transaction.Commit();
				}
				return affectedRows;
			}


			catch ( SqlException e )
			{
				for ( int i = 0; i < e.Errors.Count; i++ )
				{
					// Error 8102 is identity error. 
					if ( e.Errors[i].Number == 8102 )
					{
						// Expensive call but neccessary to inform user of an important configuration setup. 
						throw new IdentityException( e.Errors[i].Message );
					}
				}

				if ( handleTransactionInternally )
				{
					transaction.Rollback();
				}
				throw;
			}

			catch ( Exception )
			{
				if ( handleTransactionInternally )
				{
					transaction.Rollback();
				}
				throw;
			}

			finally
			{
				if ( handleTransactionInternally )
				{
					transaction.Dispose();
				}
				if ( handleConnectionInternally )
				{
					conn.Close();
					conn.Dispose();
				}
			}
		}

		async Task<int> ITransaction.CommitTransactionAsync( string connectionName, SqlCredential credentials, SqlConnection connection, SqlTransaction transaction )
		{
			int affectedRows = 0;
			if ( !_list.Any() )
			{
				return affectedRows;
			}

			base.IndexCheck();
			base.MatchTargetCheck();

			DataTable dt = BulkOperationsHelper.CreateDataTable<T>( _columns, _customColumnMappings, _matchTargetOn, _outputIdentity );
			dt = BulkOperationsHelper.ConvertListToDataTable( dt, _list, _columns, _outputIdentityDic );

			// Must be after ToDataTable is called. 
			BulkOperationsHelper.DoColumnMappings( _customColumnMappings, _columns, _matchTargetOn );
			BulkOperationsHelper.DoColumnMappings( _customColumnMappings, _updatePredicates );

			bool handleConnectionInternally = false;
			SqlConnection conn = BulkOperationsHelper.GetSqlConnection( connectionName, credentials, connection );
			if ( conn.State != ConnectionState.Open )
			{
				await conn.OpenAsync();
				handleConnectionInternally = true;
			}
			var dtCols = BulkOperationsHelper.GetDatabaseSchema( conn, _schema, _tableName, transaction );

			bool handleTransactionInternally = false;
			if ( transaction == null )
			{
				transaction = conn.BeginTransaction();
				handleTransactionInternally = true;
			}
			try
			{
				SqlCommand command = conn.CreateCommand();
				command.Connection = conn;
				command.Transaction = transaction;
				command.CommandTimeout = _sqlTimeout;

				//Creating temp table on database
				command.CommandText = BulkOperationsHelper.BuildCreateTempTable( _columns, dtCols, _outputIdentity );
				await command.ExecuteNonQueryAsync();

				//Bulk insert into temp table
				await BulkOperationsHelper.InsertToTmpTableAsync( conn, transaction, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
					_bulkCopyNotifyAfter, _bulkCopyTimeout, _sqlBulkCopyOptions, _bulkCopyDelegates );

				if ( _disableIndexList != null && _disableIndexList.Any() )
				{
					command.CommandText = BulkOperationsHelper.GetIndexManagementCmd( IndexOperation.Disable, _tableName,
						_schema, conn, _disableIndexList, _disableAllIndexes );
					await command.ExecuteNonQueryAsync();
				}

				string comm = BulkOperationsHelper.GetOutputCreateTableCmd( _outputIdentity, Constants.TempOutputTableName,
				OperationType.InsertOrUpdate, _identityColumn );

				if ( !string.IsNullOrWhiteSpace( comm ) )
				{
					command.CommandText = comm;
					command.ExecuteNonQuery();
				}

				// Updating destination table, and dropping temp table
				comm = "MERGE INTO " + BulkOperationsHelper.GetFullQualifyingTableName( conn.Database, _schema, _tableName ) + " WITH (HOLDLOCK) AS Target " +
							  "USING " + Constants.TempTableName + " AS Source " +
							  BulkOperationsHelper.BuildJoinConditionsForUpdateOrInsert( _matchTargetOn.ToArray(), Constants.SourceAlias, Constants.TargetAlias ) +
							  "WHEN MATCHED " + BulkOperationsHelper.BuildPredicateQuery( _matchTargetOn.ToArray(), _updatePredicates, Constants.TargetAlias ) +
							  "THEN UPDATE " +
							  BulkOperationsHelper.BuildUpdateSet( _columns, Constants.SourceAlias, Constants.TargetAlias, _identityColumn ) +
							  BulkOperationsHelper.GetOutputIdentityCmd( _identityColumn, _outputIdentity, Constants.TempOutputTableName,
							  OperationType.Update ) + "; " +
							  "DROP TABLE " + Constants.TempTableName + ";";

				command.CommandText = comm;

				if ( _parameters.Count > 0 )
				{
					command.Parameters.AddRange( _parameters.ToArray() );
				}

				affectedRows = await command.ExecuteNonQueryAsync();

				if ( _disableIndexList != null && _disableIndexList.Any() )
				{
					command.CommandText = BulkOperationsHelper.GetIndexManagementCmd( IndexOperation.Rebuild, _tableName,
						_schema, conn, _disableIndexList );
					await command.ExecuteNonQueryAsync();
				}

				if ( _outputIdentity == ColumnDirection.InputOutput )
				{
					await
						BulkOperationsHelper.LoadFromTmpOutputTableAsync( command, _identityColumn, _outputIdentityDic,
						OperationType.Delete, _list );
				}

				if ( handleTransactionInternally )
				{
					transaction.Commit();
				}
				return affectedRows;
			}


			catch ( SqlException e )
			{
				for ( int i = 0; i < e.Errors.Count; i++ )
				{
					// Error 8102 is identity error. 
					if ( e.Errors[i].Number == 8102 )
					{
						// Expensive but neccessary to inform user of an important configuration setup. 
						throw new IdentityException( e.Errors[i].Message );
					}
				}
				if ( handleTransactionInternally )
				{
					transaction.Rollback();
				}
				throw;
			}

			catch ( Exception )
			{
				if ( handleTransactionInternally )
				{
					transaction.Rollback();
				}
				throw;
			}
			finally
			{
				if ( handleTransactionInternally )
				{
					transaction.Dispose();
				}
				if ( handleConnectionInternally )
				{
					conn.Close();
					conn.Dispose();
				}
			}
		}
	}
}

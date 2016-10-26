 // ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal static class Constants
    {
        internal const string DefaultSchemaName = "dbo";
        internal const string InternalId = "SqlBulkTools_InternalId";
        internal const string TempTableName = "#TmpTable";
        internal const string TempOutputTableName = "#TmpOutput";
        internal const string SourceAlias = "Source";
        internal const string TargetAlias = "Target";
        internal const string UniqueParamIdentifier = "Condition";

    }

    internal static class IndexOperation
    {
        internal const string Rebuild = "REBUILD";
        internal const string Disable = "DISABLE";
    }



#pragma warning disable 1591
    public enum ColumnDirection
    {        
        Input, InputOutput       
    }
    

    internal enum OperationType
    {
        Insert, InsertOrUpdate, Update, Delete
    }

    public enum PredicateType
    {
        Update, Delete, Where, And, Or
    }

#pragma warning restore 1591
}
